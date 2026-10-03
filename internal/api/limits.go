package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/Imposter/go-searchlight/internal/query"
)

// readBody reads the whole request body, bounded by the route's limit (set by the
// middleware with http.MaxBytesReader): a body over it is a 413, read no further than
// the limit.
func readBody(r *http.Request) ([]byte, error) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, bodyError(err)
	}
	return b, nil
}

// bodyError maps a failed body read: over the limit is a 413, over the in-flight
// budget a 429, a client too slow for read_timeout a 408, anything else a 400.
func bodyError(err error) *Error {
	var mbe *http.MaxBytesError
	var be *budgetError
	switch {
	case errors.As(err, &mbe):
		return TooLarge("the request body is over %d bytes", mbe.Limit)
	case errors.As(err, &be):
		return overBudget(be.b)
	case errors.Is(err, os.ErrDeadlineExceeded):
		return &Error{Status: http.StatusRequestTimeout, Code: CodeRequestTimeout, Detail: "the request body did not arrive within read_timeout", Err: err}
	}
	return InvalidAt("body", "the request body could not be read: %v", err)
}

// budget is an in-flight heap budget (Elasticsearch's indexing pressure): requests
// reserve their body bytes times factor, the heap a byte of body takes at its peak
// (inflight_amplification), and release them when they finish.
type budget struct {
	limit  int64
	factor int64
	used   atomic.Int64
}

// newBudget makes a budget of limit bytes, at least one largest request's worth.
func newBudget(limit, maxBody int64, factor int) *budget {
	f := int64(max(1, factor))
	return &budget{limit: max(limit, maxBody*f, 1), factor: f}
}

// reserve takes n bytes, or reports that they do not fit.
func (b *budget) reserve(n int64) bool {
	for {
		cur := b.used.Load()
		if cur+n > b.limit && n > 0 {
			return false
		}
		if b.used.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

func (b *budget) release(n int64) { b.used.Add(-n) }

// budgetError is a body read refused because the budget is spent.
type budgetError struct{ b *budget }

func (e *budgetError) Error() string { return "api: the in-flight request bytes are over budget" }

// overBudget is the 429 a request gets when its bytes do not fit the budget.
func overBudget(b *budget) *Error {
	return TooMany(DefaultRetryAfter, "requests weighing %d bytes of heap are in progress, the most this node takes at once; retry", b.used.Load())
}

// budgetReader reserves a body of unknown length from a budget as it is read.
type budgetReader struct {
	r        io.ReadCloser
	b        *budget
	reserved int64
}

func (br *budgetReader) Read(p []byte) (int, error) {
	n, err := br.r.Read(p)
	if n > 0 {
		cost := int64(n) * br.b.factor
		if !br.b.reserve(cost) {
			return 0, &budgetError{b: br.b}
		}
		br.reserved += cost
	}
	return n, err
}

func (br *budgetReader) Close() error { return br.r.Close() }

func (br *budgetReader) releaseAll() { br.b.release(br.reserved) }

// decodeJSONObject decodes a JSON object body into v (a struct with json tags),
// refusing unknown keys, trailing data and an empty body unless optional.
func decodeJSONObject(body []byte, v any, optional bool) error {
	if len(bytes.TrimSpace(body)) == 0 {
		if optional {
			return nil
		}
		return InvalidAt("body", "a JSON object body is required")
	}
	if err := strictDecode(body, v); err != nil {
		return jsonProblem(err)
	}
	return nil
}

// jsonProblem turns a JSON decoding error into a 400 at the key it is about.
func jsonProblem(err error) *Error {
	var fe *FieldError
	if errors.As(err, &fe) {
		return InvalidAt(fe.Loc, "%s", fe.Message)
	}
	loc := "body"
	var te *json.UnmarshalTypeError
	if errors.As(err, &te) && te.Field != "" {
		loc = te.Field
	}
	msg := err.Error()
	if field, ok := strings.CutPrefix(msg, "json: unknown field "); ok {
		loc = strings.Trim(field, `"`)
		msg = "unknown key " + field
	}
	return InvalidAt(loc, "the body is not valid: %s", msg)
}

// params are a request's query parameters, checked against the ones its route takes.
type params struct {
	values map[string]string
}

// parseParams refuses a parameter the route does not take, or one given twice, so a
// misspelled wait_for_seq is an error rather than a stale read.
func parseParams(r *http.Request, allowed []string) (params, *Error) {
	p := params{values: map[string]string{}}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return p, InvalidAt("params", "the query string is malformed: %v", err)
	}
	var ps []query.Problem
	for key, vals := range values {
		switch {
		case !slices.Contains(allowed, key):
			ps = append(ps, query.Problem{Loc: "params." + key, Message: "unknown parameter; this endpoint takes " + takes(allowed)})
		case len(vals) > 1:
			ps = append(ps, query.Problem{Loc: "params." + key, Message: "given more than once"})
		default:
			p.values[key] = vals[0]
		}
	}
	if len(ps) > 0 {
		slices.SortFunc(ps, func(a, b query.Problem) int { return strings.Compare(a.Loc, b.Loc) })
		return p, Invalid("the request's parameters are not valid", ps...)
	}
	return p, nil
}

func takes(allowed []string) string {
	if len(allowed) == 0 {
		return "none"
	}
	return strings.Join(allowed, ", ")
}

// int reads a whole-number parameter in [lo, hi]; def when absent.
func (p params) int(name string, def, lo, hi int64) (int64, *Error) {
	v, ok := p.values[name]
	if !ok {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < lo || n > hi {
		return 0, InvalidAt("params."+name, "%s is a whole number from %d to %d", name, lo, hi)
	}
	return n, nil
}

// bool reads true or false; false when absent.
func (p params) bool(name string) (bool, *Error) {
	v, ok := p.values[name]
	if !ok {
		return false, nil
	}
	switch v {
	case "true", "":
		return true, nil
	case "false":
		return false, nil
	}
	return false, InvalidAt("params."+name, "%s is true or false", name)
}

// waitForSeq reads wait_for_seq, a seq a write returned.
func (p params) waitForSeq() (int64, *Error) {
	return p.int("wait_for_seq", 0, 0, 1<<62)
}

// refresh reads refresh: true, wait_for or false.
func (p params) refresh() (RefreshMode, *Error) {
	v, ok := p.values["refresh"]
	if !ok {
		return RefreshNone, nil
	}
	switch v {
	case "true", "":
		return RefreshTrue, nil
	case "wait_for":
		return RefreshWaitFor, nil
	case "false":
		return RefreshNone, nil
	}
	return RefreshNone, InvalidAt("params.refresh", "refresh is true, wait_for or false")
}

// ifSeq reads a single write's condition: if_seq=N (the target's seq must be N) or
// op_type=create (the target must not exist).
func (p params) ifSeq() (int64, *Error) {
	seq, e := p.int("if_seq", 0, 1, 1<<62)
	if e != nil {
		return 0, e
	}
	switch op := p.values["op_type"]; op {
	case "", "index", "upsert":
	case "create":
		if seq != 0 {
			return 0, InvalidAt("params.op_type", "op_type=create and if_seq exclude each other")
		}
		seq = IfAbsent
	default:
		return 0, InvalidAt("params.op_type", "op_type is create or upsert")
	}
	return seq, nil
}

// queue bounds the reads in progress: acquire takes a slot without waiting, or
// refuses (a 429), like Elasticsearch's bounded search queue.
type queue chan struct{}

func (q queue) acquire() bool {
	select {
	case q <- struct{}{}:
		return true
	default:
		return false
	}
}

func (q queue) release() { <-q }
