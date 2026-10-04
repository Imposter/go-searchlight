// Package workloads runs the benchmark's workloads (spec section 14) against search
// engines reached by URL: Searchlight's HTTP API and Elasticsearch's, through the
// [Engine] interface. Each workload runs warmup iterations, then measured ones at a
// fixed concurrency (closed loop) or a fixed arrival rate (open loop, latency measured
// from each request's scheduled start, so a stalled engine is charged for the queue it
// causes), recording latencies in an HDR-style [Histogram].
package workloads

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/bench/report"
)

// Doc is a document to write.
type Doc struct {
	ID   string
	Body json.RawMessage
}

// SearchResult is what the workloads read from a search response.
type SearchResult struct {
	IDs      []string
	Total    int64
	Relation string
	// Next is the engine's own cursor for the page after this one; nil on a short page.
	Next []any
	// Aggs is the response's aggregations, as the engine wrote them.
	Aggs json.RawMessage
}

// Prepared is a search an engine has translated and encoded ahead of the timed path.
type Prepared interface{ isPrepared() }

// Engine is a search engine under test.
type Engine interface {
	// Name is "searchlight" or "elasticsearch".
	Name() string
	Info(ctx context.Context) (report.EngineInfo, error)
	// Ready waits until the engine serves.
	Ready(ctx context.Context) error
	// CreateIndex (re)creates a document index; CreatePercolatorIndex one for saved
	// queries over documents with fields.
	CreateIndex(ctx context.Context, index string, fields []datasets.Field, shards int) error
	CreatePercolatorIndex(ctx context.Context, index string, fields []datasets.Field, shards int) error
	DeleteIndex(ctx context.Context, index string) error
	// Bulk writes docs in one request, acknowledged once durable; refresh is "",
	// "true" or "wait_for".
	Bulk(ctx context.Context, index string, docs []Doc, refresh string) error
	// BulkPercolate writes docs and returns each one's matching saved queries.
	BulkPercolate(ctx context.Context, index string, docs []Doc) ([][]string, error)
	// Refresh makes every acknowledged write searchable.
	Refresh(ctx context.Context, index string) error
	Count(ctx context.Context, index string) (int64, error)
	// Prepare translates a Searchlight search body for this engine.
	Prepare(search []byte) (Prepared, error)
	// Search runs a prepared search, after the cursor when it is given.
	Search(ctx context.Context, index string, p Prepared, after []any) (SearchResult, error)
	// PutQueries stores saved queries; they are percolated once it returns.
	PutQueries(ctx context.Context, index string, qs []datasets.SavedSearch) error
	// Percolate returns each document's matching saved-query ids.
	Percolate(ctx context.Context, index string, docs []json.RawMessage) ([][]string, error)
	Resources(ctx context.Context, index string) (report.Resources, error)
}

// StatusError is an engine answering with an unexpected HTTP status.
type StatusError struct {
	Method, URL string
	Status      int
	Body        string
	// RetryAfter is the response's Retry-After header, parsed as seconds (0 when
	// absent or unparsable).
	RetryAfter time.Duration
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.Method, e.URL, e.Status, e.Body)
}

// retryableStatus reports whether a response's status is worth retrying: 429 (over
// a rate or budget limit) and 503 (temporarily unavailable, e.g. draining or a
// shard's write buffer backpressure), exactly what a production Elasticsearch
// client retries on.
func retryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable
}

// maxRetries bounds retries of a 429/503 so a node stuck refusing writes fails the
// request instead of retrying forever.
const maxRetries = 8

// retryBaseDelay and retryMaxDelay bound the backoff used when a response carries
// no Retry-After header.
const (
	retryBaseDelay = 200 * time.Millisecond
	retryMaxDelay  = 5 * time.Second
)

// client is an HTTP client tuned for benchmarking: many kept-alive connections per
// host, no compression (neither engine pays for gzip), no proxy. It retries 429 and
// 503 responses with backoff (honoring Retry-After when the server sends one), the
// way a production search client would, and logs each retry and the error a request
// ultimately fails with to log (nil discards them).
type client struct {
	base  string
	token string
	http  *http.Client
	log   io.Writer
}

func newClient(base, token string, log io.Writer) *client {
	tr := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	if log == nil {
		log = io.Discard
	}
	return &client{base: strings.TrimRight(base, "/"), token: token, http: &http.Client{Transport: tr, Timeout: 10 * time.Minute}, log: log}
}

// retryDelay is how long to wait before retrying attempt (0-based), honoring a
// server-given Retry-After over the default exponential backoff.
func retryDelay(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	d := retryBaseDelay << attempt
	if d <= 0 || d > retryMaxDelay { // overflow, or past the cap
		d = retryMaxDelay
	}
	return d
}

// do sends a request, retrying a 429 or 503 response with backoff (honoring
// Retry-After), and returns the response body, or an *StatusError for a status
// outside ok (2xx when ok is empty) once retries are exhausted. Every retry and the
// final error, if any, are logged to c.log so a caller never sees a bare "context
// canceled" for what was really a rate limit, a backpressure 429 or a 503.
func (c *client) do(ctx context.Context, method, path, contentType string, body []byte, ok ...int) ([]byte, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		b, err := c.doOnce(ctx, method, path, contentType, body, ok...)
		var st *StatusError
		switch {
		case err == nil:
			return b, nil
		case !errors.As(err, &st) || !retryableStatus(st.Status) || attempt >= maxRetries:
			if lastErr != nil {
				fmt.Fprintf(c.log, "%s %s %s: failed after %d attempt(s), last error: %v\n",
					time.Now().Format(time.RFC3339), method, path, attempt+1, err)
			}
			return b, err
		}
		lastErr = err
		wait := retryDelay(attempt, st.RetryAfter)
		fmt.Fprintf(c.log, "%s %s %s: HTTP %d (attempt %d/%d), retrying in %s: %s\n",
			time.Now().Format(time.RFC3339), method, path, st.Status, attempt+1, maxRetries+1, wait, st.Body)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

// doOnce sends one attempt of the request behind do's retry loop.
func (c *client) doOnce(ctx context.Context, method, path, contentType string, body []byte, ok ...int) ([]byte, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, r)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	good := res.StatusCode >= 200 && res.StatusCode < 300
	if len(ok) > 0 {
		good = false
		for _, s := range ok {
			good = good || res.StatusCode == s
		}
	}
	if !good {
		msg := string(b)
		if len(msg) > 2000 {
			msg = msg[:2000] + "..."
		}
		return b, &StatusError{Method: method, URL: c.base + path, Status: res.StatusCode, Body: msg, RetryAfter: retryAfterHeader(res.Header.Get("Retry-After"))}
	}
	return b, nil
}

// retryAfterHeader parses a Retry-After header's seconds form (the only form this
// codebase's servers send); 0 when absent or not a valid non-negative integer.
func retryAfterHeader(v string) time.Duration {
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

func (c *client) json(ctx context.Context, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		var err error
		if raw, isRaw := in.([]byte); isRaw {
			body = raw
		} else if body, err = json.Marshal(in); err != nil {
			return err
		}
	}
	ct := ""
	if body != nil {
		ct = "application/json"
	}
	b, err := c.do(ctx, method, path, ct, body)
	if err != nil || out == nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("%s %s: decoding the response: %w", method, path, err)
	}
	return nil
}

// waitReady polls fn until it succeeds or ctx ends.
func waitReady(ctx context.Context, fn func(context.Context) error) error {
	var last error
	for {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		last = fn(rctx)
		cancel()
		if last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// procRSS reads a process's resident set from /proc (Linux); 0 elsewhere or when the
// process is not visible.
func procRSS(pid int) int64 {
	if pid <= 0 {
		return 0
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if rest, ok := strings.CutPrefix(line, "VmRSS:"); ok {
			f := strings.Fields(rest)
			if len(f) >= 1 {
				kb, err := strconv.ParseInt(f[0], 10, 64)
				if err == nil {
					return kb << 10
				}
			}
		}
	}
	return 0
}

// pathsSize sums the sizes of every file under paths.
func pathsSize(paths []string) (int64, error) {
	var total int64
	for _, p := range paths {
		n, err := dirSize(p)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}
