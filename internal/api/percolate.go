package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Imposter/go-searchlight/internal/percolate"
)

// MaxPercolateDocs bounds the documents (given and stored) of one _percolate request.
const MaxPercolateDocs = 10_000

// percolateBody is POST _percolate's body.
type percolateBody struct {
	Docs []json.RawMessage `json:"docs"`
	IDs  []string          `json:"ids"`
}

// percolate serves POST /indexes/{index}/_percolate: {"docs": [{...}], "ids": [...]}
// in; for each document, given ones first, the ids of the saved queries it matches.
// Documents matching more than percolate.MaxMatches saved queries together are
// refused with a 422, never answered in part.
func (s *Server) percolate(w http.ResponseWriter, r *http.Request, p params) error {
	wait, e := p.waitForSeq()
	if e != nil {
		return e
	}
	body, err := readBody(r)
	if err != nil {
		return err
	}
	var req percolateBody
	if err := decodeJSONObject(body, &req, false); err != nil {
		return err
	}
	switch n := len(req.Docs) + len(req.IDs); {
	case n == 0:
		return InvalidAt("body", "give docs or ids to percolate")
	case n > MaxPercolateDocs:
		return TooLarge("%d documents to percolate, over %d", n, MaxPercolateDocs)
	}
	for i, d := range req.Docs {
		if int64(len(d)) > s.cfg.MaxDocBytes {
			return TooLarge("docs.%d is %d bytes, over %d (max_doc_bytes)", i, len(d), s.cfg.MaxDocBytes)
		}
	}
	withMatches, innerHits, release, e := s.percolateDetails(p)
	if e != nil {
		return e
	}
	defer release()
	res, err := s.c.Percolate(r.Context(), r.PathValue("index"), &PercolateRequest{Docs: req.Docs, IDs: req.IDs, Matches: withMatches, InnerHits: innerHits}, ReadOptions{WaitForSeq: wait})
	if err != nil {
		return err
	}
	if matches(res.Results, func(r *PercolateResult) json.RawMessage { return r.Queries }) > percolate.MaxMatches {
		return tooManyMatches(nil)
	}
	admitted := info(r.Context()).admitted
	return writePercolate(w, res, func() time.Duration { return s.clock.Since(admitted) })
}

// matches counts the saved-query ids of every result, queries giving each one's.
func matches[T any](results []T, queries func(*T) json.RawMessage) int {
	n := 0
	for i := range results {
		n += percolate.IDs(queries(&results[i])).Len()
	}
	return n
}

// emptyArray is the queries of a document that matches none.
const emptyArray = "[]"

// percolateBuffers hold the responses writePercolate builds.
var percolateBuffers = sync.Pool{New: func() any { return new([]byte) }}

// maxPooledResponse is the largest response buffer kept for reuse.
const maxPooledResponse = 4 << 20

// writePercolate writes res as {"results":[...],"stale":true,"took_ms":N,"took_us":M},
// exactly as encoding/json would, copying each result's queries array as it is. took
// is read once the results are encoded: the server's time for the request, from its
// admission to its last byte encoded, which took_ms and took_us (whole milliseconds
// and microseconds) and the Server-Timing header (total;dur, in milliseconds with
// microseconds) report.
func writePercolate(w http.ResponseWriter, res *PercolateResponse, took func() time.Duration) error {
	bp, _ := percolateBuffers.Get().(*[]byte)
	if bp == nil {
		bp = new([]byte)
	}
	b := (*bp)[:0]
	b = append(b, `{"results":[`...)
	for i := range res.Results {
		r := &res.Results[i]
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '{')
		if r.ID != "" {
			id, err := json.Marshal(r.ID)
			if err != nil {
				return err
			}
			b = append(b, `"id":`...)
			b = append(b, id...)
			b = append(b, ',')
		}
		b = strconv.AppendBool(append(b, `"found":`...), r.Found)
		if r.Seq > 0 {
			b = strconv.AppendInt(append(b, `,"_seq":`...), r.Seq, 10)
		}
		b = append(b, `,"queries":`...)
		if len(r.Queries) == 0 {
			b = append(b, emptyArray...)
		} else {
			b = append(b, r.Queries...)
		}
		if r.Matches != nil {
			ms, err := json.Marshal(r.Matches)
			if err != nil {
				return err
			}
			b = append(append(b, `,"matches":`...), ms...)
		}
		b = append(b, '}')
	}
	b = append(b, ']')
	if res.Stale {
		b = append(b, `,"stale":true`...)
	}
	d := took()
	b = strconv.AppendInt(append(b, `,"took_ms":`...), d.Milliseconds(), 10)
	b = strconv.AppendInt(append(b, `,"took_us":`...), d.Microseconds(), 10)
	b = append(b, "}\n"...)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Server-Timing", "total;dur="+strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 3, 64))
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b) //nolint:gosec // JSON built from encoding/json's own literals, served as application/json
	if cap(b) <= maxPooledResponse {
		*bp = b
		percolateBuffers.Put(bp)
	}
	return nil
}

// percolateDetails reads a percolation's percolate_matches and percolate_inner_hits
// parameters (the second implies the first), and reserves, for inner hits, the heap
// MaxPercolateInnerHits offsets may take from max_inflight_read_bytes: release gives it
// back (a no-op without).
func (s *Server) percolateDetails(p params) (matches, innerHits bool, release func(), e *Error) {
	release = func() {}
	if matches, e = p.bool("percolate_matches"); e != nil {
		return false, false, release, e
	}
	if innerHits, e = p.bool("percolate_inner_hits"); e != nil {
		return false, false, release, e
	}
	if innerHits {
		cost := min(int64(MaxPercolateInnerHits)*innerHitOffsetBytes, s.readBytes.limit/2)
		if !s.readBytes.reserve(cost) {
			return false, false, release, overBudget(s.readBytes)
		}
		release = func() { s.readBytes.release(cost) }
	}
	return matches || innerHits, innerHits, release, nil
}

// innerHitOffsetBytes is the heap one inner hit offset of a percolation's answer is
// charged against max_inflight_read_bytes: its int as a match holds it, and its digits
// in the response.
const innerHitOffsetBytes = 16
