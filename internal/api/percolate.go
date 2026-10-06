package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"
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
	res, err := s.c.Percolate(r.Context(), r.PathValue("index"), &PercolateRequest{Docs: req.Docs, IDs: req.IDs}, ReadOptions{WaitForSeq: wait})
	if err != nil {
		return err
	}
	admitted := info(r.Context()).admitted
	return writePercolate(w, res, func() time.Duration { return s.clock.Since(admitted) })
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
		b = append(b, `,"queries":`...)
		if len(r.Queries) == 0 {
			b = append(b, emptyArray...)
		} else {
			b = append(b, r.Queries...)
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
