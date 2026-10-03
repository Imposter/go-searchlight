package api

import (
	"encoding/json"
	"net/http"
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
	start := time.Now()
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
	for i := range res.Results {
		if res.Results[i].Queries == nil {
			res.Results[i].Queries = []string{}
		}
	}
	out := map[string]any{"took_ms": time.Since(start).Milliseconds(), "results": res.Results}
	if res.Stale {
		out["stale"] = true
	}
	return writeJSON(w, http.StatusOK, out)
}
