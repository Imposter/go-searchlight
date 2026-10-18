package api

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
)

// searchResponse answers POST _search.
type searchResponse struct {
	TookMS   int64                        `json:"took_ms"`
	TimedOut bool                         `json:"timed_out"`
	Stale    bool                         `json:"stale,omitempty"`
	Total    total                        `json:"total"`
	Hits     []hit                        `json:"hits"`
	Next     []any                        `json:"next"`
	Aggs     map[string]*search.AggResult `json:"aggs,omitempty"`
}

type total struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

// hit is a search hit as the API shows it: never the internal segment reference.
type hit struct {
	ID        string                       `json:"id"`
	Sort      []any                        `json:"sort"`
	Body      json.RawMessage              `json:"body,omitempty"`
	InnerHits map[string]*search.InnerHits `json:"inner_hits,omitempty"`
}

// search serves POST /indexes/{index}/_search: {query, post_filter, sort, size, from,
// search_after, track_total, aggs, fields, timeout}. Past the search's timeout, or the
// request's deadline, the response holds what the shards found by then, with timed_out
// set.
func (s *Server) search(w http.ResponseWriter, r *http.Request, p params) error {
	start := s.clock.Now()
	wait, e := p.waitForSeq()
	if e != nil {
		return e
	}
	body, err := readBody(r)
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}
	if !json.Valid(body) {
		return InvalidAt("body", "the body is not one valid JSON object")
	}
	req, problems := search.ParseRequest(body)
	if len(problems) > 0 {
		return Invalid("the search request is invalid", problems...)
	}
	if window := req.From + req.Size; window > s.cfg.MaxResultWindow {
		loc := "from"
		if req.From == 0 {
			loc = "size"
		}
		return InvalidAt(loc, "the result window is too large: from + size is %d, and max_result_window is %d; page deeper with search_after, passing the previous page's next", window, s.cfg.MaxResultWindow)
	}
	if req.From > 0 && req.Size > 0 {
		cost := windowCost(s.readBytes, req.From+req.Size)
		if !s.readBytes.reserve(cost) {
			return overBudget(s.readBytes)
		}
		defer s.readBytes.release(cost)
	}
	if objects := search.InnerHitsObjects(req); objects > 0 {
		cost := windowCost(s.readBytes, min(objects, search.MaxInnerHitsObjects))
		if !s.readBytes.reserve(cost) {
			return overBudget(s.readBytes)
		}
		defer s.readBytes.release(cost)
	}
	resp, err := s.c.Search(r.Context(), r.PathValue("index"), req, ReadOptions{WaitForSeq: wait})
	if err != nil {
		return err
	}
	out := searchResponse{
		TimedOut: resp.TimedOut,
		Stale:    resp.Stale,
		Total:    total{Value: resp.Total, Relation: resp.TotalRelation},
		Hits:     make([]hit, len(resp.Hits)),
		Next:     resp.Next,
		Aggs:     resp.Aggs,
	}
	for i := range resp.Hits {
		h := &resp.Hits[i]
		out.Hits[i] = hit{ID: h.ID, Sort: h.Sort, Body: h.Body, InnerHits: h.InnerHits}
	}
	out.TookMS = s.clock.Since(start).Milliseconds()
	return writeJSON(w, http.StatusOK, out)
}

// countBody is POST _count's body.
type countBody struct {
	Query json.RawMessage `json:"query"`
}

// count serves POST /indexes/{index}/_count: {query}, an exact count.
func (s *Server) count(w http.ResponseWriter, r *http.Request, p params) error {
	wait, e := p.waitForSeq()
	if e != nil {
		return e
	}
	body, err := readBody(r)
	if err != nil {
		return err
	}
	var req countBody
	if err := decodeJSONObject(body, &req, true); err != nil {
		return err
	}
	sr := &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}
	if len(req.Query) > 0 {
		n, problems := query.Parse(req.Query)
		if len(problems) > 0 {
			return Invalid("the query is invalid", problems...)
		}
		sr.Query = n
	}
	resp, err := s.c.Search(r.Context(), r.PathValue("index"), sr, ReadOptions{WaitForSeq: wait})
	if err != nil {
		return err
	}
	out := map[string]any{"count": resp.Total, "relation": resp.TotalRelation, "timed_out": resp.TimedOut}
	if resp.Stale {
		out["stale"] = true
	}
	return writeJSON(w, http.StatusOK, out)
}

// windowHitBytes is the heap one hit of a from search's window is charged against
// max_inflight_read_bytes: its entry in a shard's heap, its id, sort values and ref as
// a peer's JSON reply carries them, and its place in the coordinator's merge; and so is
// each inner hit object a response may hold (search.InnerHitsObjects).
const windowHitBytes = 1 << 10

// windowCost is what a search reaching window hits deep reserves from b: window hits
// at windowHitBytes, at most half of b, so one such search fits an idle budget while
// more of them at once are refused (429).
func windowCost(b *budget, window int) int64 {
	return min(int64(window)*windowHitBytes, b.limit/2)
}
