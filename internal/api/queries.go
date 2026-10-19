package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
)

// queryBody is PUT /indexes/{index}/queries/{id}'s body.
type queryBody struct {
	Query json.RawMessage `json:"query"`
	Meta  json.RawMessage `json:"meta"`
}

func (s *Server) putQuery(w http.ResponseWriter, r *http.Request, p params) error {
	body, err := readBody(r)
	if err != nil {
		return err
	}
	if int64(len(body)) > s.cfg.MaxDocBytes {
		return TooLarge("the saved query is %d bytes, over %d (max_doc_bytes)", len(body), s.cfg.MaxDocBytes)
	}
	var req queryBody
	if err := decodeJSONObject(body, &req, false); err != nil {
		return err
	}
	if len(req.Query) == 0 || string(req.Query) == "null" {
		return InvalidAt("query", "a saved query needs a query")
	}
	return s.singleWrite(w, r, p, WriteOp{Kind: OpQueryUpsert, ID: r.PathValue("id"), Query: req.Query, Meta: req.Meta})
}

func (s *Server) deleteQuery(w http.ResponseWriter, r *http.Request, p params) error {
	return s.singleWrite(w, r, p, WriteOp{Kind: OpQueryDelete, ID: r.PathValue("id")})
}

func (s *Server) getQuery(w http.ResponseWriter, r *http.Request, p params) error {
	wait, e := p.waitForSeq()
	if e != nil {
		return e
	}
	q, err := s.c.GetQuery(r.Context(), r.PathValue("index"), r.PathValue("id"), ReadOptions{WaitForSeq: wait})
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, q)
}

// Saved-query listing bounds.
const (
	DefaultListSize = 100
	MaxListSize     = 1000
)

// listFields are the members a saved-query listing's fields parameter may name.
var listFields = []string{"id", "seq", "version", "query", "meta"}

func (s *Server) listQueries(w http.ResponseWriter, r *http.Request, p params) error {
	if _, e := p.waitForSeq(); e != nil {
		return e
	}
	size, e := p.int("size", DefaultListSize, 1, MaxListSize)
	if e != nil {
		return e
	}
	var fields []string
	if raw, ok := p.values["fields"]; ok {
		for _, f := range strings.Split(raw, ",") {
			f = strings.TrimSpace(f)
			if !slices.Contains(listFields, f) {
				return InvalidAt("params.fields", "fields names members of a saved query: %s", strings.Join(listFields, ", "))
			}
			if !slices.Contains(fields, f) {
				fields = append(fields, f)
			}
		}
	}
	lean := fields != nil && !slices.Contains(fields, "query") && !slices.Contains(fields, "meta")
	after := p.values["after"]
	list, err := s.c.ListQueries(r.Context(), r.PathValue("index"), after, int(size), lean)
	if err != nil {
		return err
	}
	var next *string
	if int64(len(list)) == size {
		next = &list[len(list)-1].ID
	}
	if fields == nil {
		if list == nil {
			list = []*SavedQuery{}
		}
		return writeJSON(w, http.StatusOK, map[string]any{"queries": list, "next": next})
	}
	out := make([]map[string]any, len(list))
	for i, q := range list {
		m := map[string]any{"id": q.ID}
		for _, f := range fields {
			switch f {
			case "seq":
				m[f] = q.Seq
			case "version":
				m[f] = q.Version
			case "query":
				m[f] = q.Query
			case "meta":
				m[f] = q.Meta
			}
		}
		out[i] = m
	}
	return writeJSON(w, http.StatusOK, map[string]any{"queries": out, "next": next})
}
