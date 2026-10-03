package api

import (
	"net/http"
)

// MaxFieldEntries bounds the entries per field a field catalogue lists.
const MaxFieldEntries = 1000

// fields serves GET /indexes/{index}/_fields?entries=N: the mapping's fields, with
// each keyword_list field's N most frequent entries.
func (s *Server) fields(w http.ResponseWriter, r *http.Request, p params) error {
	wait, e := p.waitForSeq()
	if e != nil {
		return e
	}
	entries, e := p.int("entries", 0, 0, MaxFieldEntries)
	if e != nil {
		return e
	}
	cat, err := s.c.Fields(r.Context(), r.PathValue("index"), int(entries), ReadOptions{WaitForSeq: wait})
	if err != nil {
		return err
	}
	if cat.Fields == nil {
		cat.Fields = []FieldInfo{}
	}
	return writeJSON(w, http.StatusOK, cat)
}
