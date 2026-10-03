package api

import (
	"net/http"
)

// writeResponse answers a single document or saved-query write.
type writeResponse struct {
	ID  string `json:"id"`
	Seq int64  `json:"seq"`
	// TimedOut is set when refresh=wait_for or refresh=true ran out of time: the
	// write is committed but may not be searchable yet.
	TimedOut bool `json:"timed_out,omitempty"`
}

// singleWrite commits one op with the request's refresh and condition parameters.
func (s *Server) singleWrite(w http.ResponseWriter, r *http.Request, p params, op WriteOp) error {
	refresh, e := p.refresh()
	if e != nil {
		return e
	}
	if op.IfSeq, e = p.ifSeq(); e != nil {
		return e
	}
	if op.Kind == OpDelete || op.Kind == OpQueryDelete {
		if op.IfSeq == IfAbsent {
			return InvalidAt("params.op_type", "op_type=create is for writes, not deletes")
		}
	}
	res, err := s.c.Write(r.Context(), r.PathValue("index"), []WriteOp{op}, WriteOptions{Refresh: refresh})
	if err != nil {
		return err
	}
	if len(res.Items) != 1 {
		return ProblemFor(errItemCount)
	}
	if it := res.Items[0]; it.Err != nil {
		return it.Err
	}
	return writeJSON(w, http.StatusOK, writeResponse{ID: op.ID, Seq: res.Items[0].Seq, TimedOut: res.TimedOut})
}

func (s *Server) putDoc(w http.ResponseWriter, r *http.Request, p params) error {
	body, err := readBody(r)
	if err != nil {
		return err
	}
	if int64(len(body)) > s.cfg.MaxDocBytes {
		return TooLarge("the document is %d bytes, over %d (max_doc_bytes)", len(body), s.cfg.MaxDocBytes)
	}
	return s.singleWrite(w, r, p, WriteOp{Kind: OpUpsert, ID: r.PathValue("id"), Body: body})
}

func (s *Server) deleteDoc(w http.ResponseWriter, r *http.Request, p params) error {
	return s.singleWrite(w, r, p, WriteOp{Kind: OpDelete, ID: r.PathValue("id")})
}

func (s *Server) getDoc(w http.ResponseWriter, r *http.Request, p params) error {
	// Reads of one document are realtime (from the system of record), so every
	// committed seq is already visible: wait_for_seq is checked and satisfied.
	if _, e := p.waitForSeq(); e != nil {
		return e
	}
	doc, err := s.c.GetDocument(r.Context(), r.PathValue("index"), r.PathValue("id"))
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, doc)
}
