package api

import (
	"net/http"
)

// writeResponse answers a single document or saved-query write.
type writeResponse struct {
	ID string `json:"id"`
	// Seq is the write's seq, omitted for a noop.
	Seq int64 `json:"seq,omitempty"`
	// Version is the write's client version, when it named one.
	Version int64 `json:"version,omitempty"`
	// Result is "noop" for a versioned write whose version was not above the stored
	// one: nothing was written.
	Result string `json:"result,omitempty"`
	// TimedOut is set when refresh=wait_for or refresh=true ran out of time: the
	// write is committed but may not be searchable yet.
	TimedOut bool `json:"timed_out,omitempty"`
}

// resultNoop is a noop's result, as Elasticsearch names it.
const resultNoop = "noop"

// singleWrite commits one op with the request's refresh, condition and version
// parameters.
func (s *Server) singleWrite(w http.ResponseWriter, r *http.Request, p params, op WriteOp) error {
	refresh, e := p.refresh()
	if e != nil {
		return e
	}
	if op.IfSeq, e = p.ifSeq(); e != nil {
		return e
	}
	if op.Version, e = p.version(op.IfSeq); e != nil {
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
	it := res.Items[0]
	if it.Err != nil {
		return it.Err
	}
	out := writeResponse{ID: op.ID, Seq: it.Seq, Version: it.Version, TimedOut: res.TimedOut}
	if it.Noop {
		out.Result = resultNoop
	}
	return writeJSON(w, http.StatusOK, out)
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
	wait, e := p.waitForSeq()
	if e != nil {
		return e
	}
	doc, err := s.c.GetDocument(r.Context(), r.PathValue("index"), r.PathValue("id"), ReadOptions{WaitForSeq: wait})
	if err != nil {
		return err
	}
	return writeJSON(w, http.StatusOK, doc)
}
