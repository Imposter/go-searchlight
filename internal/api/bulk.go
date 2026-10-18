package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/Imposter/go-searchlight/internal/percolate"
)

var errItemCount = errors.New("api: the coordinator answered a different number of items than ops")

// bulkMeta is an action line's metadata: {"upsert": {"id": "a", "if_seq": 3}} or
// {"query_upsert": {"id": "q", "version": 7}}.
type bulkMeta struct {
	ID      json.RawMessage `json:"id"`
	AltID   json.RawMessage `json:"_id"`
	IfSeq   *int64          `json:"if_seq"`
	Version *int64          `json:"version"`
}

// bulkItem is one op's outcome in a _bulk response.
type bulkItem struct {
	Op     string `json:"op"`
	ID     string `json:"id"`
	Status int    `json:"status"`
	Seq    int64  `json:"seq,omitempty"`
	// Version is the op's client version, when it named one; Result is "noop" when it
	// was not above the stored one.
	Version int64  `json:"version,omitempty"`
	Result  string `json:"result,omitempty"`
	// Queries are the upserted document's matching saved queries, with
	// percolate=true.
	Queries *json.RawMessage `json:"queries,omitempty"`
	Error   map[string]any   `json:"error,omitempty"`
}

// bulkResponse answers a _bulk request.
type bulkResponse struct {
	TookMS int64 `json:"took_ms"`
	// Seq is the newest seq the request committed: pass it as wait_for_seq.
	Seq      int64 `json:"seq"`
	Errors   bool  `json:"errors"`
	TimedOut bool  `json:"timed_out"`
	// Percolated, with percolate=true, says whether the items carry their
	// matching saved queries: false when the percolation could not finish after
	// the writes committed (a deadline, a closed copy, too many matches).
	Percolated *bool `json:"percolated,omitempty"`
	// PercolateReason is CodeTooManyMatches when Percolated is false because the
	// documents match more than percolate.MaxMatches saved queries together.
	PercolateReason string     `json:"percolate_reason,omitempty"`
	Items           []bulkItem `json:"items"`
}

// bulk serves POST /indexes/{index}/_bulk: NDJSON, an action line per op, then the
// document for an upsert, or the saved query for a query_upsert:
//
//	{"upsert": {"id": "a"}}            (also "index"; "create" requires it to be new)
//	{"title": "...", "price": 3}
//	{"delete": {"id": "b", "if_seq": 12}}
//	{"query_upsert": {"id": "q", "version": 7}}
//	{"query": {...}, "meta": {...}}
//	{"query_delete": {"id": "r", "version": 8}}
//
// A version (Elasticsearch's external versioning) makes an op apply only when it is
// higher than the stored one; otherwise its item's result is noop, and nothing is
// written for it.
//
// Every op commits in one transaction. An op refused on its own (an invalid document,
// a failed if_seq) is reported in its item, with errors set, and the rest commit; a
// malformed action line, a line over max_doc_bytes or more than max_bulk_ops ops
// refuse the whole request before anything is written.
func (s *Server) bulk(w http.ResponseWriter, r *http.Request, p params) error {
	start := s.clock.Now()
	refresh, e := p.refresh()
	if e != nil {
		return e
	}
	perc, e := p.bool("percolate")
	if e != nil {
		return e
	}
	wait, e := p.waitForSeq()
	if e != nil {
		return e
	}
	ops, err := s.parseBulk(r.Body)
	if err != nil {
		return err
	}
	res, err := s.c.Write(r.Context(), r.PathValue("index"), ops, WriteOptions{Refresh: refresh, Percolate: perc, WaitForSeq: wait})
	if err != nil {
		return err
	}
	if len(res.Items) != len(ops) {
		return ProblemFor(errItemCount)
	}
	out := bulkResponse{Seq: res.Seq, TimedOut: res.TimedOut, Items: make([]bulkItem, len(ops))}
	switch {
	case perc && res.Percolated && matches(res.Items, func(it *ItemResult) json.RawMessage { return it.Queries }) > percolate.MaxMatches:
		res.Percolated = false
		out.PercolateReason = CodeTooManyMatches
	case perc && res.PercolateErr != nil && ProblemFor(res.PercolateErr).Code == CodeTooManyMatches:
		out.PercolateReason = CodeTooManyMatches
	}
	if perc {
		out.Percolated = &res.Percolated
	}
	id := info(r.Context()).id
	for i := range ops {
		it := &res.Items[i]
		item := bulkItem{Op: ops[i].Kind.String(), ID: ops[i].ID, Status: http.StatusOK, Seq: it.Seq, Version: it.Version}
		if it.Noop {
			item.Result = resultNoop
		}
		if it.Err != nil {
			pe := ProblemFor(it.Err)
			item.Status, item.Seq, item.Error = pe.Status, 0, problemBody(pe, id)
			out.Errors = true
		} else if perc && res.Percolated && ops[i].Kind == OpUpsert && !it.Noop {
			q := it.Queries
			if len(q) == 0 {
				q = json.RawMessage(emptyArray)
			}
			item.Queries = &q
		}
		out.Items[i] = item
	}
	out.TookMS = s.clock.Since(start).Milliseconds()
	return writeJSON(w, http.StatusOK, out)
}

// parseBulk reads the NDJSON body into ops, bounded by max_bulk_ops and, per line,
// max_doc_bytes. Blank lines, a blank last line included, are skipped.
func (s *Server) parseBulk(body io.Reader) ([]WriteOp, error) {
	br := bufio.NewReaderSize(body, 64<<10)
	var ops []WriteOp
	line := 0
	next := func() ([]byte, bool, error) {
		for {
			b, err := s.readLine(br)
			if b == nil && err != nil {
				if errors.Is(err, io.EOF) {
					return nil, false, nil
				}
				return nil, false, err
			}
			line++
			if len(bytes.TrimSpace(b)) > 0 {
				return b, true, nil
			}
			if errors.Is(err, io.EOF) {
				return nil, false, nil
			}
		}
	}
	for {
		action, ok, err := next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if len(ops) == s.cfg.MaxBulkOps {
			return nil, TooLarge("the bulk holds more than %d operations (max_bulk_ops)", s.cfg.MaxBulkOps)
		}
		op, e := parseAction(action, line)
		if e != nil {
			return nil, e
		}
		switch op.Kind {
		case OpUpsert, OpQueryUpsert:
			doc, ok, err := next()
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, InvalidAt(fmt.Sprintf("line.%d", line), "the %s of %q has no line after it", op.Kind, op.ID)
			}
			if op.Kind == OpUpsert {
				op.Body = doc
				break
			}
			var q queryBody
			if err := decodeJSONObject(doc, &q, false); err != nil || len(q.Query) == 0 || string(q.Query) == "null" {
				return nil, InvalidAt(fmt.Sprintf("line.%d", line), `a query_upsert's line is {"query": ..., "meta": {...}}`)
			}
			op.Query, op.Meta = q.Query, q.Meta
		}
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return nil, InvalidAt("body", "a bulk holds at least one operation")
	}
	return ops, nil
}

// readLine reads one line without its newline, refusing one over max_doc_bytes (it
// never buffers more than that). At the end of the body it returns the last line,
// if any, with io.EOF.
func (s *Server) readLine(br *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		chunk, err := br.ReadSlice('\n')
		if int64(len(out)+len(chunk)) > s.cfg.MaxDocBytes+1 {
			return nil, TooLarge("a bulk line is over %d bytes (max_doc_bytes)", s.cfg.MaxDocBytes)
		}
		out = append(out, chunk...)
		switch {
		case err == nil:
			return bytes.TrimRight(out, "\r\n"), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(out) == 0 {
				return nil, io.EOF
			}
			return bytes.TrimRight(out, "\r\n"), io.EOF
		default:
			return nil, bodyError(err)
		}
	}
}

// parseAction reads an action line. An id must be valid UTF-8 as its JSON spells it:
// decoding would replace invalid UTF-8 with U+FFFD, silently writing another id, and an
// escape that is no character (a lone surrogate, "\ud800") decodes to U+FFFD too, so
// an id holding one its JSON did not spell is refused.
func parseAction(raw []byte, line int) (WriteOp, *Error) {
	loc := fmt.Sprintf("line.%d", line)
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil || len(obj) != 1 {
		return WriteOp{}, InvalidAt(loc, `an action line is one of {"upsert": {"id": ...}}, {"create": {...}}, {"delete": {...}}, {"query_upsert": {...}} or {"query_delete": {...}}`)
	}
	var op WriteOp
	var metaRaw json.RawMessage
	for key, v := range obj {
		metaRaw = v
		switch key {
		case "upsert", "index":
			op.Kind = OpUpsert
		case "create":
			op.Kind, op.IfSeq = OpUpsert, IfAbsent
		case "query_upsert":
			op.Kind = OpQueryUpsert
		case "query_delete":
			op.Kind = OpQueryDelete
		case "delete":
			op.Kind = OpDelete
		default:
			return WriteOp{}, InvalidAt(loc+"."+key, "unknown action: use upsert, create, delete, query_upsert or query_delete")
		}
		loc += "." + key
	}
	var meta bulkMeta
	if err := strictDecode(metaRaw, &meta); err != nil {
		return WriteOp{}, InvalidAt(loc, `an action's metadata is {"id": "...", "if_seq": n} or {"id": "...", "version": n}: %v`, err)
	}
	idRaw := meta.ID
	switch {
	case meta.ID != nil && meta.AltID != nil:
		return WriteOp{}, InvalidAt(loc, "give id or _id, not both")
	case meta.AltID != nil:
		idRaw = meta.AltID
	case meta.ID == nil:
		return WriteOp{}, InvalidAt(loc+".id", "an action needs an id")
	}
	if !utf8.Valid(idRaw) {
		return WriteOp{}, InvalidAt(loc+".id", "an id must be valid UTF-8")
	}
	if err := json.Unmarshal(idRaw, &op.ID); err != nil {
		return WriteOp{}, InvalidAt(loc+".id", "an id is a string")
	}
	if strings.ContainsRune(op.ID, utf8.RuneError) && !spellsReplacement(idRaw) {
		return WriteOp{}, InvalidAt(loc+".id", "an id must be valid UTF-8 (a lone surrogate escape is not)")
	}
	if meta.IfSeq != nil {
		if *meta.IfSeq < 1 || op.IfSeq == IfAbsent {
			return WriteOp{}, InvalidAt(loc+".if_seq", "if_seq is a seq of at least 1, and not with create")
		}
		op.IfSeq = *meta.IfSeq
	}
	if meta.Version != nil {
		if *meta.Version < 1 || *meta.Version > MaxVersion || op.IfSeq != 0 {
			return WriteOp{}, InvalidAt(loc+".version", "version is a whole number from 1 to %d, and excludes if_seq and create", int64(MaxVersion))
		}
		op.Version = *meta.Version
	}
	return op, nil
}

// spellsReplacement reports whether a JSON string spells U+FFFD itself, as the
// character or as its escape.
func spellsReplacement(raw []byte) bool {
	return bytes.Contains(raw, []byte(string(utf8.RuneError))) || bytes.Contains(bytes.ToLower(raw), []byte(`\ufffd`))
}
