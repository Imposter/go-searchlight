package mysql

import (
	"strings"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// Batch statements are multi-row VALUES lists and IN lists of ? placeholders,
// split so that none passes the limits: l.Params placeholders, and about
// l.Bytes of the statement the driver sends. The driver interpolates the
// arguments into the statement text (InterpolateParams), escaping them, so an
// argument counts as its escaped size: up to twice its length, plus quotes.

// values returns head + "(?, ..., ?), (?, ..., ?)..." + tail statements over
// args, which holds ncols values per row. A statement closes before the row
// that would pass a limit; a single row larger than l.Bytes goes alone (the
// driver then sends it as a prepared statement, which max_allowed_packet must
// still fit).
func values(what, head, tail string, ncols int, l dialect.Limits, args []any) []dialect.Stmt {
	rows := len(args) / ncols
	row := "(" + strings.TrimSuffix(strings.Repeat("?, ", ncols), ", ") + ")"
	var out []dialect.Stmt
	for start := 0; start < rows; {
		end, size := start, len(head)+len(tail)
		for end < rows {
			sz := escapedSize(args[end*ncols:(end+1)*ncols]) + len(row) + 2
			if end > start && (over(l.Params, (end-start+1)*ncols) || over(l.Bytes, size+sz)) {
				break
			}
			size += sz
			end++
		}
		var b strings.Builder
		b.Grow(len(head) + (end-start)*(len(row)+2) + len(tail))
		b.WriteString(head)
		for i := start; i < end; i++ {
			if i > start {
				b.WriteString(", ")
			}
			b.WriteString(row)
		}
		b.WriteString(tail)
		out = append(out, dialect.Stmt{What: what, SQL: b.String(), Args: args[start*ncols : end*ncols]})
		start = end
	}
	return out
}

// in returns head + "?, ?, ...)" statements (head ends in "IN ("), each with
// fixed's arguments first and then as many of ids as the limits allow.
func in(what, head string, fixed []any, ids []string, l dialect.Limits) []dialect.Stmt {
	var out []dialect.Stmt
	base := len(head) + 1 + escapedSize(fixed)
	for start := 0; start < len(ids); {
		end, size := start, base
		for end < len(ids) {
			sz := 2*len(ids[end]) + 5
			if end > start && (over(l.Params, len(fixed)+end-start+1) || over(l.Bytes, size+sz)) {
				break
			}
			size += sz
			end++
		}
		args := make([]any, 0, len(fixed)+end-start)
		args = append(args, fixed...)
		for _, id := range ids[start:end] {
			args = append(args, id)
		}
		out = append(out, dialect.Stmt{What: what, SQL: head + strings.TrimSuffix(strings.Repeat("?, ", end-start), ", ") + ")", Args: args})
		start = end
	}
	return out
}

// over reports whether n passes limit; a limit of 0 is no limit.
func over(limit, n int) bool { return limit > 0 && n > limit }

// escapedSize is about how many bytes args take interpolated: strings and
// bytes escaped (at most doubled) and quoted, numbers in decimal.
func escapedSize(args []any) int {
	n := 0
	for _, a := range args {
		switch v := a.(type) {
		case string:
			n += 2*len(v) + 3
		case []byte:
			n += 2*len(v) + 10 // _binary'...'
		default:
			n += 20
		}
	}
	return n
}

// Column lists of the batch-written tables, in the order of the args
// functions below.
const (
	changeColumns   = "seq, index_name, shard, kind, id, payload, payload_z, at, index_uid, mapping_version"
	documentColumns = "index_name, shard, id, body, body_z, seq"
	queryColumns    = "index_name, shard, id, query, meta, seq"
)

func changeArgs(rows []dialect.ChangeRow) []any {
	args := make([]any, 0, len(rows)*10)
	for i := range rows {
		r := &rows[i]
		args = append(args, r.Seq, r.Index, r.Shard, r.Kind, r.ID, r.Payload, dialect.Blob(r.PayloadZ), r.At, r.IndexUID, r.MappingVersion)
	}
	return args
}

func documentArgs(rows []dialect.DocumentRow) []any {
	args := make([]any, 0, len(rows)*6)
	for i := range rows {
		r := &rows[i]
		args = append(args, r.Index, r.Shard, r.ID, r.Body, dialect.Blob(r.BodyZ), r.Seq)
	}
	return args
}

func queryArgs(rows []dialect.QueryRow) []any {
	args := make([]any, 0, len(rows)*6)
	for i := range rows {
		r := &rows[i]
		args = append(args, r.Index, r.Shard, r.ID, r.Query, r.Meta, r.Seq)
	}
	return args
}
