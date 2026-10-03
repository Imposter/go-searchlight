package dialect

import "strings"

// Builders for engines that spell a batch as multi-row VALUES lists and IN
// lists with ? placeholders (SQLite and MySQL). They decide only how many
// rows go in one statement; the SQL around them is the engine's.

// Values returns head + "(?, ..., ?), (?, ..., ?)..." + tail statements over
// args, which holds ncols values per row. A statement takes at most
// maxParams arguments and, when maxBytes > 0, closes once its string and
// []byte arguments pass maxBytes (a single larger row still goes alone).
func Values(what, head, tail string, ncols, maxParams, maxBytes int, args []any) []Stmt {
	rows := len(args) / ncols
	per := max(1, maxParams/ncols)
	row := "(" + strings.TrimSuffix(strings.Repeat("?, ", ncols), ", ") + ")"
	var out []Stmt
	for start := 0; start < rows; {
		end, size := start, 0
		for end < rows && end-start < per && (maxBytes <= 0 || end == start || size < maxBytes) {
			size += argBytes(args[end*ncols : (end+1)*ncols])
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
		out = append(out, Stmt{What: what, SQL: b.String(), Args: args[start*ncols : end*ncols]})
		start = end
	}
	return out
}

// In returns head + "?, ?, ..." + tail statements, each with fixed's
// arguments first and then as many of ids as fit in maxParams.
func In(what, head, tail string, fixed []any, ids []string, maxParams int) []Stmt {
	per := max(1, maxParams-len(fixed))
	var out []Stmt
	for start := 0; start < len(ids); start += per {
		part := ids[start:min(start+per, len(ids))]
		args := make([]any, 0, len(fixed)+len(part))
		args = append(args, fixed...)
		for _, id := range part {
			args = append(args, id)
		}
		out = append(out, Stmt{What: what, SQL: head + strings.TrimSuffix(strings.Repeat("?, ", len(part)), ", ") + tail, Args: args})
	}
	return out
}

func argBytes(args []any) int {
	n := 0
	for _, a := range args {
		switch v := a.(type) {
		case string:
			n += len(v)
		case []byte:
			n += len(v)
		}
	}
	return n
}

// ChangeArgs flattens changelog rows into ChangeColumns order.
func ChangeArgs(rows []ChangeRow) []any {
	args := make([]any, 0, len(rows)*9)
	for i := range rows {
		r := &rows[i]
		args = append(args, r.Seq, r.Index, r.Shard, r.Kind, r.ID, r.Payload, r.At, r.IndexUID, r.MappingVersion)
	}
	return args
}

// DocumentArgs flattens document rows into DocumentColumns order.
func DocumentArgs(rows []DocumentRow) []any {
	args := make([]any, 0, len(rows)*5)
	for i := range rows {
		r := &rows[i]
		args = append(args, r.Index, r.Shard, r.ID, r.Body, r.Seq)
	}
	return args
}

// QueryArgs flattens saved-query rows into QueryColumns order.
func QueryArgs(rows []QueryRow) []any {
	args := make([]any, 0, len(rows)*6)
	for i := range rows {
		r := &rows[i]
		args = append(args, r.Index, r.Shard, r.ID, r.Query, r.Meta, r.Seq)
	}
	return args
}

// Column lists of the batch-written tables, in the order of the Args
// functions above.
const (
	ChangeColumns   = "seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version"
	DocumentColumns = "index_name, shard, id, body, seq"
	QueryColumns    = "index_name, shard, id, query, meta, seq"
)
