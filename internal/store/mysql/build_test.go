package mysql

import (
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

func TestValuesParams(t *testing.T) {
	args := make([]any, 0, 7*3)
	for i := range 7 {
		args = append(args, i, "x", []byte("yy"))
	}
	// 7 rows of 3 columns, at most 6 parameters a statement: 2 rows each.
	st := values("w", "INSERT INTO t (a, b, c) VALUES ", " AS new", 3, dialect.Limits{Params: 6}, args)
	if len(st) != 4 {
		t.Fatalf("%d statements, want 4", len(st))
	}
	if st[0].SQL != "INSERT INTO t (a, b, c) VALUES (?, ?, ?), (?, ?, ?) AS new" || len(st[0].Args) != 6 || st[0].What != "w" {
		t.Fatalf("first %+v", st[0])
	}
	if st[3].SQL != "INSERT INTO t (a, b, c) VALUES (?, ?, ?) AS new" || st[3].Args[0] != 6 {
		t.Fatalf("last %+v", st[3])
	}
	if values("w", "V ", "", 3, dialect.Limits{}, nil) != nil {
		t.Fatal("no rows, but statements")
	}
	if st := values("w", "V ", "", 3, dialect.Limits{}, args); len(st) != 1 {
		t.Fatalf("no limits: %d statements", len(st))
	}
}

// The byte budget closes a statement before the row that would pass it,
// counting arguments escaped, and never splits a row.
func TestValuesBytes(t *testing.T) {
	body := strings.Repeat("b", 1000) // escapes to at most 2003 bytes
	var args []any
	for i := range 10 {
		args = append(args, i, body)
	}
	limit := 5000
	st := values("w", "INSERT INTO t (a, b) VALUES ", "", 2, dialect.Limits{Bytes: limit}, args)
	rows := 0
	for _, s := range st {
		n := len(s.Args) / 2
		rows += n
		if n < 1 {
			t.Fatal("an empty statement")
		}
		if size := len(s.SQL) + escapedSize(s.Args); n > 1 && size > limit {
			t.Fatalf("a %d-row statement of about %d bytes passes %d", n, size, limit)
		}
	}
	if rows != 10 || len(st) != 5 {
		t.Fatalf("%d rows in %d statements, want 10 in 5", rows, len(st))
	}
	// A row larger than the budget goes alone.
	if st := values("w", "V ", "", 2, dialect.Limits{Bytes: 100}, args); len(st) != 10 {
		t.Fatalf("oversized rows: %d statements", len(st))
	}
}

func TestIn(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e"}
	st := in("d", "DELETE FROM t WHERE k = ? AND id IN (", []any{"k"}, ids, dialect.Limits{Params: 3})
	if len(st) != 3 {
		t.Fatalf("%d statements, want 3", len(st))
	}
	if st[0].SQL != "DELETE FROM t WHERE k = ? AND id IN (?, ?)" || len(st[0].Args) != 3 || st[0].Args[0] != "k" || st[0].Args[2] != "b" {
		t.Fatalf("first %+v", st[0])
	}
	if !strings.HasSuffix(st[2].SQL, "IN (?)") || st[2].Args[1] != "e" {
		t.Fatalf("last %+v", st[2])
	}
	// The byte budget applies to deletes too.
	long := []string{strings.Repeat("x", 500), strings.Repeat("y", 500), strings.Repeat("z", 500)}
	if st := in("d", "DELETE FROM t WHERE id IN (", nil, long, dialect.Limits{Bytes: 1500}); len(st) != 3 {
		t.Fatalf("byte budget on deletes: %d statements, want 3", len(st))
	}
}
