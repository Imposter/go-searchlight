package dialect

import (
	"strings"
	"testing"
)

func TestValues(t *testing.T) {
	args := make([]any, 0, 7*3)
	for i := range 7 {
		args = append(args, i, "x", []byte("yy"))
	}
	// 7 rows of 3 columns, at most 6 parameters a statement: 2 rows each.
	st := Values("w", "INSERT INTO t (a, b, c) VALUES ", " ON CONFLICT", 3, 6, 0, args)
	if len(st) != 4 {
		t.Fatalf("%d statements, want 4", len(st))
	}
	if st[0].SQL != "INSERT INTO t (a, b, c) VALUES (?, ?, ?), (?, ?, ?) ON CONFLICT" || len(st[0].Args) != 6 || st[0].What != "w" {
		t.Fatalf("first %+v", st[0])
	}
	if st[3].SQL != "INSERT INTO t (a, b, c) VALUES (?, ?, ?) ON CONFLICT" || st[3].Args[0] != 6 {
		t.Fatalf("last %+v", st[3])
	}
	// A byte budget closes a statement once passed: 3 bytes a row, 5 allowed.
	st = Values("w", "V ", "", 3, 1000, 5, args)
	if len(st) != 4 || len(st[0].Args) != 6 {
		t.Fatalf("byte budget: %d statements, first has %d args", len(st), len(st[0].Args))
	}
	if Values("w", "V ", "", 3, 1000, 0, nil) != nil {
		t.Fatal("no rows, but statements")
	}
}

func TestIn(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e"}
	st := In("d", "DELETE FROM t WHERE k = ? AND id IN (", ")", []any{"k"}, ids, 3)
	if len(st) != 3 {
		t.Fatalf("%d statements, want 3", len(st))
	}
	if st[0].SQL != "DELETE FROM t WHERE k = ? AND id IN (?, ?)" || len(st[0].Args) != 3 || st[0].Args[0] != "k" || st[0].Args[2] != "b" {
		t.Fatalf("first %+v", st[0])
	}
	if !strings.HasSuffix(st[2].SQL, "IN (?)") || st[2].Args[1] != "e" {
		t.Fatalf("last %+v", st[2])
	}
}
