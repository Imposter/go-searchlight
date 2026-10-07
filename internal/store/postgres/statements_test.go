package postgres

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// writeSQL takes 30 arguments, numbered once each.
func TestWriteSQLPlaceholders(t *testing.T) {
	got := regexp.MustCompile(`\$\d+`).FindAllString(writeSQL, -1)
	want := make([]string, 30)
	for i := range want {
		want[i] = fmt.Sprintf("$%d", i+1)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("placeholders %v", got)
	}
	w := &dialect.Write{Counter: 7}
	st := write(w, limits)
	if len(st) != 1 || len(st[0].Args) != 30 {
		t.Fatalf("an empty write: %d statements, %d args", len(st), len(st[0].Args))
	}
	for i, a := range st[0].Args {
		if a == nil {
			t.Fatalf("argument %d is nil", i+1)
		}
	}
}

// A write past the byte limit is split into parts that together carry every
// row once, in order, the deletes in the first part and the notifications in
// the last, each part setting the counter.
func TestWriteParts(t *testing.T) {
	w := &dialect.Write{
		DocumentDeletes: []dialect.DeleteGroup{{Index: "i", Shard: 1, IDs: []string{"x", "y"}}},
		QueryDeletes:    []dialect.DeleteGroup{{Index: "i", Shard: 0, IDs: []string{"q"}}},
		Counter:         99,
		Notify:          []string{"n1", "n2"},
	}
	for i := range 10 {
		w.Changes = append(w.Changes, dialect.ChangeRow{Seq: int64(90 + i), Index: "i", ID: fmt.Sprint("c", i), Payload: strings.Repeat("p", 40)})
	}
	for i := range 3 {
		w.Documents = append(w.Documents, dialect.DocumentRow{Index: "i", ID: fmt.Sprint("d", i), Body: strings.Repeat("b", 40), Seq: int64(i)})
	}
	parts := write(w, dialect.Limits{Bytes: 120}) // three 40-byte rows a part
	if len(parts) != 4 {
		t.Fatalf("%d parts, want 4", len(parts))
	}
	var seqs []int64
	var docs []string
	for p, st := range parts {
		a := st.Args
		if st.SQL != writeSQL || a[28] != int64(99) {
			t.Fatalf("part %d: counter %v", p, a[28])
		}
		seqs = append(seqs, column[int64](t, a[0])...)
		docs = append(docs, column[string](t, a[12])...)
		wantDeletes, wantNotify := 0, 0
		if p == 0 {
			wantDeletes = 2
		}
		if p == len(parts)-1 {
			wantNotify = 2
		}
		if n := len(column[string](t, a[24])); n != wantDeletes {
			t.Errorf("part %d: %d document deletes", p, n)
		}
		if n := len(column[string](t, a[29])); n != wantNotify {
			t.Errorf("part %d: %d notifications", p, n)
		}
	}
	if !slices.Equal(seqs, []int64{90, 91, 92, 93, 94, 95, 96, 97, 98, 99}) {
		t.Errorf("seqs %v", seqs)
	}
	if !slices.Equal(docs, []string{"d0", "d1", "d2"}) {
		t.Errorf("documents %v", docs)
	}
	if len(write(w, limits)) != 1 || len(write(w, dialect.Limits{})) != 1 {
		t.Error("a small write is not one statement")
	}
}

// column is one of writeSQL's array arguments.
func column[T any](t *testing.T, arg any) []T {
	t.Helper()
	c, ok := arg.([]T)
	if !ok {
		t.Fatalf("argument %T, want %T", arg, c)
	}
	return c
}
