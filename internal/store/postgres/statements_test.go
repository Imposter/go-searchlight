package postgres

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// writeSQL takes 46 arguments, each used once (the nonces are $31, the versions and
// tombstones, added later, $32 to $43, the body hashes $44, and the sequence rows $29,
// $30, $45 and $46).
func TestWriteSQLPlaceholders(t *testing.T) {
	var got []int
	for _, p := range regexp.MustCompile(`\$(\d+)`).FindAllStringSubmatch(writeSQL, -1) {
		n, err := strconv.Atoi(p[1])
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, n)
	}
	slices.Sort(got)
	want := make([]int, 46)
	for i := range want {
		want[i] = i + 1
	}
	if !slices.Equal(got, want) {
		t.Fatalf("placeholders %v", got)
	}
	w := &dialect.Write{Counters: []dialect.CounterRow{{Index: "i", Shard: 1, Value: 7, MappingVersion: 1}}}
	st := write(w, limits)
	if len(st) != 1 || len(st[0].Args) != 46 {
		t.Fatalf("an empty write: %d statements, %d args", len(st), len(st[0].Args))
	}
	for i, a := range st[0].Args {
		if a == nil {
			t.Fatalf("argument %d is nil", i+1)
		}
	}
}

// A write past the byte limit is split into parts that together carry every
// row once, in order, the deletes in the first part, each part setting the
// sequence rows.
func TestWriteParts(t *testing.T) {
	w := &dialect.Write{
		DocumentDeletes: []dialect.DeleteGroup{{Index: "i", Shard: 1, IDs: []string{"x", "y"}}},
		QueryDeletes:    []dialect.DeleteGroup{{Index: "i", Shard: 0, IDs: []string{"q"}}},
		Counters:        []dialect.CounterRow{{Index: "i", Shard: 0, Value: 99, MappingVersion: 3}},
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
		if st.SQL != writeSQL || !slices.Equal(column[string](t, a[28]), []string{"i"}) || !slices.Equal(column[int64](t, a[44]), []int64{99}) || !slices.Equal(column[int64](t, a[45]), []int64{3}) {
			t.Fatalf("part %d: sequence rows %v %v %v", p, a[28], a[44], a[45])
		}
		seqs = append(seqs, column[int64](t, a[0])...)
		docs = append(docs, column[string](t, a[12])...)
		wantDeletes := 0
		if p == 0 {
			wantDeletes = 2
		}
		if n := len(column[string](t, a[24])); n != wantDeletes {
			t.Errorf("part %d: %d document deletes", p, n)
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

// A connection refused past max_connections is told apart from other errors.
func TestTooManyConnections(t *testing.T) {
	if !tooManyConnections(fmt.Errorf("open: %w", &pgconn.PgError{Code: "53300"})) {
		t.Fatal("53300 is not too many connections")
	}
	if tooManyConnections(&pgconn.PgError{Code: "40001"}) || tooManyConnections(errors.New("x")) {
		t.Fatal("other errors are too many connections")
	}
}
