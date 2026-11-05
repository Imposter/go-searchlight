package percolate

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// randomDateBox is a box on spot (x a number, y a date): each axis absent or bounded on
// one side or both, each end open or closed, y's bounds numbers or date math.
func randomDateBox(r *rand.Rand) string {
	exprs := []string{"now", "now-7d", "now-7d/d", "now/d", "now/w", "now-1M/M", "now+1h/h", "now-90m", "2026-10-09", "2026-10-09T00:00:00Z", "now+300000y"}
	yBound := func() string {
		if r.IntN(4) == 0 {
			return fmt.Sprint(dateT0.Add(-time.Duration(r.IntN(20*24)) * time.Hour).UnixMilli())
		}
		return fmt.Sprintf("%q", exprs[r.IntN(len(exprs))])
	}
	xBound := func() string { return fmt.Sprint(r.IntN(12) - 1) }
	axis := func(bound func() string) string {
		lower := fmt.Sprintf("%q:%s", []string{"gt", "gte"}[r.IntN(2)], bound())
		upper := fmt.Sprintf("%q:%s", []string{"lt", "lte"}[r.IntN(2)], bound())
		switch r.IntN(3) {
		case 0:
			return "{" + lower + "}"
		case 1:
			return "{" + upper + "}"
		}
		return "{" + lower + "," + upper + "}"
	}
	var parts []string
	if r.IntN(4) > 0 {
		parts = append(parts, `"x":`+axis(xBound))
	}
	if r.IntN(5) > 0 {
		parts = append(parts, `"y":`+axis(yBound))
	}
	out := "{"
	for i, p := range parts {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return `{"field":"spot","op":"box","value":` + out + `}}`
}

// spotDocs are documents of up to four points: x small, y around dateT0.
func spotDocs(t *testing.T, r *rand.Rand, n int) []schema.Doc {
	t.Helper()
	var docs []schema.Doc
	for i := range n {
		pairs := ""
		for k := range r.IntN(5) {
			if k > 0 {
				pairs += ","
			}
			pairs += fmt.Sprintf("[%d,%d]", r.IntN(10), dateT0.Add(time.Duration(r.IntN(22*24)-20*24)*time.Hour).UnixMilli()+int64(r.IntN(3)-1))
		}
		d, _, err := schema.Analyze(testMapping(), fmt.Sprint("d", i), []byte(`{"brand":"x","spot":[`+pairs+`]}`))
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, d)
	}
	return append(docs, schema.Doc{Fields: map[string]schema.Value{}})
}

// Box programs decide as the matcher does on the tree resolved at the same now, for
// boxes of every shape, end and rounding, at instants around the documents' points.
func TestBoxProgramsMatchTheResolvedMatcher(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	var queries []query.Node
	for len(queries) < 400 {
		raw := randomDateBox(r)
		switch r.IntN(4) {
		case 0:
			raw = `{"not":` + raw + `}`
		case 1:
			raw = `{"all":[` + raw + `,` + randomDateBox(r) + `]}`
		case 2:
			raw = `{"any":[` + raw + `,{"field":"brand","op":"eq","value":"acme"}]}`
		}
		n, problems := query.Parse([]byte(raw))
		if len(problems) > 0 {
			t.Fatal(raw, problems)
		}
		queries = append(queries, n)
	}
	ft := &fieldTable{at: map[string]uint32{}}
	pc := progCompiler{field: ft.field}
	progs := make([][]byte, len(queries))
	for i, n := range queries {
		progs[i] = pc.compile(n)
		if err := checkProg(progs[i], uint32(len(ft.names))); err != nil {
			t.Fatal(err)
		}
	}
	docs := spotDocs(t, r, 80)
	sc := new(scratch)
	matched := 0
	for _, now := range []time.Time{dateT0, dateT0.Add(36 * time.Hour), dateT0.Add(-17*24*time.Hour - time.Millisecond)} {
		sc.now = now.UnixMilli()
		for i, n := range queries {
			resolved, _ := query.Resolve(n, now)
			m := query.Compile(resolved)
			for k := range docs {
				got, want := evalOn(ft, sc, progs[i], &docs[k]), m.Match(&docs[k])
				if got != want {
					t.Fatalf("at %s, query %s on %v: program %v, matcher %v", now, query.Canonical(n), docs[k].Fields, got, want)
				}
				if got {
					matched++
				}
			}
		}
	}
	if matched < 1000 {
		t.Fatalf("only %d matches: the boxes are too narrow to test anything", matched)
	}
}

// Saved boxes percolate exactly at each now, through refreshes and a merge: a box
// anchors on its field's presence and holds no instant.
func TestPercolateBoxesOverTime(t *testing.T) {
	m := newShardModel(t)
	r := rand.New(rand.NewPCG(8, 8))
	for i := range 60 {
		m.putQuery(fmt.Sprintf("b%02d", i), randomDateBox(r))
		if i == 30 {
			m.refresh()
		}
	}
	m.putQuery("week5", `{"field":"spot","op":"box","value":{"x":{"gte":5},"y":{"gte":"now-7d/d"}}}`)
	m.refresh()
	docs := map[string]string{}
	for i := range 30 {
		pairs := ""
		for k := range 1 + r.IntN(4) {
			if k > 0 {
				pairs += ","
			}
			pairs += fmt.Sprintf(`[%d,%q]`, r.IntN(10), dateT0.Add(-time.Duration(r.IntN(20*24))*time.Hour).Format(time.RFC3339))
		}
		docs[fmt.Sprint("d", i)] = `{"spot":[` + pairs + `]}`
	}
	docs["split"] = fmt.Sprintf(`{"spot":[[9,%q],[1,%q]]}`, dateT0.Add(-30*24*time.Hour).Format(time.RFC3339), dateT0.Format(time.RFC3339))
	docs["fresh"] = fmt.Sprintf(`{"spot":[6,%q]}`, dateT0.Add(-time.Hour).Format(time.RFC3339))
	at0 := m.percolateAt(docs, dateT0)
	if slices.Contains(at0["split"], "week5") || !slices.Contains(at0["fresh"], "week5") {
		t.Fatalf("at t0: split %v, fresh %v", at0["split"], at0["fresh"])
	}
	m.merge()
	later := m.percolateAt(docs, dateT0.Add(8*24*time.Hour))
	if slices.Contains(later["fresh"], "week5") {
		t.Fatalf("eight days on, fresh still matches week5: %v", later["fresh"])
	}
	for _, now := range []time.Time{dateT0.Add(-3 * 24 * time.Hour), dateT0.Add(40 * 24 * time.Hour)} {
		m.percolateAt(docs, now)
	}
}
