package percolate

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

var dateT0 = time.Date(2026, 10, 16, 13, 45, 30, 0, time.UTC)

// dateDocs are documents seen 6 and 8 days before dateT0, and one with no date.
func dateDocs() map[string]string {
	at := func(d time.Duration) string { return dateT0.Add(-d).Format(time.RFC3339) }
	return map[string]string{
		"six":   `{"brand":"acme","seen":"` + at(6*24*time.Hour) + `","price":5}`,
		"eight": `{"brand":"acme","seen":"` + at(8*24*time.Hour) + `","price":50}`,
		"today": `{"brand":"zotac","seen":"` + dateT0.Format(time.RFC3339) + `"}`,
		"none":  `{"brand":"acme"}`,
	}
}

var dateQueries = map[string]string{
	"week":      `{"field":"seen","op":"gte","value":"now-7d"}`,
	"acme-week": `{"all":[{"field":"brand","op":"eq","value":"acme"},{"field":"seen","op":"gte","value":"now-7d"}]}`,
	"cheap-old": `{"all":[{"field":"price","op":"lt","value":10},{"not":{"field":"seen","op":"gte","value":"now-7d/d"}}]}`,
	"today":     `{"all":[{"field":"seen","op":"gte","value":"now/d"},{"field":"seen","op":"lte","value":"now/d"}]}`,
	"window":    `{"field":"seen","op":"between","value":["now-7d/d","now/d"]}`,
	"either":    `{"any":[{"field":"brand","op":"eq","value":"zotac"},{"field":"seen","op":"lt","value":"now-7d"}]}`,
	"absolute":  `{"field":"seen","op":"gt","value":"2026-10-09"}`,
	"acme":      `{"field":"brand","op":"eq","value":"acme"}`,
}

// percolateAt percolates docs at now and compares with brute force: every visible
// query resolved at now, then matched.
func (m *shardModel) percolateAt(docs map[string]string, now time.Time) map[string][]string {
	m.t.Helper()
	g := m.s.Acquire()
	defer g.Release()
	ids := slices.Sorted(func(yield func(string) bool) {
		for id := range docs {
			if !yield(id) {
				return
			}
		}
	})
	in := make([]schema.Doc, len(ids))
	for i, id := range ids {
		in[i] = schema.Doc{ID: id, Body: []byte(docs[id])}
	}
	got, err := m.p.Percolate(query.WithNow(context.Background(), now), g, in)
	if err != nil {
		m.t.Fatal(err)
	}
	out := map[string][]string{}
	for i, id := range ids {
		d, _, err := schema.Analyze(testMapping(), id, []byte(docs[id]))
		if err != nil {
			m.t.Fatal(err)
		}
		var want []string
		for qid, n := range m.visible {
			resolved, _ := query.Resolve(n, now)
			if query.Match(resolved, &d) {
				want = append(want, qid)
			}
		}
		slices.Sort(want)
		if g := strs(m.t, got[i]); !slices.Equal(g, want) {
			m.t.Fatalf("document %s at %s: percolate %v, brute force %v", id, now, g, want)
		}
		out[id] = want
	}
	return out
}

// A saved query keeps its date math: each percolation resolves it at its own now, the
// query never saved again, through refreshes and a merge.
func TestPercolateDateMathOverTime(t *testing.T) {
	m := newShardModel(t)
	for id, raw := range dateQueries {
		m.putQuery(id, raw)
	}
	m.refresh()
	docs := dateDocs()

	at0 := m.percolateAt(docs, dateT0)
	if !slices.Contains(at0["six"], "week") || slices.Contains(at0["eight"], "week") {
		t.Fatalf("at t0: six %v, eight %v", at0["six"], at0["eight"])
	}
	if !slices.Contains(at0["six"], "acme-week") || slices.Contains(at0["six"], "cheap-old") {
		t.Fatalf("at t0: six %v", at0["six"])
	}
	if !slices.Contains(at0["today"], "today") || slices.Contains(at0["six"], "today") {
		t.Fatalf("at t0: today %v", at0["today"])
	}

	later := dateT0.Add(2 * 24 * time.Hour)
	at2 := m.percolateAt(docs, later)
	if slices.Contains(at2["six"], "week") || slices.Contains(at2["six"], "acme-week") || !slices.Contains(at2["six"], "cheap-old") {
		t.Fatalf("two days on, six is eight days old: %v", at2["six"])
	}
	if slices.Contains(at2["today"], "today") || !slices.Contains(at2["today"], "week") {
		t.Fatalf("two days on: today %v", at2["today"])
	}

	m.putQuery("late", `{"field":"seen","op":"lt","value":"now-1d/d"}`)
	m.refresh()
	m.merge()
	for _, now := range []time.Time{dateT0, later, dateT0.Add(-30 * 24 * time.Hour), dateT0.Add(400 * 24 * time.Hour)} {
		m.percolateAt(docs, now)
	}
}

// Without a now on its context, a percolation resolves at its own time.
func TestPercolateDateMathDefaultsToTheCallsTime(t *testing.T) {
	m := newShardModel(t)
	m.putQuery("recent", `{"field":"seen","op":"gte","value":"now-1h"}`)
	m.refresh()
	g := m.s.Acquire()
	defer g.Release()
	fresh := `{"seen":"` + time.Now().UTC().Format(time.RFC3339) + `"}`
	stale := `{"seen":"` + time.Now().UTC().Add(-2*time.Hour).Format(time.RFC3339) + `"}`
	got, err := m.p.Percolate(context.Background(), g, []schema.Doc{{ID: "a", Body: []byte(fresh)}, {ID: "b", Body: []byte(stale)}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(strs(t, got[0]), []string{"recent"}) || got[1] != nil {
		t.Fatalf("got %s, %s", got[0], got[1])
	}
}

// Brute force over another builder's segments resolves date math at the call's now too.
func TestPercolateDateMathDefaultQueryIndex(t *testing.T) {
	s, err := shard.Open(context.Background(), t.TempDir(), testMapping(), shard.Options{RefreshInterval: -1, DisableMerges: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	n, _ := query.Parse([]byte(dateQueries["week"]))
	if err := s.Apply(context.Background(), []shard.Change{{Seq: 1, Kind: shard.QueryUpsert, QueryID: "week", Query: n}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	g := s.Acquire()
	defer g.Release()
	doc := []schema.Doc{{ID: "six", Body: []byte(dateDocs()["six"])}}
	for _, tc := range []struct {
		now  time.Time
		want int
	}{{dateT0, 1}, {dateT0.Add(2 * 24 * time.Hour), 0}} {
		got, err := New(Options{}).Percolate(query.WithNow(context.Background(), tc.now), g, doc)
		if err != nil {
			t.Fatal(err)
		}
		if got[0].Len() != tc.want {
			t.Fatalf("at %s: %s", tc.now, got[0])
		}
	}
}

// Date math anchors on its field's presence: never a range, a posting filter or a
// proven condition, which would hold an instant.
func TestDateMathAnchorsHoldNoInstant(t *testing.T) {
	for _, raw := range dateQueries {
		n, _ := query.Parse([]byte(raw))
		ex := newExtractor(nil)
		set := ex.node(n)
		var dated []query.Node
		query.Walk(n, func(_ string, node query.Node) bool {
			if l, ok := node.(*query.Leaf); ok && l.IsDateMath() {
				dated = append(dated, l)
			}
			return true
		})
		for _, r := range set.ranges {
			if r.Field == "seen" {
				t.Errorf("%s: anchored on a range of seen", raw)
			}
		}
		filter, filtered := postingFilter(n, &set)
		if filtered && filter.field == "seen" {
			t.Errorf("%s: a posting filter on seen", raw)
		}
		_, proven := provenRest(n, &set, filter, filtered)
		for _, l := range dated {
			if proven != "" && strings.Contains(proven, string(query.Canonical(l))) {
				t.Errorf("%s: date math proven", raw)
			}
		}
	}
}

// Date programs decide as the matcher does on the tree resolved at the same now, for
// every op, bound and rounding, at instants around the documents' dates.
func TestDateProgramsMatchTheResolvedMatcher(t *testing.T) {
	exprs := []string{
		"now", "now-7d", "now-7d/d", "now/d", "now/w", "now/M", "now-1M/M", "now+1h/h", "now/y",
		"now-90m", "2026-10-09", "2026-10-09T00:00:00Z", "2026-01-31||+1M", "2026-10-09||/w",
		"now+300000y",
	}
	ops := []string{query.OpLt, query.OpLte, query.OpGt, query.OpGte}
	r := rand.New(rand.NewPCG(7, 7))
	bound := func() string {
		if r.IntN(4) == 0 {
			return fmt.Sprint(dateT0.Add(-time.Duration(r.IntN(20*24)) * time.Hour).UnixMilli())
		}
		return fmt.Sprintf("%q", exprs[r.IntN(len(exprs))])
	}
	leaf := func() string {
		if r.IntN(5) == 0 {
			return fmt.Sprintf(`{"field":"seen","op":"between","value":[%s,%s]}`, bound(), bound())
		}
		return fmt.Sprintf(`{"field":"seen","op":%q,"value":%s}`, ops[r.IntN(len(ops))], bound())
	}
	var queries []query.Node
	for range 300 {
		raw := leaf()
		switch r.IntN(4) {
		case 0:
			raw = `{"not":` + raw + `}`
		case 1:
			raw = `{"all":[` + raw + `,` + leaf() + `]}`
		case 2:
			raw = `{"any":[` + raw + `,{"field":"brand","op":"eq","value":"acme"}]}`
		}
		n, problems := query.Parse([]byte(raw))
		if len(problems) > 0 {
			t.Fatal(problems)
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
	var docs []schema.Doc
	for h := -20 * 24; h <= 2*24; h += 7 {
		body := fmt.Sprintf(`{"brand":"x","seen":%d}`, dateT0.Add(time.Duration(h)*time.Hour).UnixMilli()+int64(r.IntN(3)-1))
		d, _, err := schema.Analyze(testMapping(), "d", []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		docs = append(docs, d)
	}
	docs = append(docs, schema.Doc{Fields: map[string]schema.Value{}})
	sc := new(scratch)
	for _, now := range []time.Time{dateT0, dateT0.Add(36 * time.Hour), dateT0.Add(-17*24*time.Hour - time.Millisecond)} {
		sc.now = now.UnixMilli()
		for i, n := range queries {
			resolved, _ := query.Resolve(n, now)
			m := query.Compile(resolved)
			for k := range docs {
				if got, want := evalOn(ft, sc, progs[i], &docs[k]), m.Match(&docs[k]); got != want {
					t.Fatalf("at %s, query %s on %v: program %v, matcher %v", now, query.Canonical(n), docs[k].Fields, got, want)
				}
			}
		}
	}
}

// A date program cut short or holding an unknown op or bound kind is refused.
func TestCheckProgRefusesBadDateOperands(t *testing.T) {
	ft := &fieldTable{at: map[string]uint32{}}
	pc := progCompiler{field: ft.field}
	n, _ := query.Parse([]byte(`{"field":"seen","op":"between","value":[5,"now/d"]}`))
	p := pc.compile(n)
	if p[0] != pDate {
		t.Fatalf("tag %d", p[0])
	}
	if err := checkProg(p, 1); err != nil {
		t.Fatal(err)
	}
	for cut := 1; cut < len(p); cut++ {
		if checkProg(p[:cut], 1) == nil {
			t.Errorf("cut at %d of %d accepted", cut, len(p))
		}
	}
	for _, at := range []int{2, 3} {
		bad := slices.Clone(p)
		bad[at] = 9
		if checkProg(bad, 1) == nil {
			t.Errorf("byte %d of 9 accepted", at)
		}
	}
}

// A date math bound that reads no now is resolved when the segment is built: it
// anchors on a range and compiles to the range tags, as a number would.
func TestFixedDateMathIndexesAsARange(t *testing.T) {
	n, _ := query.Parse([]byte(`{"field":"seen","op":"gt","value":"2026-10-09||/M"}`))
	set := newExtractor(nil).node(query.ResolveFixed(n))
	if len(set.terms) != 0 || len(set.ranges) != 1 || set.ranges[0].Field != "seen" ||
		set.ranges[0].Lo != float64(time.Date(2026, 10, 31, 23, 59, 59, 999e6, time.UTC).UnixMilli()) {
		t.Fatalf("anchors %+v", set)
	}
	ft := &fieldTable{at: map[string]uint32{}}
	pc := progCompiler{field: ft.field}
	for _, raw := range []string{
		`{"field":"seen","op":"gt","value":"2026-10-09||/M"}`,
		`{"field":"seen","op":"between","value":["2026-10-01","2026-10-31"]}`,
	} {
		q, _ := query.Parse([]byte(raw))
		if p := pc.compile(q); p[0] == pDate {
			t.Errorf("%s compiles to a date leaf", raw)
		}
	}
	q, _ := query.Parse([]byte(`{"field":"seen","op":"between","value":["2026-10-01","now"]}`))
	if p := pc.compile(q); p[0] != pDate {
		t.Errorf("a bound reading now compiles to tag %d", p[0])
	}
}

// The upgrade note's claim: a query segment holding no date math is one an older build
// (v1.0.0, which knew the program tags below pDate) reads as it is, in the same format;
// one holding a now-relative condition has a program that build refuses, so it treats
// the file as corrupt.
func TestOlderBuildsAndDateMathSegments(t *testing.T) {
	if FormatName != "percolate/3" || formatVersion != 3 {
		t.Fatalf("format %s (%d): the upgrade note describes percolate/3", FormatName, formatVersion)
	}
	if pDate != pBox-1 || pBox != numProgTags-1 {
		t.Fatal("pDate and pBox are not the newest program tags, in that order")
	}
	build := func(raws ...string) *Segment {
		t.Helper()
		var qs []shard.StoredQuery
		for i, raw := range raws {
			n, problems := query.Parse([]byte(raw))
			if len(problems) > 0 {
				t.Fatal(problems)
			}
			qs = append(qs, shard.StoredQuery{ID: fmt.Sprintf("q%d", i), Seq: int64(i + 1), Query: n})
		}
		data, err := encodeSegment(context.Background(), qs, nil)
		if err != nil {
			t.Fatal(err)
		}
		seg, err := openData("test.perc", data)
		if err != nil {
			t.Fatal(err)
		}
		return seg
	}
	refused := func(seg *Segment) int {
		bad := 0
		for r := range seg.NumQueries() {
			if p := seg.program(r); len(p) > 0 && checkProgTags(p, uint32(len(seg.fields)), pDate) != nil {
				bad++
			}
		}
		return bad
	}
	plain := build(`{"field":"price","op":"lt","value":10}`, `{"all":[{"field":"brand","op":"eq","value":"acme"},{"field":"seen","op":"gte","value":"2026-10-01||/M"}]}`)
	if n := refused(plain); n != 0 {
		t.Fatalf("an older build refuses %d programs of a segment with no now-relative date math", n)
	}
	dated := build(`{"field":"price","op":"lt","value":10}`, dateQueries["acme-week"])
	if n := refused(dated); n != 1 {
		t.Fatalf("an older build refuses %d programs of a segment with one now-relative query, want 1", n)
	}
}
