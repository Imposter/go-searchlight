package percolate

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// shardModel drives a real shard with the percolator's index and keeps the saved
// queries it should hold, to check percolation against brute force at every step.
type shardModel struct {
	t       *testing.T
	dir     string
	s       *shard.Shard
	seq     int64
	queries map[string]query.Node // as applied
	visible map[string]query.Node // as of the last refresh
	p       *Percolator
}

func newShardModel(t *testing.T) *shardModel {
	t.Helper()
	m := &shardModel{t: t, dir: t.TempDir(), queries: map[string]query.Node{}, visible: map[string]query.Node{}, p: New(Options{Threads: 4})}
	m.open()
	t.Cleanup(func() {
		if m.s != nil {
			_ = m.s.Close(context.Background())
		}
	})
	return m
}

func (m *shardModel) open() {
	m.t.Helper()
	s, err := shard.Open(context.Background(), m.dir, testMapping(), shard.Options{
		QueryIndex: Index{}, RefreshInterval: -1, DisableMerges: true, RefreshBytes: -1,
	})
	if err != nil {
		m.t.Fatal(err)
	}
	m.s = s
}

func (m *shardModel) reopen() {
	m.t.Helper()
	if err := m.s.Close(context.Background()); err != nil {
		m.t.Fatal(err)
	}
	m.open()
	m.seq = max(m.seq, m.s.CommittedSeq())
}

func (m *shardModel) apply(changes ...shard.Change) {
	m.t.Helper()
	for i := range changes {
		m.seq++
		changes[i].Seq = m.seq
	}
	if err := m.s.Apply(context.Background(), changes); err != nil {
		m.t.Fatal(err)
	}
}

func (m *shardModel) putQuery(id, raw string) {
	m.t.Helper()
	n, problems := query.Parse([]byte(raw))
	if len(problems) > 0 {
		m.t.Fatalf("%s: %v", raw, problems)
	}
	m.apply(shard.Change{Kind: shard.QueryUpsert, QueryID: id, Query: n, Meta: []byte(`{"search":"` + id + `"}`)})
	m.queries[id] = n
}

func (m *shardModel) delQuery(id string) {
	m.t.Helper()
	m.apply(shard.Change{Kind: shard.QueryDelete, QueryID: id})
	delete(m.queries, id)
}

func (m *shardModel) putDoc(id, body string) {
	m.t.Helper()
	d, _, err := schema.Analyze(testMapping(), id, []byte(body))
	if err != nil {
		m.t.Fatal(err)
	}
	m.apply(shard.Change{Kind: shard.Upsert, Doc: &d})
}

func (m *shardModel) refresh() {
	m.t.Helper()
	if err := m.s.Refresh(context.Background()); err != nil {
		m.t.Fatal(err)
	}
	m.visible = maps.Clone(m.queries)
}

func (m *shardModel) merge() {
	m.t.Helper()
	if err := m.s.ForceMerge(context.Background(), 1); err != nil {
		m.t.Fatal(err)
	}
}

// check percolates docs (as bodies, analyzed by Percolate, and pre-analyzed) and
// compares with brute force over the model's queries.
func (m *shardModel) check(docs map[string]string) {
	m.t.Helper()
	g := m.s.Acquire()
	defer g.Release()
	if g.NumQueries() != uint64(len(m.visible)) {
		m.t.Fatalf("generation holds %d live queries, the model %d", g.NumQueries(), len(m.visible))
	}
	ids := slices.Sorted(func(yield func(string) bool) {
		for id := range docs {
			if !yield(id) {
				return
			}
		}
	})
	raw := make([]schema.Doc, len(ids))
	analyzed := make([]schema.Doc, len(ids))
	want := make([][]string, len(ids))
	for i, id := range ids {
		raw[i] = schema.Doc{ID: id, Body: []byte(docs[id])}
		d, _, err := schema.Analyze(testMapping(), id, []byte(docs[id]))
		if err != nil {
			m.t.Fatal(err)
		}
		analyzed[i] = d
		for qid, n := range m.visible {
			if query.Match(n, &d) {
				want[i] = append(want[i], qid)
			}
		}
		slices.Sort(want[i])
	}
	for _, in := range [][]schema.Doc{raw, analyzed} {
		got, err := m.p.Percolate(context.Background(), g, in)
		if err != nil {
			m.t.Fatal(err)
		}
		for i := range ids {
			if g := strs(m.t, got[i]); !slices.Equal(g, want[i]) {
				m.t.Fatalf("document %s: percolate %v, brute force %v", ids[i], g, want[i])
			}
		}
	}
}

// Review Focus 2: a saved query updated many times, deleted and re-added matches
// exactly once with its latest body, across refreshes, merges and a reopen.
func TestPercolateQueryUpdatesAndDeletes(t *testing.T) {
	m := newShardModel(t)
	docs := map[string]string{
		"d1": `{"brand":"Acme","title":"RTX 4090 Founders Edition","price":1999,"tags":["gpu","new"],"stock":true}`,
		"d2": `{"brand":"Zotac","title":"RTX 4080 Super","price":1199,"tags":"gpu, refurbished","stock":false}`,
		"d3": `{"brand":"acme","title":"Straße café ﬁne","price":5}`,
		"d4": `{"title":"x"}`,
	}
	for id, body := range docs {
		m.putDoc(id, body)
	}
	m.refresh()

	m.putQuery("cheap", `{"field":"price","op":"lt","value":100}`)
	m.putQuery("acme", `{"field":"brand","op":"eq","value":"ACME"}`)
	m.putQuery("not-refurb", `{"not":{"field":"tags","op":"has","value":"refurbished"}}`)
	m.check(docs)
	m.refresh()
	m.check(docs)

	// Update one query many times, across refreshes: only the latest version matches.
	for i, raw := range []string{
		`{"field":"title","op":"contains","value":"4090"}`,
		`{"field":"title","op":"contains","value":"4080"}`,
		`{"all":[{"field":"title","op":"words_any","value":["café","rtx"]},{"field":"price","op":"between","value":[1,2000]}]}`,
	} {
		m.putQuery("gpu", raw)
		if i%2 == 0 {
			m.refresh()
		}
		m.check(docs)
	}
	m.refresh()
	m.check(docs)

	// Delete, re-add, delete and re-add in one buffer and across refreshes.
	m.delQuery("acme")
	m.check(docs)
	m.refresh()
	m.check(docs)
	m.putQuery("acme", `{"field":"brand","op":"in","value":["zotac","acme"]}`)
	m.delQuery("acme")
	m.putQuery("acme", `{"field":"brand","op":"starts_with","value":"Zo"}`)
	m.refresh()
	m.check(docs)

	g := m.s.Acquire()
	segs := len(g.QuerySegments)
	g.Release()
	if segs < 3 {
		t.Fatalf("%d query segments before the merge, want several", segs)
	}
	m.merge()
	m.check(docs)
	g = m.s.Acquire()
	if len(g.QuerySegments) != 1 {
		t.Fatalf("%d query segments after ForceMerge(1)", len(g.QuerySegments))
	}
	g.Release()

	m.reopen()
	m.check(docs)

	// After the merge, update and delete again: the merged segment is masked.
	m.putQuery("cheap", `{"field":"price","op":"gte","value":1000}`)
	m.delQuery("not-refurb")
	m.refresh()
	m.check(docs)
	m.merge()
	m.check(docs)
}

// Random interleavings of query upserts, deletes, refreshes, merges and reopens, each
// step checked against brute force.
func TestPercolateRandomQueryChurn(t *testing.T) {
	m := newShardModel(t)
	g := newGen(42)
	g.positive = true
	docs := map[string]string{}
	for i := range 40 {
		id, body := g.docJSON(i)
		docs[id] = string(body)
		m.putDoc(id, string(body))
	}
	m.refresh()
	r := rand.New(rand.NewPCG(7, 7))
	for step := range 15 {
		for range 1 + r.IntN(20) {
			id := fmt.Sprintf("q%d", r.IntN(50))
			if r.IntN(4) == 0 {
				if _, ok := m.queries[id]; ok {
					m.delQuery(id)
				}
				continue
			}
			raw := g.queryJSON()
			if _, problems := query.Parse(raw); len(problems) > 0 {
				continue
			}
			m.putQuery(id, string(raw))
		}
		switch r.IntN(10) {
		case 0:
			m.merge()
		case 1:
			m.refresh()
			m.reopen()
		default:
			m.refresh()
		}
		if step%3 == 0 || step == 14 {
			m.check(docs)
		}
	}
}

func TestPercolateCancelled(t *testing.T) {
	m := newShardModel(t)
	m.putQuery("q", `{"field":"brand","op":"eq","value":"acme"}`)
	m.refresh()
	g := m.s.Acquire()
	defer g.Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	docs := []schema.Doc{{ID: "a", Body: []byte(`{"brand":"acme"}`)}, {ID: "b", Body: []byte(`{}`)}}
	if _, err := m.p.Percolate(ctx, g, docs); err == nil {
		t.Fatal("Percolate on a cancelled context succeeded")
	}
	got, err := m.p.Percolate(context.Background(), g, docs)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(strs(t, got[0]), []string{"q"}) || got[1] != nil {
		t.Fatalf("got %v", got)
	}
}

func TestPercolateRefusesUnanalyzableDocument(t *testing.T) {
	m := newShardModel(t)
	m.refresh()
	g := m.s.Acquire()
	defer g.Release()
	if _, err := m.p.Percolate(context.Background(), g, []schema.Doc{{ID: "a", Body: []byte(`[1]`)}}); err == nil {
		t.Fatal("a document that is not an object was percolated")
	}
	if _, err := m.p.Percolate(context.Background(), g, []schema.Doc{{ID: "", Body: []byte(`{}`)}}); err == nil {
		t.Fatal("a document with no id was percolated")
	}
}

// A shard whose query segments were built by DefaultQueryIndex (no anchors) still
// percolates exactly, by brute force.
func TestPercolateDefaultQueryIndexFallback(t *testing.T) {
	dir := t.TempDir()
	s, err := shard.Open(context.Background(), dir, testMapping(), shard.Options{RefreshInterval: -1, DisableMerges: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	n, _ := query.Parse([]byte(`{"field":"brand","op":"eq","value":"acme"}`))
	if err := s.Apply(context.Background(), []shard.Change{{Seq: 1, Kind: shard.QueryUpsert, QueryID: "q", Query: n}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	g := s.Acquire()
	defer g.Release()
	got, err := Percolate(context.Background(), g, []schema.Doc{{ID: "a", Body: []byte(`{"brand":"ACME"}`)}, {ID: "b", Body: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(strs(t, got[0]), []string{"q"}) || got[1] != nil {
		t.Fatalf("got %v", got)
	}
}
