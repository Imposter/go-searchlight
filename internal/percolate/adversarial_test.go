package percolate

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// rgen generates adversarial root conjunctions and documents: boundary numbers,
// Unicode-folded, NUL and U+FFFD texts, missing and wrong-type fields, and not and ne
// inside any.
type rgen struct{ r *rand.Rand }

var rvVocab = []string{
	"acme", "ACME", "Straße", "STRASSE", "strasse", "ﬁne", "fine", "İstanbul", "i̇stanbul", "istanbul",
	"�", "a�b", "x", "", " ", "日本語", "é", "é", "4090", "rtx 4090", "rtx",
}

var rvTitleWords = []string{"rtx", "4090", "ti", "fine", "ﬁne", "straße", "strasse", "�", "oled", "4k", "new", "york", "a", "ab"}

var rvNums = []any{
	json.Number("-0"), json.Number("0"), json.Number("1"), json.Number("9.99"), json.Number("10"),
	json.Number("10.000000000000002"), json.Number("1e308"), json.Number("-1e308"), json.Number("5e-324"),
	json.Number("-5e-324"), json.Number("1759363200000"), json.Number("1759363199999"), json.Number("100"),
}

func (g *rgen) pick(list []string) string { return list[g.r.IntN(len(list))] }

func (g *rgen) num() any { return rvNums[g.r.IntN(len(rvNums))] }

func (g *rgen) phrase() string {
	n := 1 + g.r.IntN(3)
	s := ""
	for i := range n {
		if i > 0 {
			s += []string{" ", "-", "  ", "_"}[g.r.IntN(4)]
		}
		s += g.pick(rvTitleWords)
	}
	return s
}

func (g *rgen) needle() string {
	p := []rune(g.phrase())
	i := g.r.IntN(len(p) + 1)
	j := i + g.r.IntN(len(p)-i+1)
	return string(p[i:j])
}

func (g *rgen) strList(n int, one func() string) any {
	if g.r.IntN(3) == 0 {
		return one()
	}
	out := make([]any, 1+g.r.IntN(n))
	for i := range out {
		if g.r.IntN(10) == 0 {
			out[i] = g.num()
		} else {
			out[i] = one()
		}
	}
	return out
}

func (g *rgen) leaf() map[string]any {
	numField := []string{"price", "seen"}[g.r.IntN(2)]
	switch g.r.IntN(17) {
	case 0:
		return map[string]any{"field": []string{"brand", "_id", "title"}[g.r.IntN(3)], "op": "eq", "value": g.pick(rvVocab)}
	case 1:
		return map[string]any{"field": "brand", "op": "in", "value": []any{g.pick(rvVocab), g.pick(rvVocab), g.num(), true}}
	case 2:
		return map[string]any{"field": "stock", "op": "eq", "value": g.r.IntN(2) == 0}
	case 3:
		return map[string]any{"field": "stock", "op": "in", "value": []any{g.r.IntN(2) == 0}}
	case 4, 5:
		op := []string{"lt", "lte", "gt", "gte", "eq"}[g.r.IntN(5)]
		return map[string]any{"field": numField, "op": op, "value": g.num()}
	case 6:
		return map[string]any{"field": numField, "op": "between", "value": []any{g.num(), g.num()}}
	case 7:
		op := []string{"has", "has_any", "has_all"}[g.r.IntN(3)]
		return map[string]any{"field": "tags", "op": op, "value": g.strList(3, func() string { return g.pick(rvVocab) })}
	case 8:
		v := []any{true, false, "false", nil}[g.r.IntN(4)]
		l := map[string]any{"field": []string{"brand", "tags", "price", "missing"}[g.r.IntN(4)], "op": "exists"}
		if v != nil {
			l["value"] = v
		}
		return l
	case 9:
		return map[string]any{"field": "tags", "op": []string{"nonempty", "empty"}[g.r.IntN(2)]}
	case 10:
		return map[string]any{"field": "title", "op": []string{"words_any", "words_all"}[g.r.IntN(2)], "value": g.strList(3, g.phrase)}
	case 11:
		return map[string]any{"field": []string{"title", "brand", "_id"}[g.r.IntN(3)], "op": []string{"contains", "contains_any", "contains_all"}[g.r.IntN(3)], "value": g.strList(3, g.needle)}
	case 12:
		return map[string]any{"field": "title", "op": "starts_with", "value": g.needle()}
	case 13:
		return map[string]any{"field": []string{"brand", "stock", "price"}[g.r.IntN(3)], "op": "ne", "value": []any{g.pick(rvVocab), true, g.num()}[g.r.IntN(3)]}
	case 14:
		return map[string]any{"field": "title", "op": "similar", "value": map[string]any{"text": g.phrase(), "min": []any{0.2, 0.5, 1}[g.r.IntN(3)]}}
	case 15:
		return map[string]any{"field": "extra", "op": []string{"eq", "gte", "has"}[g.r.IntN(3)], "value": []any{g.pick(rvVocab), g.num()}[g.r.IntN(2)]}
	default:
		return map[string]any{"field": "brand", "op": "eq", "value": g.pick(rvVocab[:4])}
	}
}

// child is a leaf, or an any holding a not / ne next to a leaf, or a double not.
func (g *rgen) child() any {
	switch g.r.IntN(8) {
	case 0:
		return map[string]any{"any": []any{map[string]any{"not": g.leaf()}, g.leaf()}}
	case 1:
		return map[string]any{"not": map[string]any{"not": g.leaf()}}
	case 2:
		return map[string]any{"any": []any{g.leaf(), g.leaf()}}
	default:
		return g.leaf()
	}
}

// query returns a root conjunction (where filters and proven leaves apply), sometimes a
// lone leaf, and the same children shuffled ("" for a leaf): a second member of its
// class, whose posting filter may differ.
func (g *rgen) query() (string, string) {
	if g.r.IntN(6) == 0 {
		raw, _ := json.Marshal(g.leaf())
		return string(raw), ""
	}
	children := make([]any, 1+g.r.IntN(5))
	for i := range children {
		children[i] = g.child()
	}
	raw, _ := json.Marshal(map[string]any{"all": children})
	g.r.Shuffle(len(children), func(i, j int) { children[i], children[j] = children[j], children[i] })
	perm, _ := json.Marshal(map[string]any{"all": children})
	return string(raw), string(perm)
}

func (g *rgen) doc() []byte {
	d := map[string]any{}
	set := func(f string, v any) {
		if g.r.IntN(5) > 0 {
			d[f] = v
		}
	}
	set("brand", []any{g.pick(rvVocab), g.num(), true, []any{"x"}, nil, "acme\x00", "a\x00b"}[g.r.IntN(7)])
	title := g.phrase() + " " + g.phrase()
	if g.r.IntN(8) == 0 {
		title += "\x00" + g.phrase()
	}
	set("title", []any{title, "", g.num(), map[string]any{"k": 1}}[min(g.r.IntN(6), 3)])
	set("tags", []any{[]any{g.pick(rvVocab), g.pick(rvVocab)}, []any{}, g.pick(rvVocab) + ", " + g.pick(rvVocab), []any{g.num(), true}}[g.r.IntN(4)])
	set("price", []any{g.num(), g.num(), "10", nil}[g.r.IntN(4)])
	set("stock", []any{true, false, "true", 1}[g.r.IntN(4)])
	set("seen", []any{g.num(), "2025-10-02", "2025-10-01T23:59:59.999Z", "1970-01-01", "bad"}[g.r.IntN(5)])
	set("extra", []any{g.pick(rvVocab), g.num(), []any{"x"}}[g.r.IntN(3)])
	raw, _ := json.Marshal(d)
	return raw
}

// Root conjunctions over boundary numbers, Unicode-folded and NUL / U+FFFD texts,
// missing and wrong-type fields, not and ne inside any, and permuted twins (class
// members with different posting filters), in three segments with deletes, against
// brute force through Percolator.one.
func TestRootConjunctionsCrossCheck(t *testing.T) {
	m := testMapping()
	for seed := range uint64(6) {
		g := &rgen{r: rand.New(rand.NewPCG(seed, 99))}
		var qs []shard.StoredQuery
		for len(qs) < 6000 {
			a, b := g.query()
			for _, raw := range []string{a, b} {
				if raw == "" {
					continue
				}
				n, problems := query.Parse([]byte(raw))
				if len(problems) > 0 {
					continue
				}
				qs = append(qs, shard.StoredQuery{ID: fmt.Sprintf("q%06d", len(qs)), Seq: int64(len(qs) + 1), Query: n})
			}
		}
		g.r.Shuffle(len(qs), func(i, j int) { qs[i].ID, qs[j].ID = qs[j].ID, qs[i].ID })
		compiled := map[string]*query.Compiled{}
		for i := range qs {
			compiled[qs[i].ID] = query.Compile(qs[i].Query)
		}
		var views []view
		deleted := map[string]bool{}
		filters, empty := 0, 0
		for part := range 3 {
			lo, hi := part*len(qs)/3, (part+1)*len(qs)/3
			data, err := encodeSegment(context.Background(), qs[lo:hi], []shard.TermStats{nil, randStats{seed: seed, docs: 50}, randStats{seed: seed}}[part])
			if err != nil {
				t.Fatal(err)
			}
			seg, err := openData(fmt.Sprintf("r%d", part), data)
			if err != nil {
				t.Fatal(err)
			}
			filters += len(seg.filtered) / filteredSize
			for r := range seg.n {
				if u32(seg.verifies, verifySize*int(r)+4) == 0 {
					empty++
				}
			}
			v := view{seg: seg, n: seg.NumQueries()}
			if part != 0 {
				v.deletes = deletesEvery(seg, uint32(5+part), deleted)
			}
			views = append(views, v)
		}
		p := New(Options{Threads: 1})
		sc := new(scratch)
		for _, v := range views {
			sc.fit(v.n, v.seg.NumEntries(), len(v.seg.fields))
		}
		matches := 0
		for i := range 1500 {
			d, _, err := schema.Analyze(m, []string{"acme", "x", fmt.Sprintf("d%d", i), "rtx 4090"}[g.r.IntN(4)], g.doc())
			if err != nil {
				continue
			}
			ids, _, err := p.one(context.Background(), nil, views, &d, sc)
			if err != nil {
				t.Fatal(err)
			}
			got := strs(t, ids)
			var want []string
			for i := range qs {
				if !deleted[qs[i].ID] && compiled[qs[i].ID].Match(&d) {
					want = append(want, qs[i].ID)
				}
			}
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("seed %d doc %s: missing %v extra %v", seed, d.Body, minus(want, got), minus(got, want))
			}
			matches += len(want)
		}
		t.Logf("seed %d: %d queries, %d filtered postings, %d empty programs, %d matches", seed, len(qs), filters, empty, matches)
	}
}

// Concurrent percolations on acquired generations while queries are upserted, deleted,
// refreshed and merged: every answer equals brute force over its own generation.
func TestConcurrentQueryChurn(t *testing.T) {
	m := newShardModel(t)
	g := &rgen{r: rand.New(rand.NewPCG(5, 5))}
	var docs []schema.Doc
	for i := range 60 {
		d, _, err := schema.Analyze(testMapping(), fmt.Sprintf("d%d", i), g.doc())
		if err == nil {
			docs = append(docs, d)
		}
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	var checked atomic.Int64
	errs := make(chan error, 8)
	for w := range 4 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(w), 1))
			for !stop.Load() {
				gen := m.s.Acquire()
				batch := docs[r.IntN(len(docs)/2):][:1+r.IntN(len(docs)/2)]
				got, err := m.p.Percolate(context.Background(), gen, batch)
				if err == nil {
					err = bruteCheck(gen, batch, got)
				}
				gen.Release()
				if err != nil {
					errs <- err
					return
				}
				checked.Add(1)
			}
		})
	}
	r := rand.New(rand.NewPCG(9, 9))
	for step := range 120 {
		for range 1 + r.IntN(30) {
			id := fmt.Sprintf("q%d", r.IntN(200))
			if r.IntN(4) == 0 {
				if _, ok := m.queries[id]; ok {
					m.delQuery(id)
				}
				continue
			}
			raw, _ := g.query()
			if _, problems := query.Parse([]byte(raw)); len(problems) > 0 {
				continue
			}
			m.putQuery(id, raw)
		}
		m.refresh()
		if step%7 == 0 {
			m.merge()
		}
	}
	stop.Store(true)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("%d concurrent batches checked", checked.Load())
}

// bruteCheck compares got, docs percolated against g, with brute force over g.
func bruteCheck(g *shard.Generation, docs []schema.Doc, got []IDs) error {
	for i := range docs {
		var want []string
		for _, qs := range g.QuerySegments {
			for ord := range qs.NumQueries {
				if qs.Deletes.Contains(ord) {
					continue
				}
				q, err := qs.Segment.Query(ord)
				if err != nil {
					return err
				}
				if query.Match(q.Query, &docs[i]) {
					want = append(want, q.ID)
				}
			}
		}
		slices.Sort(want)
		have, err := got[i].Strings()
		if err != nil {
			return err
		}
		if !slices.Equal(have, want) {
			return fmt.Errorf("gen %d doc %s: missing %v extra %v", g.Gen(), docs[i].ID, minus(want, have), minus(have, want))
		}
	}
	return nil
}
