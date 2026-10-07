package percolate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// percolateEveryWay percolates d through views whole and split into windows (2 to 7,
// on 1 to 3 workers), fails unless every way gives the same answer, and returns it.
func percolateEveryWay(tb testing.TB, p *Percolator, views []view, d *schema.Doc, sc *scratch) (IDs, docStats) {
	tb.Helper()
	ids, st, err := p.one(context.Background(), nil, views, d, sc)
	if err != nil {
		tb.Fatal(err)
	}
	var size scratchSize
	for _, v := range views {
		size = scratchSize{n: max(size.n, v.n), entries: max(size.entries, v.seg.NumEntries()), fields: max(size.fields, len(v.seg.fields))}
	}
	for _, w := range [][2]int{{1, 2}, {2, 3}, {2, 4}, {3, 7}} {
		k := w[1]
		got, gst, err := p.split(context.Background(), nil, views, d, newSplitPlan(views, w[0], k), sc, size)
		if err != nil {
			tb.Fatal(err)
		}
		if !bytes.Equal(got, ids) {
			tb.Fatalf("document %s in %d windows: %s, whole: %s", d.ID, k, got, ids)
		}
		if gst.matched != st.matched {
			tb.Fatalf("document %s in %d windows: %d matched, whole %d", d.ID, k, gst.matched, st.matched)
		}
	}
	return ids, st
}

// segmentOf builds and opens a segment of queries with ids, each matching a document
// whose brand is its brand.
func segmentOf(tb testing.TB, ids []string, brand func(string) string) *Segment {
	tb.Helper()
	qs := make([]shard.StoredQuery, len(ids))
	for i, id := range ids {
		raw, _ := json.Marshal(map[string]any{"field": "brand", "op": "eq", "value": brand(id)})
		qs[i] = shard.StoredQuery{ID: id, Seq: int64(i + 1), Query: parseQuery(tb, string(raw))}
	}
	data, err := encodeSegment(context.Background(), qs, nil)
	if err != nil {
		tb.Fatal(err)
	}
	seg, err := openData("ids", data)
	if err != nil {
		tb.Fatal(err)
	}
	return seg
}

// Matches spread over segments whose ids interleave, nest or lie apart, ids escaped or
// sharing long prefixes, come out as the sorted union, whole or split into windows.
func TestSegmentRunsMergeAsSortedUnion(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	m := testMapping()
	for round := range 300 {
		var all []string
		seen := map[string]bool{}
		for len(all) < 1+r.IntN(400) {
			var id string
			switch r.IntN(3) {
			case 0:
				id = randomID(r)
			case 1:
				id = fmt.Sprintf("q%09d", r.IntN(100_000))
			default:
				id = fmt.Sprintf("query-with-a-long-prefix-%d", r.IntN(1000))
			}
			if !seen[id] {
				seen[id] = true
				all = append(all, id)
			}
		}
		switch round % 3 {
		case 0:
			r.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
		default:
			slices.Sort(all)
		}
		brand := func(id string) string { return []string{"acme", "zeta"}[len(id)%2] }
		var views []view
		for rest := all; len(rest) > 0; {
			n := 1 + r.IntN(len(rest))
			seg := segmentOf(t, rest[:n], brand)
			views = append(views, view{seg: seg, n: seg.n})
			rest = rest[n:]
		}
		p := New(Options{Threads: 1})
		sc := new(scratch)
		for _, v := range views {
			sc.fit(v.n, v.seg.NumEntries(), len(v.seg.fields))
		}
		d, _, err := schema.Analyze(m, "d", []byte(`{"brand":"acme"}`))
		if err != nil {
			t.Fatal(err)
		}
		ids, _ := percolateEveryWay(t, p, views, &d, sc)
		var want []string
		for _, id := range all {
			if brand(id) == "acme" {
				want = append(want, id)
			}
		}
		slices.Sort(want)
		wantJSON, _ := json.Marshal(want)
		if len(want) == 0 {
			wantJSON = nil
		}
		if !bytes.Equal(ids, wantJSON) {
			t.Fatalf("round %d: %s, want %s", round, ids, wantJSON)
		}
	}
}

// A canceled split stops before its next window and reports the cancellation.
func TestSplitStopsWhenCanceled(t *testing.T) {
	ids := make([]string, 200)
	for i := range ids {
		ids[i] = fmt.Sprintf("q%03d", i)
	}
	seg := segmentOf(t, ids, func(string) string { return "acme" })
	views := []view{{seg: seg, n: seg.n}}
	d, _, err := schema.Analyze(testMapping(), "d", []byte(`{"brand":"acme"}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := New(Options{Threads: 2})
	sc := new(scratch)
	sc.fit(seg.n, seg.NumEntries(), len(seg.fields))
	size := scratchSize{n: seg.n, entries: seg.NumEntries(), fields: len(seg.fields)}
	if got, _, err := p.split(ctx, nil, views, &d, newSplitPlan(views, 2, 4), sc, size); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatalf("got %s, %v; want the cancellation", got, err)
	}
	if got, _, err := p.split(context.Background(), nil, views, &d, newSplitPlan(views, 2, 4), sc, size); err != nil || len(strs(t, got)) != len(ids) {
		t.Fatalf("after a canceled split: %d matches, %v", len(strs(t, got)), err)
	}
}
