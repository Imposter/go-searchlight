package percolate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// percolateEveryWay percolates d through views whole and split into 2, 3 and 5
// windows, fails unless every way gives the same answer, and returns it.
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
	for _, k := range []int{2, 3, 5} {
		got, gst, err := p.split(context.Background(), nil, views, d, newSplitPlan(views, k), sc, size)
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
