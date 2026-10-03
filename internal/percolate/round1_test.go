package percolate

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// pairHeavySegment builds n queries all[words_all w<i>, eq brand B]: each anchors on the
// pair (word w<i>, value B), so B is half of n pairs and each word of one.
func pairHeavySegment(t testing.TB, n int) *Segment {
	t.Helper()
	qs := make([]shard.StoredQuery, n)
	for i := range qs {
		raw := fmt.Sprintf(`{"all":[{"field":"title","op":"words_all","value":"w%d"},{"field":"brand","op":"eq","value":"B"}]}`, i)
		qs[i] = shard.StoredQuery{ID: fmt.Sprintf("q%06d", i), Query: parseQuery(t, raw)}
	}
	data, err := encodeSegment(context.Background(), qs, nil)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := openData("pairs", data)
	if err != nil {
		t.Fatal(err)
	}
	return seg
}

func wordsDoc(t testing.TB, words int) schema.Doc {
	t.Helper()
	var title strings.Builder
	for i := range words {
		fmt.Fprintf(&title, "w%d ", i)
	}
	body, _ := json.Marshal(map[string]any{"title": title.String(), "brand": "B"})
	d, _, err := schema.Analyze(testMapping(), "d", body)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// Pair probing is linear in the members a document holds, never quadratic: a document
// holding m member words and the shared value B walks about m partner records (each
// pair is owned by its half with fewer pairs, the word), where probing every pair of
// members would take m²/2 lookups.
func TestPairProbingIsLinear(t *testing.T) {
	seg := pairHeavySegment(t, 20000)
	sc := new(scratch)
	sc.fit(seg.NumQueries(), seg.NumEntries())
	for _, m := range []int{100, 1000, 3000} {
		d := wordsDoc(t, m)
		sc.pairOps = 0
		seg.collect(&d, sc)
		if sc.pairOps > m+1 {
			t.Fatalf("%d member words: %d pair operations, want at most %d", m, sc.pairOps, m+1)
		}
		if len(sc.cands) != m {
			t.Fatalf("%d member words: %d candidates, want %d", m, len(sc.cands), m)
		}
		sc.reset()
	}
}

// With the walk never allowed, pair probing falls back to the held members' postings,
// which is sound with no threshold: the completeness property still holds.
func TestPairFallbackIsSound(t *testing.T) {
	budget, weight := pairWalkBudget, candidateWeight
	pairWalkBudget, candidateWeight = -1, 0
	defer func() { pairWalkBudget, candidateWeight = budget, weight }()
	for _, mode := range []string{"positive", "dense"} {
		g := newGen(5)
		g.positive, g.dense = true, mode == "dense"
		checkCompleteness(t, g, 5, 400, 250, nil)
	}
	// And it is still linear: the pair-heavy document adds the members' postings.
	seg := pairHeavySegment(t, 2000)
	sc := new(scratch)
	sc.fit(seg.NumQueries(), seg.NumEntries())
	d := wordsDoc(t, 1000)
	seg.collect(&d, sc)
	if want := 2000 + 1000; sc.pairOps > want {
		t.Fatalf("fallback: %d operations, want at most %d", sc.pairOps, want)
	}
}
