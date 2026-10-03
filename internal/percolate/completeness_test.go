package percolate

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// The completeness property (spec section 7): for random queries and documents across
// every op and type, nested all/any/not, truncated grams, missing and wrong-type
// fields, the candidate set always contains every true match, and the final answer
// equals brute force with query.Match.
//
// The default run checks 100k query/document pairs per seed over three seeds and three
// statistics regimes. Reproduce a failure with -percolate.seed=N; run the long mode
// with -percolate.long (or SEARCHLIGHT_LONG=1), which checks millions of pairs.

var (
	flagSeed = flag.Uint64("percolate.seed", 0, "run the completeness property with this seed only")
	flagLong = flag.Bool("percolate.long", false, "run the completeness property in its long mode")
)

func longMode() bool { return *flagLong || os.Getenv("SEARCHLIGHT_LONG") != "" }

func TestCompletenessProperty(t *testing.T) {
	seeds := []uint64{1, 2, 3}
	queries, docs := 400, 250 // 100k pairs per seed and regime
	if longMode() {
		seeds = []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		queries, docs = 2000, 1000
	}
	if *flagSeed != 0 {
		seeds = []uint64{*flagSeed}
	}
	for _, seed := range seeds {
		for _, regime := range []string{"no-stats", "random-stats", "empty-stats"} {
			// mixed draws every op evenly (many queries end up Always); positive
			// rarely negates, so most queries are anchored and most matches come
			// through anchors; dense is positive over a tiny vocabulary, so that
			// conjunctions (pair anchors) match often.
			for _, mode := range []string{"mixed", "positive", "dense"} {
				t.Run(fmt.Sprintf("seed=%d/%s/%s", seed, regime, mode), func(t *testing.T) {
					var stats shard.TermStats
					switch regime {
					case "random-stats":
						stats = randStats{seed: seed, docs: 1000}
					case "empty-stats":
						stats = randStats{seed: seed}
					}
					g := newGen(seed)
					g.positive = mode != "mixed"
					g.dense = mode == "dense"
					checkCompleteness(t, g, seed, queries, docs, stats)
				})
			}
		}
	}
}

func checkCompleteness(t *testing.T, g *gen, seed uint64, numQueries, numDocs int, stats shard.TermStats) {
	t.Helper()
	qs := make([]shard.StoredQuery, 0, numQueries)
	for len(qs) < numQueries {
		raw := g.queryJSON()
		n, problems := query.Parse(raw)
		if len(problems) > 0 {
			continue // a shape Parse refuses: not a storable query
		}
		qs = append(qs, shard.StoredQuery{ID: fmt.Sprintf("q%05d", len(qs)), Seq: int64(len(qs) + 1), Query: n})
	}
	data, err := encodeSegment(context.Background(), qs, stats)
	if err != nil {
		t.Fatal(err)
	}
	seg, err := openData("prop", data)
	if err != nil {
		t.Fatal(err)
	}
	compiled := make([]*query.Compiled, len(qs))
	for i := range qs {
		compiled[i] = query.Compile(qs[i].Query)
	}
	// Some queries deleted: the answer must leave them out.
	deletes := roaring.New()
	for i := range qs {
		if g.r.IntN(10) == 0 {
			deletes.Add(uint32(i))
		}
	}
	v := &view{seg: seg, n: seg.NumQueries(), deletes: deletes}
	sc := new(scratch)
	sc.fit(seg.NumQueries(), seg.NumEntries())
	m := testMapping()
	isAlways := make(map[uint32]bool)
	for i := 0; i < len(seg.always); i += 4 {
		isAlways[u32(seg.always, i)] = true
	}
	pairs, matches, anchored, candidates := 0, 0, 0, 0
	for di := range numDocs {
		id, body := g.docJSON(di)
		d, _, err := schema.Analyze(m, id, body)
		if err != nil {
			t.Fatalf("analyze %s: %v", body, err)
		}
		seg.collect(&d, sc)
		cand := make(map[uint32]bool, len(sc.cands))
		for _, ord := range sc.cands {
			cand[ord] = true
		}
		candidates += len(cand)
		var want []string
		for i := range qs {
			pairs++
			if !compiled[i].Match(&d) {
				continue
			}
			matches++
			if !isAlways[uint32(i)] {
				anchored++
			}
			if !cand[uint32(i)] {
				raw, _ := encodeQuery(qs[i].Query)
				a := Extract(qs[i].Query, stats)
				t.Fatalf("seed %d: query %s matches document %s but is no candidate\nquery: %s\nanchors: %+v",
					seed, qs[i].ID, body, raw, a)
			}
			if !deletes.Contains(uint32(i)) {
				want = append(want, qs[i].ID)
			}
		}
		var st docStats
		err = verify(v, &d, sc, &st)
		got := sc.results(true)
		if err != nil {
			t.Fatal(err)
		}
		sc.reset()
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("seed %d: document %s: percolate %v, brute force %v", seed, body, got, want)
		}
	}
	if anchored == 0 || anchored*200 < pairs {
		t.Fatalf("only %d of %d pairs match through anchors: the generator is too sparse to test anything", anchored, pairs)
	}
	t.Logf("%d pairs, %d matches (%d through anchors), %.1f candidates per document of %d queries (%d always-check)",
		pairs, matches, anchored, float64(candidates)/float64(numDocs), len(qs), seg.NumAlways())
}

// randStats are arbitrary, deterministic term statistics: anchors chosen under them
// differ from the priors', and every choice must be complete.
type randStats struct {
	seed uint64
	docs uint64
}

func (r randStats) NumDocs() uint64 { return r.docs }

func (r randStats) DocFreq(field string, kind segment.TermKind, term string) uint64 {
	if r.docs == 0 {
		return 0
	}
	h := fnv.New64a()
	fmt.Fprintf(h, "%d\x00%s\x00%d\x00%s", r.seed, field, kind, term)
	return h.Sum64() % (r.docs + 1)
}

// gen generates random queries and documents over one shared vocabulary, so that
// conditions hit documents often.
type gen struct {
	r        *rand.Rand
	positive bool // rarely negate: no ne, empty or exists:false, few nots
	dense    bool // draw words from a tiny vocabulary
}

// positiveOps are the ops a positive generator draws: the anchorable ones.
var positiveOps = []string{
	query.OpContains, query.OpContainsAny, query.OpContainsAll, query.OpEq, query.OpIn,
	query.OpLt, query.OpLte, query.OpGt, query.OpGte, query.OpBetween,
	query.OpHas, query.OpHasAny, query.OpHasAll, query.OpNonempty, query.OpExists,
	query.OpStartsWith, query.OpWordsAll, query.OpWordsAny, query.OpSimilar,
}

func newGen(seed uint64) *gen { return &gen{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))} }

var vocab = []string{
	"rtx", "4090", "RTX", "Ti", "café", "Cafe", "straße", "STRASSE", "İstanbul", "istanbul",
	"ﬁne", "fine", "a", "ab", "abc", "x y", "New  York", " new york ", "é", "é", "😀",
	"日本語", "foo_bar", "foo-bar", "ǅ", "Σίσυφος", "OLED", "oled", "4k", "4K", "Ultra",
	"refurbished", "open box", "", " ",
}

var fields = []string{"brand", "title", "tags", "price", "stock", "seen", "extra", "_id", "missing"}

var numbers = []float64{-1, 0, 0.5, 1, 2, 10, 99.99, 100, 250, 1e6, -100}

func (g *gen) pick(list []string) string { return list[g.r.IntN(len(list))] }

func (g *gen) word() string {
	if g.dense {
		return g.pick(vocab[:5])
	}
	return g.pick(vocab)
}

func (g *gen) phrase(maxWords int) string {
	n := 1 + g.r.IntN(maxWords)
	parts := make([]string, n)
	for i := range parts {
		parts[i] = g.word()
	}
	return strings.Join(parts, " ")
}

func (g *gen) number() float64 {
	if g.r.IntN(3) == 0 {
		return float64(g.r.IntN(300))
	}
	return numbers[g.r.IntN(len(numbers))]
}

// needle is a piece of a phrase: whole, a word, or a run of runes of one.
func (g *gen) needle() string {
	switch g.r.IntN(4) {
	case 0:
		return g.phrase(2)
	case 1:
		return g.word()
	default:
		runes := []rune(g.phrase(2))
		if len(runes) == 0 {
			return ""
		}
		i := g.r.IntN(len(runes))
		j := i + g.r.IntN(len(runes)-i+1)
		return string(runes[i:j])
	}
}

func (g *gen) scalar() any {
	switch g.r.IntN(5) {
	case 0:
		return g.number()
	case 1:
		return g.r.IntN(2) == 0
	default:
		return g.word()
	}
}

func (g *gen) strings(n int, one func() string) any {
	if g.r.IntN(3) == 0 {
		return one()
	}
	list := make([]any, 1+g.r.IntN(n))
	for i := range list {
		if g.r.IntN(12) == 0 {
			list[i] = g.number() // a non-string entry, which the text ops skip
		} else {
			list[i] = one()
		}
	}
	return list
}

func (g *gen) leaf() map[string]any {
	op := query.Ops[g.r.IntN(len(query.Ops))]
	if g.positive {
		op = positiveOps[g.r.IntN(len(positiveOps))]
	}
	l := map[string]any{"field": g.pick(fields), "op": op}
	var v any
	switch op {
	case query.OpEq, query.OpNe:
		v = g.scalar()
	case query.OpIn:
		list := make([]any, 1+g.r.IntN(4))
		for i := range list {
			list[i] = g.scalar()
		}
		v = list
	case query.OpLt, query.OpLte, query.OpGt, query.OpGte:
		if g.r.IntN(10) == 0 {
			v = g.word()
		} else {
			v = g.number()
		}
	case query.OpBetween:
		v = []any{g.number(), g.number()}
	case query.OpExists:
		switch g.r.IntN(4) {
		case 0:
			v = true
		case 1:
			v = false
		case 2:
			v = "false"
		}
	case query.OpContains, query.OpContainsAny, query.OpContainsAll, query.OpHas, query.OpHasAny, query.OpHasAll:
		v = g.strings(3, g.needle)
	case query.OpStartsWith:
		v = g.needle()
	case query.OpWordsAll, query.OpWordsAny:
		v = g.strings(3, func() string { return g.phrase(2) })
	case query.OpSimilar:
		mins := []any{0.1, 0.3, 0.5, 1, 0, -1, 2, "x"}
		v = map[string]any{"text": g.phrase(3), "min": mins[g.r.IntN(len(mins))]}
	}
	if v != nil {
		l["value"] = v
	}
	return l
}

func (g *gen) node(depth int) any {
	switch r := g.r.IntN(10); {
	case r < 2 && depth > 0 && (!g.positive || g.r.IntN(5) == 0):
		inner := g.node(depth)
		if g.r.IntN(4) == 0 {
			inner = map[string]any{"not": inner} // a double not
		}
		return map[string]any{"not": inner}
	case r < 5 && depth > 0:
		children := make([]any, 1+g.r.IntN(4))
		for i := range children {
			children[i] = g.node(depth - 1)
		}
		key := "all"
		if g.r.IntN(2) == 0 {
			key = "any"
		}
		return map[string]any{key: children}
	default:
		return g.leaf()
	}
}

func (g *gen) queryJSON() []byte {
	if g.r.IntN(100) == 0 {
		return []byte(`{"all":[]}`)
	}
	raw, err := json.Marshal(g.node(3))
	if err != nil {
		panic(err)
	}
	return raw
}

// docJSON returns a random document: each field present or not, of its type or of
// another (a wrong-type value), a title sometimes past MaxGramChars, and dynamic
// fields typed by their own value.
func (g *gen) docJSON(i int) (string, []byte) {
	ids := []string{"rtx", "RTX 4090", "café", "abc", "x"}
	id := fmt.Sprintf("d%d", i)
	if g.r.IntN(4) == 0 {
		id = ids[g.r.IntN(len(ids))]
	}
	doc := map[string]any{}
	for _, f := range []string{"brand", "title", "tags", "price", "stock", "seen", "extra"} {
		if g.r.IntN(10) < 3 {
			continue
		}
		var v any
		switch {
		case g.r.IntN(6) == 0:
			v = g.wrongValue()
		case f == "brand":
			v = g.word()
		case f == "title":
			v = g.title()
		case f == "tags":
			if g.r.IntN(3) == 0 {
				v = g.phrase(3) + ", " + g.phrase(2) + ", "
			} else {
				list := make([]any, g.r.IntN(4))
				for k := range list {
					list[k] = g.word()
				}
				v = list
			}
		case f == "price":
			v = g.number()
		case f == "stock":
			v = g.r.IntN(2) == 0
		case f == "seen":
			if g.r.IntN(2) == 0 {
				v = "2026-10-02"
			} else {
				v = g.number()
			}
		default:
			v = g.scalar()
		}
		doc[f] = v
	}
	body, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return id, body
}

func (g *gen) wrongValue() any {
	switch g.r.IntN(6) {
	case 0:
		return g.word()
	case 1:
		return g.number()
	case 2:
		return true
	case 3:
		return []any{g.word(), 1}
	case 4:
		return map[string]any{"k": g.word()}
	default:
		return []any{}
	}
}

func (g *gen) title() string {
	t := g.phrase(6)
	if g.r.IntN(15) == 0 {
		// Past MaxGramChars: no grams in the segment, and the percolator must still
		// find a needle at the very end.
		t = strings.Repeat("filler words ", schema.MaxGramChars/12+1) + t
	}
	if g.r.IntN(30) == 0 {
		t += "\x00nul"
	}
	return t
}
