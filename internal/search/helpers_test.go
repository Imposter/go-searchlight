package search

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

var (
	seedFlag = flag.Uint64("search.seed", 0, "seed for the search property tests (0: from the clock, or SEARCHLIGHT_SEED)")
	longFlag = flag.Bool("search.long", false, "run the search property tests long (also SEARCHLIGHT_LONG=1)")
)

// testSeed returns the property tests' seed, logged so a failure can be replayed with
// -search.seed=N (or SEARCHLIGHT_SEED=N).
func testSeed(t *testing.T) uint64 {
	t.Helper()
	seed := *seedFlag
	if s := os.Getenv("SEARCHLIGHT_SEED"); seed == 0 && s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("SEARCHLIGHT_SEED=%q: %v", s, err)
		}
		seed = n
	}
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("seed %d (replay with -search.seed=%d)", seed, seed)
	return seed
}

func longMode() bool {
	return *longFlag || os.Getenv("SEARCHLIGHT_LONG") == "1"
}

var testMapping = &schema.Mapping{Fields: map[string]schema.FieldType{
	"title":   schema.Text,
	"brand":   schema.Keyword,
	"tags":    schema.KeywordList,
	"price":   schema.Number,
	"created": schema.Date,
	"active":  schema.Bool,
	"desc":    schema.Text,
}}

var quiet = slog.New(slog.DiscardHandler)

// cluster is a few shards over one index, with a model of what each shows.
type cluster struct {
	t       testing.TB
	shards  []*shard.Shard
	seq     int64
	visible []map[string]*schema.Doc // what each shard's current generation holds
	pending [][]shard.Change         // applied but not yet refreshed, per shard
	cache   *shard.FilterCache
}

func newCluster(t testing.TB, n int) *cluster {
	t.Helper()
	c := &cluster{t: t, cache: shard.NewFilterCache(8<<20, nil)}
	for range n {
		s, err := shard.Open(context.Background(), t.TempDir(), testMapping, shard.Options{
			RefreshInterval: -1, DisableMerges: true, Logger: quiet, FilterCache: c.cache,
		})
		if err != nil {
			t.Fatalf("shard.Open: %v", err)
		}
		c.shards = append(c.shards, s)
		c.visible = append(c.visible, map[string]*schema.Doc{})
		c.pending = append(c.pending, nil)
	}
	t.Cleanup(func() {
		for _, s := range c.shards {
			_ = s.Close(context.Background())
		}
	})
	return c
}

func (c *cluster) route(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(len(c.shards)))
}

func (c *cluster) upsert(id, body string) {
	c.t.Helper()
	d, _, err := schema.Analyze(testMapping, id, []byte(body))
	if err != nil {
		c.t.Fatalf("Analyze(%q, %s): %v", id, body, err)
	}
	c.seq++
	c.apply(c.route(id), shard.Change{Seq: c.seq, Kind: shard.Upsert, Doc: &d})
}

func (c *cluster) remove(id string) {
	c.t.Helper()
	c.seq++
	c.apply(c.route(id), shard.Change{Seq: c.seq, Kind: shard.Delete, DocID: id})
}

func (c *cluster) apply(i int, ch shard.Change) {
	c.t.Helper()
	if err := c.shards[i].Apply(context.Background(), []shard.Change{ch}); err != nil {
		c.t.Fatalf("Apply: %v", err)
	}
	c.pending[i] = append(c.pending[i], ch)
}

func (c *cluster) refresh(i int) {
	c.t.Helper()
	if err := c.shards[i].Refresh(context.Background()); err != nil {
		c.t.Fatalf("Refresh: %v", err)
	}
	for _, ch := range c.pending[i] {
		if ch.Kind == shard.Upsert {
			c.visible[i][ch.Doc.ID] = ch.Doc
		} else {
			delete(c.visible[i], ch.DocID)
		}
	}
	c.pending[i] = nil
}

func (c *cluster) refreshAll() {
	for i := range c.shards {
		c.refresh(i)
	}
}

func (c *cluster) merge(i, maxSegments int) {
	c.t.Helper()
	if err := c.shards[i].ForceMerge(context.Background(), maxSegments); err != nil {
		c.t.Fatalf("ForceMerge: %v", err)
	}
}

// docs returns every visible document.
func (c *cluster) docs() []*schema.Doc {
	var out []*schema.Doc
	for _, v := range c.visible {
		for _, d := range v {
			out = append(out, d)
		}
	}
	return out
}

// search runs r on every shard and reduces.
func (c *cluster) search(r *Request) (*Response, error) {
	var parts []*ShardResult
	for _, s := range c.shards {
		g := s.Acquire()
		res, err := ExecuteShard(context.Background(), g, r)
		g.Release()
		if err != nil {
			return nil, err
		}
		parts = append(parts, res)
	}
	return Reduce(parts, r), nil
}

// brute returns the visible documents q matches, in r's order.
func brute(docs []*schema.Doc, q query.Node, sorts []SortField) []*schema.Doc {
	m := query.Compile(q)
	var out []*schema.Doc
	for _, d := range docs {
		if m.Match(d) {
			out = append(out, d)
		}
	}
	sortDocs(out, sorts)
	return out
}

// sortValue is a document's value for sort key f, as the engine returns it.
func sortValue(d *schema.Doc, f string) any {
	if f == IDField {
		return d.ID
	}
	v, ok := d.Fields[f]
	if !ok || !v.Present {
		return nil
	}
	switch testMapping.Fields[f] {
	case schema.Number, schema.Date:
		if v.Number != nil {
			return *v.Number
		}
	case schema.Keyword, schema.Text:
		if v.Text != nil {
			return *v.Text
		}
	case schema.Bool:
		if v.Bool != nil {
			return *v.Bool
		}
	}
	return nil
}

func sortDocs(docs []*schema.Doc, sorts []SortField) {
	slices.SortStableFunc(docs, func(a, b *schema.Doc) int {
		for _, s := range sorts {
			if c := cmpValue(sortValue(a, s.Field), sortValue(b, s.Field), s.Desc); c != 0 {
				return c
			}
			if s.Field == IDField {
				return 0
			}
		}
		return strings.Compare(a.ID, b.ID)
	})
}

// expectedSort is a document's Hit.Sort for sorts.
func expectedSort(d *schema.Doc, sorts []SortField) []any {
	var out []any
	for _, s := range sorts {
		out = append(out, sortValue(d, s.Field))
		if s.Field == IDField {
			return out
		}
	}
	return append(out, d.ID)
}

// Random documents and queries.

var vocab = []string{
	"red", "Red", "RED", "blue", "acme", "ACME", "Acme Corp", "straße", "STRASSE", "strasse",
	"İstanbul", "istanbul", "ﬁsh", "fish", "ΣΊΣΥΦΟΣ", "σίσυφος", "naïve", "café", "x", "ab",
	"abc", "abcd", "tea cup", "teacup", "pro max", "ultra", "", "  spaced   out ", "a\x00b",
	"ꭰꭱ", "ᏣᎳᎩ", "wireless", "headphones", "über", "10", "12.5", "true",
	"aba", "bab", "abab", "babab", "aba bab",
}

var ids = func() []string {
	out := []string{"A", "a", "İd", "id", "ID", "a b", "ß", "ss", "x\u0301", strings.Repeat("long", 60)}
	for i := range 50 {
		out = append(out, fmt.Sprintf("doc-%02d", i))
	}
	return out
}()

func pick[T any](rng *rand.Rand, xs []T) T { return xs[rng.IntN(len(xs))] }

func phrase(rng *rand.Rand, words int) string {
	seps := []string{" ", " ", ", ", "-", "! ", "  "}
	var b strings.Builder
	for i := range words {
		if i > 0 {
			b.WriteString(pick(rng, seps))
		}
		b.WriteString(pick(rng, vocab))
	}
	return b.String()
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func randomNumber(rng *rand.Rand) string {
	switch rng.IntN(6) {
	case 0:
		return strconv.Itoa(rng.IntN(20))
	case 1:
		return strconv.FormatFloat(float64(rng.IntN(400))/4, 'f', -1, 64)
	case 2:
		return strconv.Itoa(-rng.IntN(5))
	case 3:
		return "-0"
	case 4:
		return "1e3"
	default:
		return strconv.FormatFloat(rng.Float64()*100, 'g', -1, 64)
	}
}

// randomDoc returns a document body with every field type, often missing or mistyped.
func randomDoc(rng *rand.Rand) string {
	var members []string
	add := func(name, value string) { members = append(members, jsonString(name)+":"+value) }
	if rng.IntN(8) > 0 {
		switch rng.IntN(10) {
		case 0:
			add("title", randomNumber(rng))
		case 1:
			add("title", `["a","b"]`)
		default:
			add("title", jsonString(phrase(rng, 1+rng.IntN(4))))
		}
	}
	if rng.IntN(6) > 0 {
		switch rng.IntN(10) {
		case 0:
			add("brand", "true")
		case 1:
			add("brand", `{"x":1}`)
		default:
			add("brand", jsonString(pick(rng, vocab)))
		}
	}
	if rng.IntN(5) > 0 {
		switch rng.IntN(6) {
		case 0:
			add("tags", jsonString(phrase(rng, 3)))
		case 1:
			add("tags", "[]")
		case 2:
			add("tags", "7")
		default:
			n := rng.IntN(4)
			var entries []string
			for range n {
				if rng.IntN(5) == 0 {
					entries = append(entries, randomNumber(rng))
				} else {
					entries = append(entries, jsonString(pick(rng, vocab)))
				}
			}
			add("tags", "["+strings.Join(entries, ",")+"]")
		}
	}
	if rng.IntN(6) > 0 {
		if rng.IntN(10) == 0 {
			add("price", `"cheap"`)
		} else {
			add("price", randomNumber(rng))
		}
	}
	if rng.IntN(5) > 0 {
		switch rng.IntN(5) {
		case 0:
			add("created", jsonString(time.Date(2026, time.Month(1+rng.IntN(12)), 1+rng.IntN(28), rng.IntN(24), 0, 0, 0, time.UTC).Format(time.RFC3339)))
		case 1:
			add("created", `"not a date"`)
		default:
			add("created", strconv.FormatInt(1_700_000_000_000+rng.Int64N(400)*86_400_000, 10))
		}
	}
	if rng.IntN(4) > 0 {
		switch rng.IntN(6) {
		case 0:
			add("active", `"yes"`)
		default:
			add("active", strconv.FormatBool(rng.IntN(2) == 0))
		}
	}
	if rng.IntN(3) == 0 {
		words := 3 + rng.IntN(8)
		if rng.IntN(4) == 0 {
			words = 260 // past schema.MaxGramChars: no grams, truncated
		}
		add("desc", jsonString(phrase(rng, words)))
	}
	if rng.IntN(10) == 0 {
		add("blob", pick(rng, []string{"{}", "[]", `{"a":[1]}`}))
	}
	rng.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })
	return "{" + strings.Join(members, ",") + "}"
}

var queryFields = []string{"title", "brand", "tags", "price", "created", "active", "desc", "_id", "blob", "nope"}

// randomValue returns a JSON value for op: mostly the shape it takes, sometimes not.
func randomValue(rng *rand.Rand, op string) string {
	text := func() string {
		s := pick(rng, vocab)
		if rng.IntN(3) == 0 {
			s = phrase(rng, 2) // a needle across a word boundary
		}
		if strings.ContainsRune(s, 0) {
			s = "ab"
		}
		if rng.IntN(2) == 0 && len([]rune(s)) > 2 {
			r := []rune(s)
			i := rng.IntN(len(r) - 1)
			s = string(r[i:min(len(r), i+2+rng.IntN(5))])
		}
		if strings.ContainsRune(s, 0) || len([]rune(s)) > 500 {
			s = "ab"
		}
		return s
	}
	list := func(gen func() string) string {
		n := 1 + rng.IntN(3)
		parts := make([]string, n)
		for i := range parts {
			parts[i] = gen()
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	scalar := func() string {
		switch rng.IntN(4) {
		case 0:
			return randomNumber(rng)
		case 1:
			return strconv.FormatBool(rng.IntN(2) == 0)
		default:
			return jsonString(text())
		}
	}
	if rng.IntN(12) == 0 { // the wrong shape now and then
		return pick(rng, []string{"null", "[]", `"x"`, "3", "true", `[1,"a",false]`})
	}
	switch op {
	case query.OpExists:
		return pick(rng, []string{"null", "true", "false"})
	case query.OpEmpty, query.OpNonempty:
		return "null"
	case query.OpEq, query.OpNe:
		return scalar()
	case query.OpIn:
		return list(scalar)
	case query.OpLt, query.OpLte, query.OpGt, query.OpGte:
		if rng.IntN(3) == 0 {
			return strconv.FormatInt(1_700_000_000_000+rng.Int64N(400)*86_400_000, 10)
		}
		return randomNumber(rng)
	case query.OpBetween:
		a, b := randomNumber(rng), randomNumber(rng)
		if rng.IntN(3) == 0 {
			lo := 1_700_000_000_000 + rng.Int64N(400)*86_400_000
			a, b = strconv.FormatInt(lo, 10), strconv.FormatInt(lo+rng.Int64N(100)*86_400_000, 10)
		}
		return "[" + a + "," + b + "]"
	case query.OpContains, query.OpStartsWith, query.OpWordsAll, query.OpWordsAny, query.OpHas:
		if rng.IntN(4) == 0 && op != query.OpStartsWith {
			return list(func() string { return jsonString(text()) })
		}
		if op == query.OpWordsAll || op == query.OpWordsAny {
			return jsonString(phrase(rng, 1+rng.IntN(2)))
		}
		return jsonString(text())
	case query.OpContainsAny, query.OpContainsAll, query.OpHasAny, query.OpHasAll:
		return list(func() string { return jsonString(text()) })
	case query.OpSimilar:
		return fmt.Sprintf(`{"text":%s,"min":%s}`, jsonString(phrase(rng, 1+rng.IntN(2))),
			pick(rng, []string{"0.1", "0.3", "0.5", "0.8", "1"}))
	}
	return "null"
}

func randomLeaf(rng *rand.Rand) string {
	f := pick(rng, queryFields)
	op := pick(rng, query.Ops)
	if ops := query.OpsFor(fieldType(testMapping, f)); len(ops) > 0 && rng.IntN(5) > 0 {
		op = pick(rng, ops) // mostly an op the field takes
	}
	return fmt.Sprintf(`{"field":%s,"op":%q,"value":%s}`, jsonString(f), op, randomValue(rng, op))
}

func randomNodeJSON(rng *rand.Rand, depth int) string {
	if depth >= 3 || rng.IntN(3) == 0 {
		return randomLeaf(rng)
	}
	switch rng.IntN(5) {
	case 0:
		return `{"not":` + randomNodeJSON(rng, depth) + `}`
	case 1, 2:
		n := 1 + rng.IntN(3)
		parts := make([]string, n)
		for i := range parts {
			parts[i] = randomNodeJSON(rng, depth+1)
		}
		return `{"all":[` + strings.Join(parts, ",") + `]}`
	default:
		n := 1 + rng.IntN(3)
		parts := make([]string, n)
		for i := range parts {
			parts[i] = randomNodeJSON(rng, depth+1)
		}
		return `{"any":[` + strings.Join(parts, ",") + `]}`
	}
}

// randomQuery returns a parsed random query and its JSON. Now and then its leaves are
// stripped to Value alone, as a tree built by hand.
func randomQuery(_ testing.TB, rng *rand.Rand) (query.Node, string) {
	for {
		raw := `{"all":[]}`
		if rng.IntN(15) > 0 {
			raw = randomNodeJSON(rng, 0)
		}
		n, ps := query.Parse([]byte(raw))
		if len(ps) > 0 {
			continue
		}
		if rng.IntN(5) == 0 {
			query.Walk(n, func(_ string, x query.Node) bool {
				if l, ok := x.(*query.Leaf); ok {
					l.Arg = query.Arg{}
				}
				return true
			})
		}
		return n, raw
	}
}

var sortChoices = [][]SortField{
	nil,
	{{Field: "price"}},
	{{Field: "price", Desc: true}},
	{{Field: "brand"}},
	{{Field: "title", Desc: true}},
	{{Field: "active"}},
	{{Field: "_id", Desc: true}},
	{{Field: "created"}, {Field: "price", Desc: true}},
	{{Field: "nope"}},
	{{Field: "active", Desc: true}, {Field: "brand"}},
}

func floatEq(a, b float64) bool {
	return a == b || math.Abs(a-b) <= 1e-9*math.Max(math.Abs(a), math.Abs(b))
}
