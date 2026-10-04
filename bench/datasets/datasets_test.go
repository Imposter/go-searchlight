package datasets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

func mapping(t *testing.T) *schema.Mapping {
	t.Helper()
	raw, err := json.Marshal(SearchlightMapping(Products, 1, "")["mapping"])
	if err != nil {
		t.Fatal(err)
	}
	var m schema.Mapping
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("the generated mapping is not a Searchlight mapping: %v: %s", err, raw)
	}
	return &m
}

func digest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestProductsDeterministic(t *testing.T) {
	a := AppendProduct(nil, 42, 1234)
	b := AppendProduct(nil, 42, 1234)
	if !bytes.Equal(a, b) {
		t.Fatalf("document 1234 differs between two calls:\n%s\n%s", a, b)
	}
	if bytes.Equal(a, AppendProduct(nil, 43, 1234)) {
		t.Fatal("another seed gave the same document")
	}
	if bytes.Equal(a, AppendProduct(nil, 42, 1235)) {
		t.Fatal("another ordinal gave the same document")
	}
}

// TestWriteParallelMatchesSequential checks that parallel, chunked generation writes
// exactly the bytes a sequential loop does, whatever the worker count.
func TestWriteParallelMatchesSequential(t *testing.T) {
	const n = 3*chunk + 17
	var want []byte
	for i := range int64(n) {
		want = AppendProductLine(want, 7, i)
	}
	for _, workers := range []int{1, 3, 8} {
		var got bytes.Buffer
		if err := Write(context.Background(), &got, ProductLines(7), 0, n, workers); err != nil {
			t.Fatal(err)
		}
		if digest(got.Bytes()) != digest(want) {
			t.Fatalf("%d workers wrote different bytes than a sequential loop", workers)
		}
	}
	// An offset start renders the same lines as the tail of a full run.
	var tail bytes.Buffer
	if err := Write(context.Background(), &tail, ProductLines(7), 5, 10, 2); err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(want), "\n")
	if tail.String() != strings.Join(lines[5:15], "") {
		t.Fatal("a window [5, 15) differs from the full run's lines")
	}
}

// TestProductsGolden pins the generator's output: a change to it changes every
// published benchmark's data, so it must be deliberate (update the digest).
// goldenProducts is the SHA-256 of the first 1000 product lines under seed 1.
const goldenProducts = "6123e6fee6304e009c770d937f37d5eb0ed5488c8ae73e0d6851301b3630e0ad"

func TestProductsGolden(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(context.Background(), &buf, ProductLines(1), 0, 1000, 4); err != nil {
		t.Fatal(err)
	}
	if got := digest(buf.Bytes()); got != goldenProducts {
		t.Fatalf("the first 1000 products under seed 1 changed: digest %s, want %s", got, goldenProducts)
	}
}

func TestProductsAnalyzeUnderStrictMapping(t *testing.T) {
	m := mapping(t)
	fieldSeen := map[string]int{}
	for i := range int64(2000) {
		line := AppendProductLine(nil, 9, i)
		id, doc, err := SplitProductLine(bytes.TrimSpace(line))
		if err != nil {
			t.Fatal(err)
		}
		if id != ProductID(i) {
			t.Fatalf("line %d has id %q", i, id)
		}
		if _, _, err := schema.Analyze(m, id, doc); err != nil {
			t.Fatalf("document %s does not analyze under the strict mapping: %v\n%s", id, err, doc)
		}
		var body map[string]any
		if err := json.Unmarshal(doc, &body); err != nil {
			t.Fatal(err)
		}
		for k, v := range body {
			fieldSeen[k]++
			if list, ok := v.([]any); ok && len(list) == 0 {
				t.Fatalf("document %s has an empty list in %s (exists differs between the engines on one)", id, k)
			}
		}
		words := len(strings.Fields(body["description"].(string))) //nolint:forcetypeassert,errcheck // always a string
		if words < 45 || words > 75 {
			t.Fatalf("description of %d words", words)
		}
	}
	for _, f := range Products {
		if fieldSeen[f.Name] == 0 {
			t.Errorf("no document has %s", f.Name)
		}
	}
	// Optional fields are sometimes missing.
	for _, f := range []string{"tags", "list_price", "rating"} {
		if fieldSeen[f] == 2000 {
			t.Errorf("every document has %s; some should not", f)
		}
	}
}

func TestZipfIsSkewed(t *testing.T) {
	z := NewZipf(1000, 1.0)
	r := rand.New(rand.NewPCG(1, 1))
	counts := make([]int, z.N())
	for range 200_000 {
		counts[z.Sample(r)]++
	}
	if counts[0] < 10*counts[99] || counts[0] < counts[1] {
		t.Fatalf("rank 0: %d, rank 1: %d, rank 99: %d: not Zipfian", counts[0], counts[1], counts[99])
	}
}

func TestVocabularyCardinalities(t *testing.T) {
	buildVocab()
	for name, c := range map[string]struct {
		got  []string
		want int
	}{"words": {words, NumWords}, "brands": {brands, NumBrands}, "categories": {categories, NumCategories}, "tags": {tags, NumTags}} {
		seen := map[string]bool{}
		for _, v := range c.got {
			key := strings.ToLower(v)
			if seen[key] {
				t.Errorf("%s: %q twice", name, v)
			}
			seen[key] = true
		}
		if len(c.got) != c.want {
			t.Errorf("%s: %d values, want %d", name, len(c.got), c.want)
		}
	}
}

func TestSavedSearchesAreValidQueries(t *testing.T) {
	m := mapping(t)
	ops := map[string]int{}
	for i := range int64(3000) {
		s := Search(5, i)
		raw, err := json.Marshal(s.Query)
		if err != nil {
			t.Fatal(err)
		}
		n, ps := query.Parse(raw)
		if len(ps) > 0 {
			t.Fatalf("search %d does not parse: %v: %s", i, ps, raw)
		}
		if ps := query.Validate(n, m); len(ps) > 0 {
			t.Fatalf("search %d is not valid under the mapping: %v: %s", i, ps, raw)
		}
		query.Walk(n, func(_ string, node query.Node) bool {
			switch x := node.(type) {
			case *query.Leaf:
				ops[x.Op]++
			case *query.Not:
				ops["not"]++
			}
			return true
		})
	}
	for _, op := range []string{"eq", "in", "lte", "lt", "gte", "between", "contains", "contains_any", "words_all", "words_any", "has", "has_any", "not"} {
		if ops[op] == 0 {
			t.Errorf("no saved search uses %s", op)
		}
	}
	a, _ := AppendSearchLine(nil, 5, 77)
	b, _ := AppendSearchLine(nil, 5, 77)
	if !bytes.Equal(a, b) {
		t.Fatal("saved search 77 differs between two calls")
	}
}

func TestSplitProductLineFallback(t *testing.T) {
	id, doc, err := SplitProductLine([]byte(`{"doc": {"a": 1}, "id": "x\"y"}`))
	if err != nil || id != `x"y` || string(doc) != `{"a": 1}` {
		t.Fatalf("id %q doc %s err %v", id, doc, err)
	}
	if _, _, err := SplitProductLine([]byte(`{"id": "x"}`)); err == nil {
		t.Fatal("a line without doc was accepted")
	}
}
