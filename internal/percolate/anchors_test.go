package percolate

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// describe renders anchors compactly: "always", or the sorted atoms.
func describe(a Anchors) string {
	if a.Always {
		return "always"
	}
	var parts []string
	for _, t := range a.Terms {
		parts = append(parts, fmt.Sprintf("%s:%s=%q", t.Kind, t.Field, t.Term))
	}
	for _, r := range a.Ranges {
		parts = append(parts, fmt.Sprintf("range:%s=[%v,%v]", r.Field, r.Lo, r.Hi))
	}
	for _, p := range a.Pairs {
		parts = append(parts, fmt.Sprintf("%s:%s=%q&%s:%s=%q", p[0].Kind, p[0].Field, p[0].Term, p[1].Kind, p[1].Field, p[1].Term))
	}
	slices.Sort(parts)
	if len(parts) == 0 {
		return "never"
	}
	return strings.Join(parts, " ")
}

func TestExtractRules(t *testing.T) {
	inf := math.Inf(1)
	cases := []struct{ query, want string }{
		// Values, normalized as the matcher compares them; _id too.
		{`{"field":"brand","op":"eq","value":" ACME  Corp "}`, `value:brand="acme corp"`},
		{`{"field":"_id","op":"eq","value":"Straße"}`, `value:_id="strasse"`},
		{`{"field":"stock","op":"eq","value":true}`, `bool:stock="t"`},
		{`{"field":"price","op":"eq","value":5}`, `range:price=[5,5]`},
		{`{"field":"brand","op":"in","value":["A","b",false,3,1e400]}`, `bool:brand="f" range:brand=[3,3] value:brand="a" value:brand="b"`},
		{`{"field":"brand","op":"in","value":[1e400]}`, `never`},
		// Ranges.
		{`{"field":"price","op":"lt","value":10}`, fmt.Sprintf("range:price=[%v,10]", -inf)},
		{`{"field":"price","op":"gte","value":10}`, fmt.Sprintf("range:price=[10,%v]", inf)},
		{`{"field":"price","op":"between","value":[1,2]}`, `range:price=[1,2]`},
		{`{"field":"price","op":"between","value":[2,1]}`, `always`},
		{`{"field":"price","op":"lt","value":"x"}`, `always`},
		// Lists.
		{`{"field":"tags","op":"has_any","value":["A","b"]}`, `entry:tags="a" entry:tags="b"`},
		{`{"field":"tags","op":"nonempty"}`, `nonempty:tags=""`},
		{`{"field":"tags","op":"empty"}`, `always`},
		// Exists: only true (or anything but false) anchors.
		{`{"field":"x","op":"exists"}`, `present:x=""`},
		{`{"field":"x","op":"exists","value":"false"}`, `present:x=""`},
		{`{"field":"x","op":"exists","value":false}`, `always`},
		{`{"field":"x","op":"ne","value":1}`, `always`},
		// Needles: a short one needs only a text.
		{`{"field":"title","op":"contains","value":"ab"}`, `text:title=""`},
		{`{"field":"title","op":"starts_with","value":"x"}`, `text:title=""`},
		{`{"field":"title","op":"contains_any","value":["ab","cd"]}`, `text:title="" text:title=""`},
		{`{"field":"title","op":"words_any","value":["Hello World","x"]}`, `word:title="hello"&word:title="world" word:title="x"`},
		{`{"field":"title","op":"words_all","value":["!!!"]}`, `always`},
		{`{"field":"title","op":"words_any","value":["!!!"]}`, `always`},
		// Similar: a positive minimum anchors on trigram keys; a refused one needs a text.
		{`{"field":"title","op":"similar","value":{"text":"x","min":0}}`, `text:title=""`},
		// Groups.
		{`{"all":[]}`, `always`},
		{`{"all":[{"not":{"field":"a","op":"eq","value":1}},{"field":"b","op":"eq","value":"x"}]}`, `value:b="x"`},
		{`{"all":[{"not":{"field":"a","op":"eq","value":1}},{"field":"b","op":"ne","value":"x"}]}`, `always`},
		{`{"any":[{"field":"a","op":"eq","value":"x"},{"field":"b","op":"eq","value":"y"}]}`, `value:a="x" value:b="y"`},
		{`{"any":[{"field":"a","op":"eq","value":"x"},{"field":"b","op":"empty"}]}`, `always`},
		{`{"not":{"not":{"field":"a","op":"eq","value":"x"}}}`, `value:a="x"`},
		{`{"not":{"field":"a","op":"eq","value":"x"}}`, `always`},
		// all: the cheapest child; an eq beats a half-open range and a presence.
		{`{"all":[{"field":"p","op":"lt","value":3},{"field":"x","op":"exists"},{"field":"b","op":"eq","value":"y"}]}`, `value:b="y"`},
	}
	for _, c := range cases {
		n, problems := query.Parse([]byte(c.query))
		if len(problems) > 0 {
			t.Fatalf("%s: %v", c.query, problems)
		}
		if got := describe(Extract(n, nil)); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.query, got, c.want)
		}
	}
}

func TestExtractNeedleGrams(t *testing.T) {
	n, _ := query.Parse([]byte(`{"field":"title","op":"contains","value":"rtx 4090"}`))
	a := Extract(n, nil)
	// With no statistics a gram with a digit and no space is preferred.
	if len(a.Terms) != 1 || a.Terms[0].Kind != AtomGram || a.Terms[0].Term != "409" {
		t.Fatalf("got %s", describe(a))
	}
	// With statistics, the rarest gram.
	a = Extract(n, fixedStats{"title/3/x 4": 0, "*": 50})
	if len(a.Terms) != 1 || a.Terms[0].Term != "x 4" {
		t.Fatalf("got %s", describe(a))
	}
	// contains_all: the rarest needle; contains_any: one gram per needle.
	n, _ = query.Parse([]byte(`{"field":"title","op":"contains_all","value":["abcdef","zzz"]}`))
	if got := describe(Extract(n, fixedStats{"title/3/zzz": 1, "*": 40})); got != `gram:title="zzz"` {
		t.Fatalf("contains_all: %s", got)
	}
	n, _ = query.Parse([]byte(`{"field":"title","op":"contains_any","value":["abcd","zzz"]}`))
	if a := Extract(n, nil); len(a.Terms) != 2 {
		t.Fatalf("contains_any: %s", describe(a))
	}
}

func TestExtractChoosesRarestChildByStats(t *testing.T) {
	n, _ := query.Parse([]byte(`{"all":[{"field":"store","op":"eq","value":"big"},{"field":"title","op":"contains","value":"abc"}]}`))
	stats := fixedStats{"store/0/big": 900, "title/3/abc": 3}
	if got := describe(Extract(n, stats)); got != `gram:title="abc"` {
		t.Fatalf("got %s", got)
	}
	stats = fixedStats{"store/0/big": 2, "title/3/abc": 300}
	if got := describe(Extract(n, stats)); got != `value:store="big"` {
		t.Fatalf("got %s", got)
	}
}

// Two pairable children anchor together when the pair is rarer than either alone.
func TestExtractPairs(t *testing.T) {
	cases := []struct {
		query string
		stats shard.TermStats
		want  string
	}{
		{
			`{"all":[{"field":"store","op":"eq","value":"s"},{"field":"tags","op":"has_any","value":["a","b"]}]}`, nil,
			`value:store="s"&entry:tags="a" value:store="s"&entry:tags="b"`,
		},
		// A pair of an atom with itself is the atom.
		{
			`{"all":[{"field":"b","op":"eq","value":"x"},{"field":"b","op":"in","value":["x","y"]}]}`, nil,
			`value:b="x" value:b="x"&value:b="y"`,
		},
		// A selective single child beats a pair of common ones.
		{
			`{"all":[{"field":"store","op":"eq","value":"s"},{"field":"brand","op":"eq","value":"b"},{"field":"title","op":"contains","value":"xyz"}]}`,
			fixedStats{"store/0/s": 500, "brand/0/b": 500, "title/3/xyz": 1},
			`gram:title="xyz"`,
		},
		// Under the product estimate a pair beats each of its halves alone.
		{
			`{"all":[{"field":"store","op":"eq","value":"s"},{"field":"brand","op":"eq","value":"b"},{"field":"model","op":"eq","value":"m"}]}`,
			fixedStats{"store/0/s": 500, "brand/0/b": 500, "model/0/m": 1},
			`value:store="s"&value:model="m"`,
		},
		// Too many keys: no pair.
		{
			`{"all":[{"field":"a","op":"in","value":["1","2","3","4","5"]},{"field":"b","op":"in","value":["1","2","3","4"]}]}`, nil,
			`value:b="1" value:b="2" value:b="3" value:b="4"`,
		},
	}
	for _, c := range cases {
		n, problems := query.Parse([]byte(c.query))
		if len(problems) > 0 {
			t.Fatal(problems)
		}
		if got := describe(Extract(n, c.stats)); got != c.want {
			t.Errorf("%s: got %s, want %s", c.query, got, c.want)
		}
	}
}

func TestExtractSimilarKeys(t *testing.T) {
	n, _ := query.Parse([]byte(`{"field":"title","op":"similar","value":{"text":"ab","min":0.3}}`))
	a := Extract(n, nil)
	// "ab" has the pg_trgm trigrams "  a", " ab", "ab ".
	if len(a.Terms) != 3 || a.Terms[0].Kind != AtomSimKey {
		t.Fatalf("got %s", describe(a))
	}
}

// fixedStats maps "field/kind/term" to a document frequency; "*" is the default.
type fixedStats map[string]uint64

func (f fixedStats) NumDocs() uint64 { return 1000 }

func (f fixedStats) DocFreq(field string, kind segment.TermKind, term string) uint64 {
	if v, ok := f[fmt.Sprintf("%s/%d/%s", field, kind, term)]; ok {
		return v
	}
	return f["*"]
}
