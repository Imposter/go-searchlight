package search

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

// patternOracle holds, for each pattern the tests generate, a Go regular expression
// written apart from compilePattern's translation: brute force's model of it.
var patternOracle sync.Map

// oraclePattern registers lucene's model, goExpr (unanchored), and returns lucene.
func oraclePattern(lucene, goExpr string) string {
	patternOracle.Store(lucene, regexp.MustCompile(`^(?s:`+goExpr+`)$`))
	return lucene
}

// luceneQuote writes s as a pattern literal: letters and digits as they are, any
// other character escaped.
func luceneQuote(s string) string {
	var b strings.Builder
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// randomPattern returns a pattern built from seed (a normalized key or part of one),
// its model registered.
func randomPattern(rng *rand.Rand, seed, other string) string {
	rs := []rune(seed)
	k := rng.IntN(len(rs) + 1)
	switch rng.IntN(7) {
	case 0:
		return oraclePattern(luceneQuote(string(rs[:k]))+".*", regexp.QuoteMeta(string(rs[:k]))+".*")
	case 1:
		return oraclePattern(luceneQuote(seed)+"|"+luceneQuote(other), regexp.QuoteMeta(seed)+"|"+regexp.QuoteMeta(other))
	case 2:
		lo, hi := pick(rng, []rune("abcrt")), pick(rng, []rune("bdsuz"))
		lo, hi = min(lo, hi), max(lo, hi)
		return oraclePattern(fmt.Sprintf("[%c-%c].*", lo, hi), fmt.Sprintf("[%c-%c].*", lo, hi))
	case 3:
		return oraclePattern(".*"+luceneQuote(string(rs[k:])), ".*"+regexp.QuoteMeta(string(rs[k:])))
	case 4:
		if len(rs) == 0 {
			return oraclePattern(`"x"?`, `x?`)
		}
		return oraclePattern(luceneQuote(string(rs[:k/2]))+"."+luceneQuote(string(rs[min(len(rs), k/2+1):]))+"(x)?", regexp.QuoteMeta(string(rs[:k/2]))+"."+regexp.QuoteMeta(string(rs[min(len(rs), k/2+1):]))+"x?")
	case 5:
		return oraclePattern(`[^a-c][a-z0-9]{1,3}.*`, `[^a-c][a-z0-9]{1,3}.*`)
	default:
		return oraclePattern(`("b"|t)[0-9]+(\ [0-9]+)*`, `(b|t)[0-9]+( [0-9]+)*`)
	}
}

// randomTermsFilters returns an include and an exclude (either nil) for a terms
// aggregation over field: exact values (seed's, now and then a number, a bool, a
// value no document has, or none) or a pattern.
func randomTermsFilters(rng *rand.Rand, field string, seed func() string) (*TermsFilter, *TermsFilter) {
	one := func() *TermsFilter {
		switch fieldType(testMapping, field) {
		case schema.Number:
			f := &TermsFilter{Values: []any{}}
			for range rng.IntN(6) {
				f.Values = append(f.Values, pick(rng, []any{json.Number(randomNumber(rng)), float64(rng.IntN(20)), 0.0}))
			}
			return f
		case schema.Date:
			f := &TermsFilter{Values: []any{}}
			for range 1 + rng.IntN(20) {
				ms := 1_700_000_000_000 + rng.Int64N(400)*86_400_000
				f.Values = append(f.Values, pick(rng, []any{float64(ms), json.Number(strconv.FormatInt(ms, 10)), time.UnixMilli(ms).UTC().Format(time.RFC3339)}))
			}
			return f
		case schema.Bool:
			return &TermsFilter{Values: pick(rng, [][]any{{}, {true}, {false}, {true, false}})}
		}
		if raw := []rune(seed()); rng.IntN(4) == 0 && !slices.ContainsFunc(raw, analysis.IsSpace) {
			typed := string(raw[:rng.IntN(len(raw)+1)])
			return &TermsFilter{Pattern: oraclePattern(luceneQuote(typed)+".*", regexp.QuoteMeta(analysis.Clean(analysis.Normalize(typed)))+".*")}
		}
		if rng.IntN(2) == 0 {
			key := func() string { return analysis.Clean(analysis.Normalize(seed())) }
			return &TermsFilter{Pattern: randomPattern(rng, key(), key())}
		}
		f := &TermsFilter{Values: []any{}}
		for range rng.IntN(8) {
			f.Values = append(f.Values, pick(rng, []any{seed(), seed(), seed(), json.Number("10"), 12.5, true, "nothing has this"}))
		}
		return f
	}
	var include, exclude *TermsFilter
	switch rng.IntN(3) {
	case 0:
		include = one()
	case 1:
		exclude = one()
	default:
		include, exclude = one(), one()
	}
	return include, exclude
}

// bruteKeeps reports whether a terms aggregation's include and exclude let key v (as
// aggValues gives it) through, by the models: exact values normalized as the field
// indexes them, patterns by patternOracle.
func bruteKeeps(spec *aggSpec, v any) bool {
	return (spec.include == nil || bruteHas(spec.include, v)) && (spec.exclude == nil || !bruteHas(spec.exclude, v))
}

func bruteHas(f *TermsFilter, v any) bool {
	if f.Values == nil {
		re, ok := patternOracle.Load(f.Pattern)
		if !ok {
			panic(fmt.Sprintf("no model of the pattern %q", f.Pattern))
		}
		s, isText := v.(string)
		return isText && re.(*regexp.Regexp).MatchString(s) //nolint:forcetypeassert,errcheck // only regexps
	}
	for _, x := range f.Values {
		switch key := v.(type) {
		case string:
			if text, ok := analysis.AsText(x); ok && analysis.Clean(text) == key {
				return true
			}
		case float64:
			var n float64
			switch y := x.(type) {
			case float64:
				n = y
			case json.Number:
				n, _ = strconv.ParseFloat(string(y), 64)
			case string:
				at, err := time.Parse(time.RFC3339, y)
				if err != nil {
					continue
				}
				n = float64(at.UnixMilli())
			default:
				continue
			}
			if n == key {
				return true
			}
		case bool:
			if x == key {
				return true
			}
		}
	}
	return false
}

// TestPatternSubset: what compilePattern takes, and what each pattern matches (the
// whole key, nothing less); and what it refuses, where.
func TestPatternSubset(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		pattern, prefix string
		match, miss     []string
	}{
		{"ni.*", "ni", []string{"ni", "nike", "nikon x"}, []string{"anike", "n", "Nike"}},
		{"nike|nikon", "nik", []string{"nike", "nikon"}, []string{"nik", "nikes", "xnike"}},
		{"n(i|o)ke", "n", []string{"nike", "noke"}, []string{"nke", "nioke"}},
		{"[a-c]x", "", []string{"ax", "cx"}, []string{"dx", "x", "axx"}},
		{"[^a-c]x", "", []string{"dx", "éx"}, []string{"ax", "x"}},
		{"a+b?", "", []string{"a", "aa", "aab"}, []string{"b", "abb", ""}},
		{"a{2,3}", "aa", []string{"aa", "aaa"}, []string{"a", "aaaa"}},
		{"a{2}", "aa", []string{"aa"}, []string{"a", "aaa"}},
		{"a{2,}", "a", []string{"aa", "aaaaa"}, []string{"a"}},
		{"a**", "", []string{"", "aaa"}, []string{"b"}},
		{`"a.b"`, "a.b", []string{"a.b"}, []string{"axb"}},
		{`"a|b"c`, "a|bc", []string{"a|bc"}, []string{"ac"}},
		{`a\.b`, "a.b", []string{"a.b"}, []string{"axb"}},
		{`\*\^\$\~\&\<\>\@\#\{\}\(\)\[\]\"\\`, `*^$~&<>@#{}()[]"\`, []string{`*^$~&<>@#{}()[]"\`}, []string{"*"}},
		{".", "", []string{"é", "\n", "x", "σ"}, []string{"", "xy"}},
		{`[\]]`, "]", []string{"]"}, []string{`\`}},
		{`[a\-z]`, "", []string{"-", "a", "z"}, []string{"b"}},
		{`[-a]`, "", []string{"-", "a"}, []string{"b"}},
		{`[a-]`, "", []string{"-", "a"}, []string{"b"}},
		{`[.*+]`, "", []string{".", "*", "+"}, []string{"x"}},
		{"straße.*", "strasse", []string{"strasse", "strassen"}, []string{"straße"}},
		{"Nike.*", "nike", []string{"nike", "nikes"}, []string{"Nike"}},
		{"NI(KE|kon)", "nik", []string{"nike", "nikon"}, []string{"niKE"}},
		{"tea \t  cup", "tea cup", []string{"tea cup"}, []string{"tea  cup"}},
		{"  nike  |  puma", "", []string{"nike", "puma"}, []string{" nike", "nike "}},
		{`a\ \ b`, "a b", []string{"a b"}, []string{"a  b"}},
		{`" A  B "`, "a b", []string{"a b"}, []string{" a b "}},
		{"x +y", "x", []string{"x y", "x  y"}, []string{"xy"}},
		{"ΣΊΣΥΦΟΣ", "σίσυφοσ", []string{"σίσυφοσ"}, []string{"ΣΊΣΥΦΟΣ"}},
		{"a\x00b", "a�b", []string{"a�b"}, []string{"a\x00b"}},
		{"[A-C]x", "", []string{"ax", "cx"}, []string{"dx"}},
		{"[ÀÉ]", "", []string{"à", "é"}, []string{"a"}},
		{"[\t]x", "", []string{" x"}, []string{"x"}},
		{"[^a-c ]", "", []string{"d"}, []string{"a", " "}},
		{"tea cup", "tea cup", []string{"tea cup"}, []string{"teacup"}},
		{"(red|blue) .*", "", []string{"red cup", "blue pot"}, []string{"green pan"}},
		{"a|", "", []string{"a", ""}, []string{"b"}},
		{"()", "", []string{""}, []string{"x"}},
		{`""`, "", []string{""}, []string{"x"}},
		{strings.Repeat("a", MaxPatternChars), strings.Repeat("a", MaxPatternChars), []string{strings.Repeat("a", MaxPatternChars)}, []string{"a"}},
	} {
		re, err := compilePattern(tc.pattern)
		if err != nil {
			t.Errorf("%q: %v", tc.pattern, err)
			continue
		}
		if re.prefix != tc.prefix {
			t.Errorf("%q: literal prefix %q, want %q", tc.pattern, re.prefix, tc.prefix)
		}
		for _, s := range tc.match {
			if !re.re.MatchString(s) {
				t.Errorf("%q does not match %q", tc.pattern, s)
			}
			if !strings.HasPrefix(s, re.prefix) {
				t.Errorf("%q matches %q, which lacks its literal prefix %q", tc.pattern, s, re.prefix)
			}
		}
		for _, s := range tc.miss {
			if re.re.MatchString(s) {
				t.Errorf("%q matches %q", tc.pattern, s)
			}
		}
	}
	for _, tc := range []struct {
		pattern string
		pos     int
		says    string
	}{
		{"", -1, "empty"},
		{strings.Repeat("a", MaxPatternChars+1), -1, "characters"},
		{"a~b", 1, "Lucene operator"},
		{"a&b", 1, "Lucene operator"},
		{"<1-5>", 0, "Lucene operator"},
		{"a@", 1, "Lucene operator"},
		{"#", 0, "Lucene operator"},
		{"^ni", 0, "no anchor"},
		{"ni$", 2, "no anchor"},
		{"*a", 0, "repeats nothing"},
		{"a|+", 2, "repeats nothing"},
		{"(?i)a", 1, "repeats nothing"},
		{"a{1001}", 1, "repetition"},
		{"a{3,2}", 1, "repetition"},
		{"a{,3}", 1, "repetition"},
		{"a{x}", 1, "repetition"},
		{"a{2", 1, "repetition"},
		{"(ab", 0, "never closed"},
		{"ab)", 2, "opens no group"},
		{"[ab", 0, "never closed"},
		{"[]", 0, "empty class"},
		{"[^]", 0, "empty class"},
		{"[b-a]", 1, "backward"},
		{"[[]", 1, "within a class"},
		{`\d`, 1, "punctuation"},
		{`[\w]`, 2, "punctuation"},
		{`a\`, 1, "escapes nothing"},
		{`"ab`, 0, "never closed"},
		{"a}", 1, "closes nothing"},
		{"]", 0, "closes nothing"},
		{strings.Repeat("(", maxPatternDepth+1) + "a" + strings.Repeat(")", maxPatternDepth+1), maxPatternDepth, "nest"},
		{"((a{1000}){1000}){1000}", -1, "too complex"},
		{"x[ß]", 2, "more than one character"},
		{"[À-ÿ]", 1, "more than one character"},
		{"[^A-Z]", 2, "negated class"},
		{"[^\t]", 2, "negated class"},
		{strings.Repeat("[a-z]{1000}", 11), -1, "too complex"},
	} {
		_, err := compilePattern(tc.pattern)
		pe, ok := err.(*patternError) //nolint:errorlint // compilePattern's own error
		if !ok {
			t.Errorf("%.40q: %v, want a refusal", tc.pattern, err)
			continue
		}
		if pe.Pos != tc.pos || !strings.Contains(pe.Message, tc.says) {
			t.Errorf("%.40q: refused at %d with %q, want at %d saying %q", tc.pattern, pe.Pos, pe.Message, tc.pos, tc.says)
		}
	}
}

// TestPatternNegatedClassKeepsFolding: a refused negated class with whitespace in it
// (whose fold check appends to the code points FoldChanged returned) leaves the folding
// of later patterns as it was: [A-C] still takes the a that A folds to.
func TestPatternNegatedClassKeepsFolding(t *testing.T) {
	for _, pattern := range []string{"[^\t]", "[^\x00-\t]"} {
		if _, err := compilePattern(pattern); err == nil {
			t.Fatalf("%q: compiled, want the negated class refused", pattern)
		}
	}
	re, err := compilePattern("[A-C]x")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"ax", "bx", "cx"} {
		if !re.re.MatchString(s) {
			t.Errorf("after negated classes, %q does not match %q", "[A-C]x", s)
		}
	}
}

// TestTermsFilterProblems: include and exclude are refused at their own locs, at
// parse (their shape, a pattern's syntax, the bounds, partitions) and against the
// mapping (a pattern on a field without text keys, a value its type cannot hold, either
// on an aggregation other than terms).
func TestTermsFilterProblems(t *testing.T) {
	t.Parallel()
	values := make([]string, MaxTermsValues+1)
	for i := range values {
		values[i] = strconv.Itoa(i)
	}
	for _, tc := range []struct{ body, loc string }{
		{`{"aggs":{"b":{"terms":{"field":"brand","include":"^ni"}}}}`, "aggs.b.terms.include"},
		{`{"aggs":{"b":{"terms":{"field":"brand","exclude":"a~b"}}}}`, "aggs.b.terms.exclude"},
		{`{"aggs":{"b":{"terms":{"field":"brand","include":7}}}}`, "aggs.b.terms.include"},
		{`{"aggs":{"b":{"terms":{"field":"brand","include":{"partition":0,"num_partitions":4}}}}}`, "aggs.b.terms.include"},
		{`{"aggs":{"b":{"terms":{"field":"brand","include":{"x":1}}}}}`, "aggs.b.terms.include"},
		{`{"aggs":{"b":{"terms":{"field":"brand","include":["a",{"x":1}]}}}}`, "aggs.b.terms.include.1"},
		{`{"aggs":{"b":{"terms":{"field":"brand","include":["a",null]}}}}`, "aggs.b.terms.include.1"},
		{`{"aggs":{"b":{"terms":{"field":"brand","include":["a",["b"]]}}}}`, "aggs.b.terms.include.1"},
		{`{"aggs":{"b":{"terms":{"field":"brand","include":[` + strings.Join(values, ",") + `]}}}}`, "aggs.b.terms.include"},
		{`{"aggregations":{"g":{"global":{},"aggs":{"b":{"terms":{"field":"brand","exclude":"(a"}}}}}}`, "aggregations.g.aggs.b.terms.exclude"},
		{`{"aggs":{"p":{"stats":{"field":"price","include":["a"]}}}}`, "aggs.p.stats.include"},
	} {
		_, ps := ParseRequest([]byte(tc.body))
		if !slices.ContainsFunc(ps, func(p query.Problem) bool { return p.Loc == tc.loc }) {
			t.Errorf("%.120s: %v, want a problem at %s", tc.body, ps, tc.loc)
		}
	}
	for _, tc := range []struct {
		agg Agg
		loc string
	}{
		{Agg{Type: AggTerms, Field: "price", Include: &TermsFilter{Pattern: "1.*"}}, "aggs.x.terms.include"},
		{Agg{Type: AggTerms, Field: "created", Exclude: &TermsFilter{Pattern: "1.*"}}, "aggs.x.terms.exclude"},
		{Agg{Type: AggTerms, Field: "active", Include: &TermsFilter{Pattern: "true"}}, "aggs.x.terms.include"},
		{Agg{Type: AggTerms, Field: "price", Include: &TermsFilter{Values: []any{1.0, "cheap"}}}, "aggs.x.terms.include.1"},
		{Agg{Type: AggTerms, Field: "created", Include: &TermsFilter{Values: []any{"not a date"}}}, "aggs.x.terms.include.0"},
		{Agg{Type: AggTerms, Field: "active", Exclude: &TermsFilter{Values: []any{true, "false"}}}, "aggs.x.terms.exclude.1"},
		{Agg{Type: AggTerms, Field: "brand", Include: &TermsFilter{Values: []any{[]any{"a"}}}}, "aggs.x.terms.include.0"},
		{Agg{Type: AggTerms, Field: "brand", Include: &TermsFilter{}}, "aggs.x.terms.include"},
		{Agg{Type: AggRange, Field: "price", Ranges: []Range{{}}, Exclude: &TermsFilter{Values: []any{}}}, "aggs.x.range.exclude"},
	} {
		var ps problems
		prepareAgg("x", tc.agg, testMapping, "aggs.x", "", &ps)
		if !slices.ContainsFunc(ps, func(p query.Problem) bool { return p.Loc == tc.loc }) {
			t.Errorf("%+v: %v, want a problem at %s", tc.agg, ps, tc.loc)
		}
	}
	for _, a := range []Agg{
		{Type: AggTerms, Field: "created", Include: &TermsFilter{Values: []any{1.7e12, "2026-10-02", "2026-10-02T09:30:00.5Z", json.Number("5")}}},
		{Type: AggTerms, Field: "brand", Include: &TermsFilter{Values: []any{"Nike", json.Number("10"), 2.5, false}}, Exclude: &TermsFilter{Pattern: "n.*"}},
		{Type: AggTerms, Field: "nope", Include: &TermsFilter{Pattern: "x"}, Exclude: &TermsFilter{Values: []any{1.0, true}}},
	} {
		var ps problems
		prepareAgg("x", a, testMapping, "aggs.x", "", &ps)
		if len(ps) > 0 {
			t.Errorf("%+v refused: %v", a, ps)
		}
	}
}

// TestTermsFilterCompiledOnce: a parsed or decoded request's pattern compiles once,
// however many shards prepare it (and the reduce); a null include is no include.
func TestTermsFilterCompiledOnce(t *testing.T) {
	t.Parallel()
	r, ps := ParseRequest([]byte(`{"aggs":{"b":{"terms":{"field":"brand","include":"ni.*","exclude":null}}}}`))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	a := r.Aggs["b"]
	if a.Exclude != nil {
		t.Fatalf("a null exclude read as %+v", a.Exclude)
	}
	var decoded TermsFilter
	if err := json.Unmarshal([]byte(`"ni.*"`), &decoded); err != nil {
		t.Fatal(err)
	}
	for _, f := range []*TermsFilter{a.Include, &decoded} {
		var res []*regexp.Regexp
		for range 3 {
			s := prepareAgg("b", Agg{Type: AggTerms, Field: "brand", Include: f}, testMapping, "aggs.b", "", new(problems))
			res = append(res, s.terms.include.re)
		}
		if res[0] == nil || res[0] != res[1] || res[1] != res[2] {
			t.Fatalf("the pattern compiled more than once: %p %p %p", res[0], res[1], res[2])
		}
	}
}

// TestTermsFilterJSON: include and exclude read as Elasticsearch writes them, and
// round-trip through JSON (the peer wire) unchanged in meaning: a float keeps its
// fraction, so a keyword's text of it is the same.
func TestTermsFilterJSON(t *testing.T) {
	t.Parallel()
	r, ps := ParseRequest([]byte(`{"aggs":{"b":{"terms":{"field":"brand","include":["Nike",10,10.0,true],"exclude":"n.*"}}}}`))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	a := r.Aggs["b"]
	if a.Exclude.Values != nil || a.Exclude.Pattern != "n.*" || len(a.Include.Values) != 4 || a.Include.Values[1] != json.Number("10") || a.Include.Values[2] != json.Number("10.0") {
		t.Fatalf("parsed %+v / %+v", a.Include, a.Exclude)
	}
	hand := Agg{Type: AggTerms, Field: "brand", Include: &TermsFilter{Values: []any{10.0, 12.5, "x"}}, Exclude: &TermsFilter{Values: []any{}}}
	raw, err := json.Marshal(hand)
	if err != nil {
		t.Fatal(err)
	}
	var back Agg
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Exclude == nil || back.Exclude.Values == nil || len(back.Exclude.Values) != 0 {
		t.Fatalf("an empty exclude came back as %+v (%s)", back.Exclude, raw)
	}
	for i, v := range hand.Include.Values {
		w, _ := analysis.AsText(v)
		g, _ := analysis.AsText(back.Include.Values[i])
		if w != g {
			t.Errorf("value %d: %q after the round trip, want %q (%s)", i, g, w, raw)
		}
	}
	if string(mustJSON(&TermsFilter{Pattern: "a.*"})) != `"a.*"` {
		t.Error("a pattern is not written as a string")
	}
}

// TestTermsFilterSemantics: include and exclude choose the values before any cut, as
// Elasticsearch: the top N are those of the included values; exclude wins over
// include; sum_other_doc_count counts only included values; exact values compare
// normalized; and an empty include chooses none.
func TestTermsFilterSemantics(t *testing.T) {
	t.Parallel()
	c := newCluster(t, 1)
	counts := map[string]int{"nike": 9, "nikon": 7, "nissan": 5, "adidas": 20, "puma": 15, "Niche": 3}
	n := 0
	for brand, k := range counts {
		for range k {
			c.upsert(fmt.Sprintf("d%03d", n), fmt.Sprintf(`{"brand":%q,"price":%d,"active":%t}`, brand, n%4, n%3 == 0))
			n++
		}
	}
	c.refreshAll()
	run := func(a Agg) *AggResult {
		t.Helper()
		got, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"t": a}})
		if err != nil {
			t.Fatal(err)
		}
		return got.Aggs["t"]
	}
	for _, tc := range []struct {
		agg   Agg
		want  string
		other int64
	}{
		{Agg{Type: AggTerms, Field: "brand", Size: 2, Include: &TermsFilter{Pattern: "ni.*"}}, "[nike:9 nikon:7]", 8},
		{Agg{Type: AggTerms, Field: "brand", Size: 2, Include: &TermsFilter{Pattern: "ni.*"}, Exclude: &TermsFilter{Values: []any{"NIKE"}}}, "[nikon:7 nissan:5]", 3},
		{Agg{Type: AggTerms, Field: "brand", Size: 10, Include: &TermsFilter{Values: []any{"Nike", "  puma ", "missing"}}}, "[puma:15 nike:9]", 0},
		{Agg{Type: AggTerms, Field: "brand", Size: 1, Include: &TermsFilter{Values: []any{"nike", "puma"}}, Exclude: &TermsFilter{Values: []any{"puma"}}}, "[nike:9]", 0},
		{Agg{Type: AggTerms, Field: "brand", Size: 10, Exclude: &TermsFilter{Pattern: "n.*|a.*"}}, "[puma:15]", 0},
		{Agg{Type: AggTerms, Field: "brand", Size: 10, Include: &TermsFilter{Values: []any{}}}, "[]", 0},
		{Agg{Type: AggTerms, Field: "brand", Size: 10, Include: &TermsFilter{Pattern: "Ni.*"}}, "[nike:9 nikon:7 nissan:5 niche:3]", 0},
		{Agg{Type: AggTerms, Field: "brand", Size: 10, MinDocCount: ptr(6), Include: &TermsFilter{Pattern: "n.*"}}, "[nike:9 nikon:7]", 8},
		{Agg{Type: AggTerms, Field: "price", Size: 10, Include: &TermsFilter{Values: []any{1.0, json.Number("3"), math.Copysign(0, -1)}}, Exclude: &TermsFilter{Values: []any{3}}}, "[0:15 1:15]", 0},
		{Agg{Type: AggTerms, Field: "active", Size: 10, Exclude: &TermsFilter{Values: []any{false}}}, "[true:20]", 0},
	} {
		res := run(tc.agg)
		if g := fmt.Sprint(bucketKeys(res.Buckets)); g != tc.want || res.SumOtherDocCount != tc.other {
			t.Errorf("%+v / %+v: %s other %d, want %s other %d", tc.agg.Include, tc.agg.Exclude, g, res.SumOtherDocCount, tc.want, tc.other)
		}
	}
}

// TestTermsFilterAcrossShards: with a small shard_size over several shards, a filtered
// terms aggregation is bounded as an unfiltered one over the chosen values alone:
// every count within its error bound of the truth, sum_other_doc_count the chosen
// values' documents outside the buckets, and no chosen value with more documents than
// the bound allows left out. Under a filter aggregation too.
func TestTermsFilterAcrossShards(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x1c1d))
	c := newCluster(t, 4)
	for i := range testtier.Pick(1500, 4000) {
		b := fmt.Sprintf("%s%02d", pick(rng, []string{"ni", "ad", "pu"}), min(39, int(rng.ExpFloat64()*6)))
		c.upsert(fmt.Sprintf("d%04d", i), fmt.Sprintf(`{"brand":%q,"price":%d}`, b, rng.IntN(10)))
	}
	c.refreshAll()
	include, exclude := &TermsFilter{Pattern: "ni.*|pu0[0-4]"}, &TermsFilter{Values: []any{"ni00", "NI03"}}
	keep := func(key string) bool {
		return (strings.HasPrefix(key, "ni") || (strings.HasPrefix(key, "pu0") && key[3] <= '4')) && key != "ni00" && key != "ni03"
	}
	for _, wrapped := range []bool{false, true} {
		truth := map[string]int64{}
		for _, d := range c.docs() {
			if key := *d.Fields["brand"].Text; keep(key) && (!wrapped || *d.Fields["price"].Number < 5) {
				truth[key]++
			}
		}
		for _, shardSize := range []int{1, 2, 3, 5, 8, 1000} {
			terms := Agg{Type: AggTerms, Field: "brand", Size: 5, ShardSize: shardSize, Include: include, Exclude: exclude}
			aggs := map[string]Agg{"t": terms}
			if wrapped {
				aggs = map[string]Agg{"w": {Type: AggFilter, Filter: mustParse(t, `{"field":"price","op":"lt","value":5}`), Aggs: aggs}}
			}
			got, err := c.search(&Request{Query: &query.All{}, Aggs: aggs})
			if err != nil {
				t.Fatal(err)
			}
			res := got.Aggs["t"]
			if wrapped {
				res = got.Aggs["w"].Aggs["t"]
			}
			var returned, all int64
			for _, b := range res.Buckets {
				key, _ := b.Key.(string)
				tc := truth[key]
				if !keep(key) || b.DocCount > tc || tc > b.DocCount+b.DocCountErrorUpperBound {
					t.Errorf("wrapped %t shard_size %d: %s counts %d (+%d), truth %d", wrapped, shardSize, key, b.DocCount, b.DocCountErrorUpperBound, tc)
				}
				returned += b.DocCount
			}
			for _, n := range truth {
				all += n
			}
			if res.SumOtherDocCount != all-returned {
				t.Errorf("wrapped %t shard_size %d: sum_other %d, want %d", wrapped, shardSize, res.SumOtherDocCount, all-returned)
			}
			if shardSize == 1000 && res.DocCountErrorUpperBound != 0 {
				t.Errorf("shard_size past every term: error bound %d", res.DocCountErrorUpperBound)
			}
			if len(res.Buckets) > 0 {
				last := res.Buckets[len(res.Buckets)-1].DocCount
				for key, n := range truth {
					if n > last+res.DocCountErrorUpperBound && !slices.ContainsFunc(res.Buckets, func(b *Bucket) bool { return b.Key == key }) {
						t.Errorf("wrapped %t shard_size %d: %s (%d) missing past the bound %d+%d", wrapped, shardSize, key, n, last, res.DocCountErrorUpperBound)
					}
				}
			}
		}
	}
}

// TestTermsFilterEveryPath: include and exclude on keyword, list, text, number and bool
// fields over several segments with deletes, on the dense path (most of a segment hit,
// counted from the dictionary) and per document, whole or in parts, merged by term or
// by global ordinal, equal brute force.
func TestTermsFilterEveryPath(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x7f17))
	c := revCorpus(t, rng, 2, 4, testtier.Pick(800, 2500))
	docs := c.docs()
	defer func() { aggChunkMin, aggChunk, globalOrdsDensity, lazyTermsRatio = 1<<15, 1<<16, 8, 16 }()
	seedText := func() string {
		return pick(rng, []string{fmt.Sprintf("B%03d", rng.IntN(40)), fmt.Sprintf("t%02d", rng.IntN(60)), "red cup 1", "7"})
	}
	for i := range cases(200) {
		raw := pick(rng, []string{`{"all":[]}`, fmt.Sprintf(`{"field":"brand","op":"ne","value":"b%03d"}`, rng.IntN(3)), `{"field":"price","op":"gte","value":10}`})
		q := mustParse(t, raw)
		a := Agg{Type: AggTerms, Field: pick(rng, []string{"brand", "brand", "tags", "title", "price", "active"}), Size: pick(rng, []int{1, 5, 50}), ShardSize: 1_000_000}
		a.Include, a.Exclude = randomTermsFilters(rng, a.Field, seedText)
		if rng.IntN(4) == 0 {
			a.Aggs = map[string]Agg{"m": {Type: AggStats, Field: "price"}}
		}
		want := bruteAgg(t, a, brute(docs, q, nil))
		if chunk := rng.IntN(3); chunk > 0 {
			aggChunkMin, aggChunk = 1, 1+rng.Uint32N(700)
		}
		globalOrdsDensity = pick(rng, []uint64{0, 8, 1 << 40})
		lazyTermsRatio = pick(rng, []uint64{0, 16, 1 << 40})
		got, err := c.search(&Request{Query: q, Aggs: map[string]Agg{"a": a}})
		aggChunkMin, aggChunk, globalOrdsDensity, lazyTermsRatio = 1<<15, 1<<16, 8, 16
		if err != nil {
			t.Fatalf("%s %+v: %v", raw, a, err)
		}
		if d := diffAgg(got.Aggs["a"], want, "a"); d != "" {
			t.Fatalf("case %d, %s, %s include %+v exclude %+v:\n%s", i, raw, a.Field, a.Include, a.Exclude, d)
		}
	}
}
