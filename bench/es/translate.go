package es

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
)

// Translator turns Searchlight queries and searches into Elasticsearch's DSL for an
// index mapped by [IndexBody] from the same fields.
type Translator struct {
	fields map[string]datasets.Field
}

// NewTranslator returns a translator for documents with fields.
func NewTranslator(fields []datasets.Field) *Translator {
	m := make(map[string]datasets.Field, len(fields))
	for _, f := range fields {
		m[f.Name] = f
	}
	return &Translator{fields: m}
}

// Query DSL constants.
var (
	matchAll  = map[string]any{"match_all": map[string]any{}}
	matchNone = map[string]any{"match_none": map[string]any{}}
)

func isMatchAll(q map[string]any) bool  { _, ok := q["match_all"]; return ok && len(q) == 1 }
func isMatchNone(q map[string]any) bool { _, ok := q["match_none"]; return ok && len(q) == 1 }

func boolQuery(clause string, qs []map[string]any) map[string]any {
	b := map[string]any{clause: qs}
	if clause == "should" {
		b["minimum_should_match"] = 1
	}
	return map[string]any{"bool": b}
}

func mustNot(q map[string]any) map[string]any {
	switch {
	case isMatchAll(q):
		return matchNone
	case isMatchNone(q):
		return matchAll
	}
	return boolQuery("must_not", []map[string]any{q})
}

// group folds constant children as Searchlight's compiler does (all: a match_none child
// decides, match_all children drop out; any the other way round).
func group(every bool, children []map[string]any) map[string]any {
	decides, drops := matchNone, matchAll
	if !every {
		decides, drops = matchAll, matchNone
	}
	kept := make([]map[string]any, 0, len(children))
	for _, c := range children {
		switch {
		case isMatchAll(c) && isMatchAll(decides), isMatchNone(c) && isMatchNone(decides):
			return decides
		case isMatchAll(c) && isMatchAll(drops), isMatchNone(c) && isMatchNone(drops):
			continue
		}
		kept = append(kept, c)
	}
	switch len(kept) {
	case 0:
		return drops
	case 1:
		return kept[0]
	}
	if every {
		return boolQuery("filter", kept)
	}
	return boolQuery("should", kept)
}

// ParseQuery parses a Searchlight query and translates it.
func (t *Translator) ParseQuery(raw []byte) (map[string]any, error) {
	n, ps := query.Parse(raw)
	if len(ps) > 0 {
		return nil, &InvalidError{Problems: ps}
	}
	return t.Query(n)
}

// Query translates a parsed Searchlight query into an Elasticsearch query that matches
// exactly the same documents (within [Caveats]).
func (t *Translator) Query(n query.Node) (map[string]any, error) {
	switch x := n.(type) {
	case *query.All:
		if x == nil {
			return matchNone, nil
		}
		return t.groupOf(true, x.Children)
	case *query.Any:
		if x == nil {
			return matchNone, nil
		}
		return t.groupOf(false, x.Children)
	case *query.Not:
		if x == nil {
			return matchNone, nil
		}
		c, err := t.Query(x.Child)
		if err != nil {
			return nil, err
		}
		return mustNot(c), nil
	case *query.Leaf:
		if x == nil {
			return matchNone, nil
		}
		return t.leaf(x)
	default:
		return nil, fmt.Errorf("es: unknown query node %T", n)
	}
}

func (t *Translator) groupOf(every bool, nodes []query.Node) (map[string]any, error) {
	children := make([]map[string]any, 0, len(nodes))
	for _, n := range nodes {
		c, err := t.Query(n)
		if err != nil {
			return nil, err
		}
		children = append(children, c)
	}
	return group(every, children), nil
}

// fieldKind is how a field's values compare.
type fieldKind uint8

const (
	kindText fieldKind = iota // keyword, text, keyword_list entries, _id
	kindNumber
	kindBool
)

func (t *Translator) field(name string) (datasets.Field, fieldKind, error) {
	if name == search.IDField {
		return datasets.Field{Name: "_id", Type: datasets.Keyword}, kindText, nil
	}
	f, ok := t.fields[name]
	if !ok {
		return f, 0, fmt.Errorf("es: field %q is not mapped", name)
	}
	switch f.Type {
	case datasets.Number, datasets.Date:
		return f, kindNumber, nil
	case datasets.Bool:
		return f, kindBool, nil
	default:
		return f, kindText, nil
	}
}

// scalarFor returns s as a value of a field of kind k, or false when s is of another
// kind (which equals nothing in that field, as Searchlight's typed parts never do).
func scalarFor(k fieldKind, s *query.Scalar) (any, bool) {
	switch {
	case k == kindText && s.Kind == query.ArgString:
		return s.Norm, true
	case k == kindNumber && s.Kind == query.ArgNumber && s.Finite:
		return s.Number, true
	case k == kindBool && s.Kind == query.ArgBool:
		return s.Bool, true
	}
	return nil, false
}

func term(field string, v any) map[string]any {
	return map[string]any{"term": map[string]any{field: v}}
}

func exists(field string) map[string]any {
	return map[string]any{"exists": map[string]any{"field": field}}
}

func (t *Translator) leaf(l *query.Leaf) (map[string]any, error) {
	f, kind, err := t.field(l.Field)
	if err != nil {
		return nil, err
	}
	if f.Type != "" && f.Name != "_id" && !slices.Contains(opsFor(f.Type), l.Op) {
		return nil, fmt.Errorf("es: %s does not apply to %s field %s", l.Op, f.Type, l.Field)
	}
	if f.Name == "_id" && !slices.Contains([]string{query.OpEq, query.OpNe, query.OpIn, query.OpExists}, l.Op) {
		return nil, fmt.Errorf("es: %s on _id has no Elasticsearch equivalent (_id takes term and ids queries only)", l.Op)
	}
	a := l.Arg
	switch l.Op {
	case query.OpEq, query.OpNe:
		q := matchNone
		if v, ok := scalarFor(kind, &a.Scalar); ok && a.Kind == a.Scalar.Kind {
			q = t.eq(f, v)
		}
		if l.Op == query.OpNe {
			return mustNot(q), nil
		}
		return q, nil
	case query.OpIn:
		if a.Kind != query.ArgList {
			return matchNone, nil
		}
		var vals []any
		for i := range a.List {
			if v, ok := scalarFor(kind, &a.List[i]); ok && !slices.Contains(vals, v) {
				vals = append(vals, v)
			}
		}
		if len(vals) == 0 {
			return matchNone, nil
		}
		if f.Name == "_id" {
			return map[string]any{"ids": map[string]any{"values": vals}}, nil
		}
		return map[string]any{"terms": map[string]any{f.Name: vals}}, nil
	case query.OpLt, query.OpLte, query.OpGt, query.OpGte:
		if a.Kind != query.ArgNumber || !a.Scalar.Finite {
			return matchNone, nil
		}
		return map[string]any{"range": map[string]any{f.Name: map[string]any{l.Op: a.Scalar.Number}}}, nil
	case query.OpBetween:
		if a.Kind != query.ArgList || len(a.List) != 2 ||
			a.List[0].Kind != query.ArgNumber || !a.List[0].Finite || a.List[1].Kind != query.ArgNumber || !a.List[1].Finite ||
			a.List[0].Number > a.List[1].Number {
			return matchNone, nil
		}
		return map[string]any{"range": map[string]any{f.Name: map[string]any{"gte": a.List[0].Number, "lte": a.List[1].Number}}}, nil
	case query.OpExists:
		want := a.Kind != query.ArgBool || a.Scalar.Bool
		if f.Name == "_id" {
			if want {
				return matchAll, nil
			}
			return matchNone, nil
		}
		if want {
			return exists(f.Name), nil
		}
		return mustNot(exists(f.Name)), nil
	case query.OpEmpty:
		return mustNot(exists(f.Name)), nil
	case query.OpNonempty:
		return exists(f.Name), nil
	case query.OpContains, query.OpContainsAny, query.OpContainsAll:
		needles := distinctNorms(texts(&a))
		if len(needles) == 0 {
			return matchNone, nil
		}
		qs := make([]map[string]any, len(needles))
		for i, n := range needles {
			qs[i] = t.contains(f, n)
		}
		return group(l.Op == query.OpContainsAll, qs), nil
	case query.OpStartsWith:
		if a.Kind != query.ArgString {
			return matchNone, nil
		}
		return map[string]any{"prefix": map[string]any{f.Name: a.Scalar.Norm}}, nil
	case query.OpWordsAll, query.OpWordsAny:
		every := l.Op == query.OpWordsAll
		var qs []map[string]any
		var seen []string
		for _, s := range texts(&a) {
			words := analysis.Words(s.Text)
			if words == analysis.NoWords {
				if every {
					return matchNone, nil
				}
				continue
			}
			phrase := strings.TrimSpace(words)
			if slices.Contains(seen, phrase) {
				continue
			}
			seen = append(seen, phrase)
			qs = append(qs, map[string]any{"match_phrase": map[string]any{f.Name + WordsSuffix: phrase}})
		}
		if len(qs) == 0 {
			return matchNone, nil
		}
		return group(every, qs), nil
	case query.OpSimilar:
		return t.similar(f, &a)
	case query.OpHas, query.OpHasAny, query.OpHasAll:
		entries := distinctNorms(texts(&a))
		if len(entries) == 0 {
			return matchNone, nil
		}
		if l.Op == query.OpHasAll {
			qs := make([]map[string]any, len(entries))
			for i, e := range entries {
				qs[i] = term(f.Name, e)
			}
			return group(true, qs), nil
		}
		if len(entries) == 1 {
			return term(f.Name, entries[0]), nil
		}
		vals := make([]any, len(entries))
		for i, e := range entries {
			vals[i] = e
		}
		return map[string]any{"terms": map[string]any{f.Name: vals}}, nil
	default:
		return nil, fmt.Errorf("es: unknown op %q", l.Op)
	}
}

func (t *Translator) eq(f datasets.Field, v any) map[string]any {
	if f.Name == "_id" {
		return map[string]any{"ids": map[string]any{"values": []any{v}}}
	}
	return term(f.Name, v)
}

// texts returns a value's strings (a string, or a list's strings).
func texts(a *query.Arg) []query.Scalar {
	switch a.Kind {
	case query.ArgString:
		return []query.Scalar{a.Scalar}
	case query.ArgList:
		var out []query.Scalar
		for _, s := range a.List {
			if s.Kind == query.ArgString {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func distinctNorms(ss []query.Scalar) []string {
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if !slices.Contains(out, s.Norm) {
			out = append(out, s.Norm)
		}
	}
	return out
}

// contains is a substring test on a normalized keyword. A needle of three or more
// characters is a phrase of its 3-grams on the trigram subfield: consecutive windows at
// consecutive positions are exactly the needle's occurrences. A shorter needle (no
// full window), or a field without the subfield, is an exact wildcard over the
// normalized keyword, which scans its terms.
func (t *Translator) contains(f datasets.Field, needle string) map[string]any {
	if f.Substring && utf8.RuneCountInString(needle) >= 3 {
		return map[string]any{"match_phrase": map[string]any{f.Name + TrigramSuffix: needle}}
	}
	return map[string]any{"wildcard": map[string]any{f.Name: map[string]any{"value": "*" + escapeWildcard(needle) + "*"}}}
}

func escapeWildcard(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == '*' || r == '?' || r == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// similarScript is pg_trgm similarity over the normalized keyword's doc value, as
// Searchlight computes it: the text split into runs of letters and digits, each padded
// with two spaces before and one after, every 3-character window, distinct; shared
// trigrams over the union, rounded to a float, at least min.
const similarScript = `if (doc[params.f].size() == 0) { return false; }
String s = doc[params.f].value; Set g = new HashSet(); int n = s.length(); int i = 0;
while (i < n) {
  while (i < n && !Character.isLetterOrDigit(s.charAt(i))) { i++; }
  if (i >= n) { break; }
  int j = i;
  while (j < n && Character.isLetterOrDigit(s.charAt(j))) { j++; }
  String w = '  ' + s.substring(i, j) + ' ';
  for (int k = 0; k + 3 <= w.length(); k++) { g.add(w.substring(k, k + 3)); }
  i = j;
}
if (g.isEmpty()) { return false; }
int shared = 0;
for (def t : params.q) { if (g.contains(t)) { shared++; } }
double r = (double) (float) ((double) shared / (g.size() + params.q.size() - shared));
return r >= params.min;`

func (t *Translator) similar(f datasets.Field, a *query.Arg) (map[string]any, error) {
	if a.Kind != query.ArgObject {
		return matchNone, nil
	}
	text, minimum := a.Object["text"], a.Object["min"]
	if text.Kind != query.ArgString || minimum.Kind != query.ArgNumber || !minimum.Finite || minimum.Number <= 0 || minimum.Number > 1 {
		return matchNone, nil
	}
	grams := analysis.Trigrams(text.Norm)
	if len(grams) == 0 {
		return matchNone, nil
	}
	if !f.DocValues {
		return nil, fmt.Errorf("es: similar on %s needs doc values in the Elasticsearch mapping (Field.DocValues)", f.Name)
	}
	return map[string]any{"script": map[string]any{"script": map[string]any{
		"lang": "painless", "source": similarScript,
		"params": map[string]any{"f": f.Name, "q": grams, "min": minimum.Number},
	}}}, nil
}

// Search translates a Searchlight search request body into an Elasticsearch _search
// body: the query in filter context (no scoring: Searchlight does not score), the
// same size, sort with the id tie-breaker, search_after, track_total_hits,
// aggregations, _source filtering and timeout.
func (t *Translator) Search(raw []byte) (map[string]any, error) {
	req, ps := search.ParseRequest(raw)
	if len(ps) > 0 {
		return nil, &InvalidError{Problems: ps}
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, err
	}
	q, err := t.Query(req.Query)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"query": map[string]any{"constant_score": map[string]any{"filter": q}}}
	size := req.Size
	if _, given := top["size"]; !given {
		size = 10 // the API's DefaultSize
	}
	body["size"] = size

	sorts, err := t.sort(req.Sort)
	if err != nil {
		return nil, err
	}
	if size > 0 || len(req.SearchAfter) > 0 {
		body["sort"] = sorts
	}
	if len(req.SearchAfter) > 0 {
		body["search_after"] = req.SearchAfter
	}
	switch req.TrackTotal {
	case 0:
	case search.TrackTotalAll:
		body["track_total_hits"] = true
	case search.TrackTotalNone:
		body["track_total_hits"] = false
	default:
		body["track_total_hits"] = req.TrackTotal
	}
	if len(req.Aggs) > 0 {
		aggs, err := t.aggs(req.Aggs)
		if err != nil {
			return nil, err
		}
		body["aggs"] = aggs
	}
	if req.Fields != nil {
		body["_source"] = map[string]any{"includes": req.Fields}
	}
	if req.Timeout > 0 {
		body["timeout"] = strconv.FormatInt(req.Timeout.Milliseconds(), 10) + "ms"
	}
	return body, nil
}

func (t *Translator) sort(fields []search.SortField) ([]any, error) {
	out := make([]any, 0, len(fields)+1)
	for _, s := range fields {
		order := "asc"
		if s.Desc {
			order = "desc"
		}
		if s.Field == search.IDField {
			out = append(out, map[string]any{IDField: map[string]any{"order": order}})
			return out, nil // the id is total: Searchlight's sort ends there too
		}
		f, ok := t.fields[s.Field]
		if !ok {
			return nil, fmt.Errorf("es: sort field %q is not mapped", s.Field)
		}
		if (f.Type == datasets.Keyword || f.Type == datasets.Text || f.Type == datasets.KeywordList) && !f.DocValues {
			return nil, fmt.Errorf("es: sort on %s needs doc values in the Elasticsearch mapping", s.Field)
		}
		out = append(out, map[string]any{s.Field: map[string]any{"order": order, "missing": "_last"}})
	}
	return append(out, map[string]any{IDField: map[string]any{"order": "asc"}}), nil
}

func (t *Translator) aggs(aggs map[string]search.Agg) (map[string]any, error) {
	out := make(map[string]any, len(aggs))
	for name, a := range aggs { //nolint:gocritic // map[string]search.Agg: indexing can't avoid the copy either
		f, ok := t.fields[a.Field]
		if !ok {
			return nil, fmt.Errorf("es: aggregation %s: field %q is not mapped", name, a.Field)
		}
		if (f.Type == datasets.Keyword || f.Type == datasets.Text || f.Type == datasets.KeywordList) && !f.DocValues {
			return nil, fmt.Errorf("es: aggregation %s on %s needs doc values in the Elasticsearch mapping", name, a.Field)
		}
		body := map[string]any{"field": a.Field}
		switch a.Type {
		case search.AggTerms:
			size := a.Size
			if size == 0 {
				size = search.DefaultTermsSize
			}
			body["size"] = size
			if a.ShardSize > 0 {
				body["shard_size"] = a.ShardSize
			}
			if a.MinDocCount != nil {
				body["min_doc_count"] = *a.MinDocCount
			}
		case search.AggRange:
			ranges := make([]any, len(a.Ranges))
			for i, r := range a.Ranges {
				rg := map[string]any{"key": rangeKey(r)}
				if r.From != nil {
					rg["from"] = *r.From
				}
				if r.To != nil {
					rg["to"] = *r.To
				}
				ranges[i] = rg
			}
			body["ranges"] = ranges
		case search.AggHistogram:
			body["interval"] = a.Interval
			if a.Offset != 0 {
				body["offset"] = a.Offset
			}
			body["min_doc_count"] = minDocs(a.MinDocCount)
		case search.AggDateHistogram:
			if a.Calendar != "" {
				body["calendar_interval"] = a.Calendar
			} else {
				body["fixed_interval"] = strconv.FormatFloat(a.Interval, 'f', -1, 64) + "ms"
			}
			if a.Offset != 0 {
				body["offset"] = strconv.FormatFloat(a.Offset, 'f', -1, 64) + "ms"
			}
			body["min_doc_count"] = minDocs(a.MinDocCount)
		case search.AggStats:
		case search.AggCardinality:
			p := a.Precision
			if p == 0 {
				p = search.DefaultPrecision
			}
			body["precision_threshold"] = min(1<<(p-2), 40_000)
		default:
			return nil, fmt.Errorf("es: aggregation %s: unknown type %q", name, a.Type)
		}
		agg := map[string]any{a.Type: body}
		if len(a.Aggs) > 0 {
			subs, err := t.aggs(a.Aggs)
			if err != nil {
				return nil, err
			}
			agg["aggs"] = subs
		}
		out[name] = agg
	}
	return out, nil
}

func minDocs(n *int) int {
	if n == nil {
		return 0
	}
	return *n
}

// rangeKey names a range bucket as Searchlight does when the request gives no key:
// "from-to", with * for an open end.
func rangeKey(r search.Range) string {
	if r.Key != "" {
		return r.Key
	}
	end := func(p *float64) string {
		if p == nil {
			return "*"
		}
		return strconv.FormatFloat(*p, 'f', -1, 64)
	}
	return end(r.From) + "-" + end(r.To)
}

// PercolatorDoc is a saved query's document in the percolator index.
func (t *Translator) PercolatorDoc(id string, raw, meta json.RawMessage) (map[string]any, error) {
	q, err := t.ParseQuery(raw)
	if err != nil {
		return nil, fmt.Errorf("saved query %s: %w", id, err)
	}
	doc := map[string]any{QueryField: q, QueryIDField: id}
	if len(bytes.TrimSpace(meta)) > 0 && !bytes.Equal(bytes.TrimSpace(meta), []byte("null")) {
		doc["meta"] = meta
	}
	return doc, nil
}

// PercolateBody is the _search body that percolates docs: every saved query matching
// any of them, in filter context, with the slots each matched.
func PercolateBody(docs []json.RawMessage, size int) map[string]any {
	return map[string]any{
		"query": map[string]any{"constant_score": map[string]any{"filter": map[string]any{
			"percolate": map[string]any{"field": QueryField, "documents": docs},
		}}},
		"size":             size,
		"_source":          false,
		"track_total_hits": true,
	}
}

// InvalidError is a Searchlight request that does not parse.
type InvalidError struct {
	Problems []query.Problem
}

func (e *InvalidError) Error() string {
	parts := make([]string, len(e.Problems))
	for i, p := range e.Problems {
		parts[i] = p.String()
	}
	return "es: invalid Searchlight request: " + strings.Join(parts, "; ")
}

// opsFor is the operators a dataset field type takes.
func opsFor(t datasets.FieldType) []string {
	st, err := schema.ParseFieldType(string(t))
	if err != nil {
		return nil
	}
	return query.OpsFor(st)
}
