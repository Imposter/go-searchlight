package search

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
)

// aggProblems are the problems of a search body with aggs alone: ParseRequest's, else
// those of its checks against the test mapping.
func aggProblems(t *testing.T, aggs string) []query.Problem {
	t.Helper()
	r, ps := ParseRequest([]byte(`{"size":0,"aggs":` + aggs + `}`))
	if len(ps) > 0 {
		return ps
	}
	if ps := ValidateQueries(r, testMapping); len(ps) > 0 {
		return ps
	}
	_, err := prepare(r, testMapping)
	var re *RequestError
	if errors.As(err, &re) {
		return re.Problems
	}
	if err != nil {
		t.Fatalf("%s: %v", aggs, err)
	}
	return nil
}

// TestOrderedAggsValidation: top_metrics, a terms order and composite refuse what they
// cannot answer, each problem at its loc.
func TestOrderedAggsValidation(t *testing.T) {
	subs := `"aggs":{"st":{"stats":{"field":"price"}},"low":{"min":{"field":"price"}},"tm":{"top_metrics":{"metrics":{"field":"price"},"sort":{"created":"desc"},"size":2}}}`
	terms := func(settings string) string {
		return `{"t":{"terms":{"field":"brand",` + settings + `},` + subs + `}}`
	}
	top := func(settings string) string { return `{"t":{"top_metrics":{` + settings + `}}}` }
	src := func(s string) string { return `{"c":{"composite":{"sources":[` + s + `]}}}` }
	for aggs, loc := range map[string]string{
		// top_metrics
		top(`"metrics":{"field":"price"}`):                                                "aggs.t.top_metrics.sort",
		top(`"metrics":{"field":"price"},"sort":"brand"`):                                 "aggs.t.top_metrics.sort.brand",
		top(`"metrics":{"field":"price"},"sort":"_id"`):                                   "aggs.t.top_metrics.sort._id",
		top(`"metrics":{"field":"price"},"sort":"_v.price"`):                              "aggs.t.top_metrics.sort._v.price",
		top(`"metrics":{"field":"price"},"sort":["price","created"]`):                     "aggs.t.top_metrics.sort",
		top(`"metrics":{"field":"price"},"sort":{"price":{"order":"desc","mode":"max"}}`): "aggs.t.top_metrics.sort.price",
		top(`"metrics":{"field":"price"},"sort":"price","size":11`):                       "aggs.t.top_metrics.size",
		top(`"metrics":{"field":"price"},"sort":"price","size":0`):                        "aggs.t.top_metrics.size",
		top(`"sort":"price"`):                                                                                       "aggs.t.top_metrics.metrics",
		top(`"metrics":{"field":"tags"},"sort":"price"`):                                                            "aggs.t.top_metrics.metrics.0",
		top(`"metrics":{"field":"spot"},"sort":"price"`):                                                            "aggs.t.top_metrics.metrics.0",
		top(`"metrics":{"field":"_v.size"},"sort":"price"`):                                                         "aggs.t.top_metrics.metrics.0",
		top(`"metrics":[{"field":"price"},{"field":"price"}],"sort":"price"`):                                       "aggs.t.top_metrics.metrics.1",
		top(`"metrics":[{"name":"price"}],"sort":"price"`):                                                          "aggs.t.top_metrics.metrics.0",
		top(`"field":"price","metrics":{"field":"price"},"sort":"price"`):                                           "aggs.t.top_metrics.field",
		top(`"metrics":[` + strings.Repeat(`{"field":"x"},`, 10) + `{"field":"y"}],"sort":"price"`):                 "aggs.t.top_metrics.metrics",
		`{"t":{"top_metrics":{"metrics":{"field":"price"},"sort":"price"},"aggs":{"m":{"min":{"field":"price"}}}}}`: "aggs.t.aggs",
		// terms order
		terms(`"order":{"nope":"asc"}`):                       "aggs.t.terms.order.0",
		terms(`"order":{"st":"asc"}`):                         "aggs.t.terms.order.0",
		terms(`"order":{"st.median":"asc"}`):                  "aggs.t.terms.order.0",
		terms(`"order":{"low.avg":"asc"}`):                    "aggs.t.terms.order.0",
		terms(`"order":{"tm.price":"asc"}`):                   "aggs.t.terms.order.0",
		terms(`"order":[{"_key":"asc"},{"_count":"desc"}]`):   "aggs.t.terms.order.1",
		terms(`"order":[{"_count":"asc"},{"_count":"desc"}]`): "aggs.t.terms.order.1",
		terms(`"order":{"_count":"up"}`):                      "aggs.t.terms.order._count",
		terms(`"order":{"_count":"asc","_key":"asc"}`):        "aggs.t.terms.order",
		terms(`"order":"_count"`):                             "aggs.t.terms.order",
		terms(`"order":[{"_count":"asc"},{"st.min":"asc"},{"st.max":"asc"},{"st.avg":"asc"},{"st.sum":"asc"},{"st.count":"asc"},{"low":"asc"},{"tm.price":"asc"},{"_key":"desc"}]`): "aggs.t.terms.order",
		terms(`"collect_mode":"breadth_first","order":{"low":"asc"}`):               "aggs.t.terms.collect_mode",
		`{"t":{"histogram":{"field":"price","interval":5,"order":{"_key":"asc"}}}}`: "aggs.t.histogram.order",
		// composite
		`{"t":{"terms":{"field":"brand"},"aggs":{"c":{"composite":{"sources":[{"a":{"terms":{"field":"brand"}}}]}}}}}`: "aggs.t.aggs.c",
		`{"f":{"filter":{"all":[]},"aggs":{"c":{"composite":{"sources":[{"a":{"terms":{"field":"brand"}}}]}}}}}`:       "aggs.f.aggs.c",
		`{"c":{"composite":{}}}`: "aggs.c.composite.sources",
		`{"c":{"composite":{"size":10001,"sources":[{"a":{"terms":{"field":"brand"}}}]}}}`:                                        "aggs.c.composite.size",
		src(strings.Repeat(`{"a":{"terms":{"field":"brand"}}},`, 10) + `{"k":{"terms":{"field":"brand"}}}`):                       "aggs.c.composite.sources",
		src(`{"a":{"terms":{"field":"brand"}}},{"a":{"terms":{"field":"tags"}}}`):                                                 "aggs.c.composite.sources.1",
		src(`{"a":{"terms":{"field":"spot"}}}`):                                                                                   "aggs.c.composite.sources.0.a.terms.field",
		src(`{"a":{"terms":{"field":"_v.size"}}}`):                                                                                "aggs.c.composite.sources.0.a.terms.field",
		src(`{"a":{"histogram":{"field":"brand","interval":5}}}`):                                                                 "aggs.c.composite.sources.0.a.histogram.field",
		src(`{"a":{"histogram":{"field":"price"}}}`):                                                                              "aggs.c.composite.sources.0.a.histogram.interval",
		src(`{"a":{"date_histogram":{"field":"created","calendar_interval":"day","fixed_interval":"1h"}}}`):                       "aggs.c.composite.sources.0.a.date_histogram",
		src(`{"a":{"date_histogram":{"field":"created","calendar_interval":"fortnight"}}}`):                                       "aggs.c.composite.sources.0.a.date_histogram.calendar_interval",
		src(`{"a":{"range":{"field":"price"}}}`):                                                                                  "aggs.c.composite.sources.0.a.range",
		src(`{"a":{"histogram":{"field":"price","interval":5,"min_doc_count":0}}}`):                                               "aggs.c.composite.sources.0.a.histogram.min_doc_count",
		src(`{"a":{"terms":{"field":"brand","missing_bucket":true}}}`):                                                            "aggs.c.composite.sources.0.a.terms.missing_bucket",
		src(`{"a":{"terms":{"field":"brand","order":"up"}}}`):                                                                     "aggs.c.composite.sources.0.a.terms.order",
		src(`{"a":{"terms":{"field":"brand"}},"b":{"terms":{"field":"brand"}}}`):                                                  "aggs.c.composite.sources.0",
		`{"c":{"composite":{"sources":[{"a":{"terms":{"field":"brand"}}},{"b":{"terms":{"field":"price"}}}],"after":{"a":"x"}}}}`: "aggs.c.composite.after.b",
		`{"c":{"composite":{"sources":[{"a":{"terms":{"field":"brand"}}}],"after":{"a":3}}}}`:                                     "aggs.c.composite.after.a",
		`{"c":{"composite":{"sources":[{"a":{"terms":{"field":"active"}}}],"after":{"a":"true"}}}}`:                               "aggs.c.composite.after.a",
		`{"c":{"composite":{"sources":[{"a":{"terms":{"field":"brand"}}}],"after":{"a":"x","zz":1}}}}`:                            "aggs.c.composite.after.zz",
		`{"c":{"composite":{"sources":[{"a":{"terms":{"field":"brand"}}}]},"aggs":{"t":{"terms":{"field":"brand"}}}}}`:            "aggs.c.aggs.t",
		`{"c":{"composite":{"sources":[{"a":{"terms":{"field":"brand"}}}],"sort":"x"}}}`:                                          "aggs.c.composite.sort",
	} {
		ps := aggProblems(t, aggs)
		if !slices.ContainsFunc(ps, func(p query.Problem) bool { return p.Loc == loc }) {
			t.Errorf("%s: problems %v, want one at %s", aggs, ps, loc)
		}
	}
	for _, aggs := range []string{
		top(`"metrics":[{"field":"price"},{"field":"brand"},{"field":"_id"},{"field":"active"},{"field":"title"},{"field":"unmapped"}],"sort":{"created":{"order":"desc"}},"size":10`),
		terms(`"order":[{"st.avg":"asc"},{"low.value":"asc"},{"_count":"asc"},{"_key":"desc"}]`),
		`{"t":{"terms":{"field":"brand","order":{"tm.price":"desc"}},"aggs":{"tm":{"top_metrics":{"metrics":{"field":"price"},"sort":"created"}}}}}`,
		`{"t":{"terms":{"field":"_v.size","order":{"m":"desc"}},"aggs":{"m":{"min":{"field":"_v.price"}},"tm":{"top_metrics":{"metrics":{"field":"price"},"sort":"created"}}}}}`,
		`{"c":{"composite":{"size":10000,"sources":[{"a":{"terms":{"field":"brand","order":"desc"}}},{"b":{"terms":{"field":"tags"}}},{"p":{"histogram":{"field":"price","interval":2.5,"offset":1}}},` +
			`{"d":{"date_histogram":{"field":"created","fixed_interval":"1d","offset":"-1h"}}},{"i":{"terms":{"field":"_id"}}},{"x":{"terms":{"field":"active"}}}],` +
			`"after":{"a":"x","b":"y","p":1,"d":0,"i":"d1","x":false}},"aggs":{"tm":{"top_metrics":{"metrics":{"field":"price"},"sort":"created"}},"n":{"cardinality":{"field":"tags"}}}}}`,
	} {
		if ps := aggProblems(t, aggs); len(ps) > 0 {
			t.Errorf("%s: %v", aggs, ps)
		}
	}
}
