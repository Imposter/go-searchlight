package search

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/Imposter/go-searchlight/internal/query"
)

// aggTypes are aggTypeList's types.
func aggTypes() []string {
	return strings.Split(strings.ReplaceAll(aggTypeList, " or ", ", "), ", ")
}

// TestOpenAPIListsEveryAggType: api/openapi.yaml's Aggregation takes every aggregation
// type ParseRequest takes, each as a property and as one of its oneOf branches, and
// names no other.
func TestOpenAPIListsEveryAggType(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]any `yaml:"properties"`
				OneOf      []struct {
					Required []string `yaml:"required"`
				} `yaml:"oneOf"`
			} `yaml:"schemas"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	agg, ok := doc.Components.Schemas["Aggregation"]
	if !ok {
		t.Fatal("api/openapi.yaml has no Aggregation schema")
	}
	var branches []string
	for _, b := range agg.OneOf {
		if len(b.Required) != 1 {
			t.Fatalf("an Aggregation oneOf branch requires %v: one type each", b.Required)
		}
		branches = append(branches, b.Required[0])
	}
	types := aggTypes()
	if len(types) != 15 {
		t.Fatalf("aggTypeList splits into %d types: %v", len(types), types)
	}
	for _, typ := range types {
		if _, ok := agg.Properties[typ]; !ok {
			t.Errorf("Aggregation has no property %s", typ)
		}
		if !slices.Contains(branches, typ) {
			t.Errorf("Aggregation's oneOf has no branch for %s", typ)
		}
	}
	for name := range agg.Properties {
		if name != "aggs" && name != "aggregations" && !slices.Contains(types, name) {
			t.Errorf("Aggregation has property %s, which is no aggregation type", name)
		}
	}
	slices.Sort(branches)
	if !slices.Equal(branches, slices.Sorted(slices.Values(types))) {
		t.Errorf("Aggregation's oneOf branches %v, want %v", branches, types)
	}
}

// TestNestingProblemsNameEveryType: the problem of a filter or filters under another
// accounts for every aggregation type: those filter, filters and global hold, and those
// that are top-level only. So does the problem of sub-aggregations under a metric.
func TestNestingProblemsNameEveryType(t *testing.T) {
	for aggs, loc := range map[string]string{
		`{"f":{"filter":{"all":[]},"aggs":{"g":{"filters":{"filters":{"a":{"all":[]}}}}}}}`:                           "aggs.f.aggs.g",
		`{"f":{"global":{},"aggs":{"g":{"filter":{"all":[]}}}}}`:                                                      "aggs.f.aggs.g",
		`{"f":{"filter":{"all":[]},"aggs":{"s":{"stats":{"field":"price"},"aggs":{"m":{"min":{"field":"price"}}}}}}}`: "aggs.f.aggs.s.aggs",
	} {
		ps := aggProblems(t, aggs)
		i := slices.IndexFunc(ps, func(p query.Problem) bool { return p.Loc == loc })
		if i < 0 {
			t.Errorf("%s: problems %v, want one at %s", aggs, ps, loc)
			continue
		}
		for _, typ := range aggTypes() {
			if !strings.Contains(ps[i].Message, typ) {
				t.Errorf("%s: %q does not name %s", aggs, ps[i].Message, typ)
			}
		}
	}
}
