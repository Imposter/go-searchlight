package query

import (
	"strings"
	"testing"
)

func TestWalkOrderAndLocs(t *testing.T) {
	n := mustParse(t, `{"all": [
		{"field": "a", "op": "eq", "value": 1},
		{"any": [{"field": "b", "op": "eq", "value": 2}, {"not": {"field": "c", "op": "eq", "value": 3}}]}
	]}`)
	var got []string
	Walk(n, func(loc string, _ Node) bool {
		got = append(got, loc)
		return true
	})
	want := []string{
		"query",
		"query.all.0",
		"query.all.1",
		"query.all.1.any.0",
		"query.all.1.any.1",
		"query.all.1.any.1.not",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("Walk order = %v, want %v", got, want)
	}
}

func TestWalkStopsDescending(t *testing.T) {
	n := mustParse(t, `{"all": [{"any": [{"field": "a", "op": "eq", "value": 1}]}, {"field": "b", "op": "eq", "value": 2}]}`)
	var got []string
	Walk(n, func(loc string, _ Node) bool {
		got = append(got, loc)
		return loc != "query.all.0" // skip into the any group, but keep the sibling
	})
	want := []string{"query", "query.all.0", "query.all.1"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("Walk order = %v, want %v", got, want)
	}
}

func TestWalkSkipsNilNodes(t *testing.T) {
	n := &All{Children: []Node{nil, (*Leaf)(nil)}}
	count := 0
	Walk(n, func(_ string, _ Node) bool { count++; return true })
	if count != 1 { // the root only; both nil children are skipped
		t.Fatalf("Walk visited %d nodes, want 1", count)
	}
}
