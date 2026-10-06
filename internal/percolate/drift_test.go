package percolate

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
)

// opSamples is a value each op reads, so a leaf of it compiles to a real condition.
// An op added to query.Ops fails TestEveryOpIsCovered until it has a sample here, a case
// in progCompiler.leaf and the evaluator, and is drawn by the generators below.
var opSamples = map[string]string{
	query.OpEq: `"a"`, query.OpNe: `"a"`, query.OpIn: `["a",1,true]`,
	query.OpLt: `1`, query.OpLte: `1`, query.OpGt: `1`, query.OpGte: `1`, query.OpBetween: `[1,2]`,
	query.OpExists: `true`, query.OpEmpty: ``, query.OpNonempty: ``,
	query.OpContains: `"abc"`, query.OpContainsAny: `["abc"]`, query.OpContainsAll: `["abc"]`,
	query.OpStartsWith: `"ab"`, query.OpWordsAll: `["a b"]`, query.OpWordsAny: `["a b"]`,
	query.OpSimilar: `{"text":"abc","min":0.5}`,
	query.OpHas:     `"a"`, query.OpHasAny: `["a"]`, query.OpHasAll: `["a"]`,
}

// unanchorableOps are the ops the positive generator leaves out: they never anchor.
var unanchorableOps = []string{query.OpNe, query.OpEmpty}

// Every op the query language has is compiled into a program, and drawn by the
// completeness generators (the positive one too, unless it never anchors).
func TestEveryOpIsCovered(t *testing.T) {
	ft := &fieldTable{at: map[string]uint32{}}
	pc := progCompiler{field: ft.field}
	drawn := map[string]int{}
	g := newGen(1)
	for range 20000 {
		raw, err := json.Marshal(g.leaf())
		if err != nil {
			t.Fatal(err)
		}
		var l struct{ Op string }
		_ = json.Unmarshal(raw, &l)
		if _, problems := query.Parse(raw); len(problems) == 0 {
			drawn[l.Op]++
		}
	}
	for _, op := range query.Ops {
		sample, ok := opSamples[op]
		if !ok {
			t.Errorf("op %q: no sample; give it one, a case in progCompiler.leaf and the evaluator, and draw it in gen.leaf", op)
			continue
		}
		leaf := map[string]any{"field": "f", "op": op}
		if sample != "" {
			leaf["value"] = json.RawMessage(sample)
		}
		raw, err := json.Marshal(leaf)
		if err != nil {
			t.Fatal(err)
		}
		n, problems := query.Parse(raw)
		if len(problems) > 0 {
			t.Errorf("op %q: sample %s does not parse: %v", op, raw, problems)
			continue
		}
		if p := pc.compile(n); p[0] == pTrue || p[0] == pFalse {
			t.Errorf("op %q compiles to a constant: progCompiler.leaf does not handle it", op)
		}
		if drawn[op] == 0 {
			t.Errorf("op %q: the completeness generator never draws a valid leaf of it", op)
		}
		if !slices.Contains(positiveOps, op) && !slices.Contains(unanchorableOps, op) {
			t.Errorf("op %q: missing from positiveOps (or, if it never anchors, from unanchorableOps)", op)
		}
	}
}
