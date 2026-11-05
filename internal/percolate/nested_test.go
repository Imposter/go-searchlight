package percolate

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

// Saved nested queries percolate exactly, through refreshes, a reopen and a merge:
// one object of the document must satisfy the whole inner query, its not per object.
func TestPercolateNested(t *testing.T) {
	m := newShardModel(t)
	r := rand.New(rand.NewPCG(13, 13))
	brands := []string{"acme", "zeta", "Acme"}
	for i := range 40 {
		inner := fmt.Sprintf(`{"all":[{"field":"v.brand","op":"eq","value":%q},{"field":"v.price","op":"lt","value":%d}]}`, brands[r.IntN(len(brands))], r.IntN(100))
		switch i % 4 {
		case 1:
			inner = `{"not":` + inner + `}`
		case 2:
			inner = fmt.Sprintf(`{"any":[%s,{"field":"v.stock","op":"eq","value":true}]}`, inner)
		}
		m.putQuery(fmt.Sprintf("n%02d", i), `{"nested":{"path":"v","query":`+inner+`}}`)
		if i == 20 {
			m.refresh()
		}
	}
	m.putQuery("both", `{"all":[{"field":"brand","op":"eq","value":"top"},{"nested":{"path":"v","query":{"field":"v.stock","op":"exists"}}}]}`)
	m.putQuery("none", `{"not":{"nested":{"path":"v","query":{"all":[]}}}}`)
	m.refresh()
	docs := map[string]string{}
	for i := range 40 {
		members := ""
		for k := range r.IntN(4) {
			if k > 0 {
				members += ","
			}
			if r.IntN(6) == 0 {
				members += `"no object"`
				continue
			}
			members += fmt.Sprintf(`{"brand":%q,"price":%d,"stock":%t}`, brands[r.IntN(len(brands))], r.IntN(100), r.IntN(3) == 0)
		}
		docs[fmt.Sprint("d", i)] = `{"brand":"top","v":[` + members + `]}`
	}
	docs["split"] = `{"v":[{"brand":"acme","price":99},{"brand":"zeta","price":1}]}`
	docs["bare"] = `{"brand":"top"}`
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	got := m.percolateAt(docs, now)
	if slices.Contains(got["bare"], "both") || !slices.Contains(got["bare"], "none") {
		t.Fatalf("a document without objects: %v", got["bare"])
	}
	m.reopen()
	m.percolateAt(docs, now)
	m.merge()
	m.percolateAt(docs, now)
}
