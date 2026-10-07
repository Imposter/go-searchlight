package store

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// TestNetStateDisjoint checks, over random batches drawn from a few keys,
// the precondition every dialect's Write relies on (Postgres' single write
// statement above all): netState sends each key of a table at most once
// across its upserts and deletes, and what it sends is the last change to
// each key.
func TestNetStateDisjoint(t *testing.T) {
	kinds := []Kind{KindUpsert, KindDelete, KindQueryUpsert, KindQueryDelete}
	for seed := range uint64(2000) {
		r := rand.New(rand.NewPCG(seed, 7))
		n := 1 + r.IntN(40)
		batch := make([]Change, n)
		p := &prepared{payload: make([]storedBody, n), query: make([]QueryPayload, n)}
		for i := range batch {
			c := Change{Index: fmt.Sprint("i", r.IntN(2)), Shard: r.IntN(3), Kind: kinds[r.IntN(len(kinds))], ID: fmt.Sprint("k", r.IntN(4))}
			batch[i] = c
			p.payload[i] = storedBody{plain: fmt.Sprintf(`{"n":%d}`, i)}
			p.query[i] = QueryPayload{Query: json.RawMessage(fmt.Sprintf(`{"q":%d}`, i)), Meta: json.RawMessage(`{}`)}
		}
		const first = 100
		w := &dialect.Write{}
		netState(w, batch, p, first)

		// The model: the last change to each key, by table.
		type key struct {
			index, id string
			shard     int
		}
		last := map[bool]map[key]int{false: {}, true: {}}
		for i, c := range batch {
			last[c.Kind.isQuery()][key{c.Index, c.ID, c.Shard}] = i
		}
		sent := map[bool]map[key]string{false: {}, true: {}} // key -> "upsert" or "delete"
		note := func(query bool, k key, what string, seq int64) {
			if prev, dup := sent[query][k]; dup {
				t.Fatalf("seed %d: %v sent twice (%s and %s) in %v", seed, k, prev, what, batch)
			}
			sent[query][k] = what
			i, ok := last[query][k]
			if !ok {
				t.Fatalf("seed %d: %v sent but never changed", seed, k)
			}
			want := "delete"
			if batch[i].Kind.isUpsert() {
				want = "upsert"
			}
			if what != want || (seq != 0 && seq != first+int64(i)) {
				t.Fatalf("seed %d: %v sent as %s seq %d, but its last change is %s at %d", seed, k, what, seq, batch[i].Kind, i)
			}
		}
		for _, d := range w.Documents {
			note(false, key{d.Index, d.ID, d.Shard}, "upsert", d.Seq)
		}
		for _, q := range w.Queries {
			note(true, key{q.Index, q.ID, q.Shard}, "upsert", q.Seq)
		}
		for _, g := range w.DocumentDeletes {
			for _, id := range g.IDs {
				note(false, key{g.Index, id, g.Shard}, "delete", 0)
			}
		}
		for _, g := range w.QueryDeletes {
			for _, id := range g.IDs {
				note(true, key{g.Index, id, g.Shard}, "delete", 0)
			}
		}
		for query, keys := range last {
			if len(sent[query]) != len(keys) {
				t.Fatalf("seed %d: %d keys sent of %d changed", seed, len(sent[query]), len(keys))
			}
		}
	}
}
