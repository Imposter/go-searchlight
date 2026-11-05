package node

import "testing"

// TestShardForIsTheDocumentedContract: an id's shard is the 32-bit FNV-1a hash of the
// id's UTF-8 bytes, modulo the index's shard count (0 for one shard), as docs/api.md's
// Routing section promises clients. The values are fixed: a change here moves
// documents between shards and breaks every client that computes them.
func TestShardForIsTheDocumentedContract(t *testing.T) {
	fnv1a := func(s string) uint32 {
		h := uint32(2166136261)
		for _, b := range []byte(s) {
			h ^= uint32(b)
			h *= 16777619
		}
		return h
	}
	for _, c := range []struct {
		id   string
		hash uint32
		of   map[int]int
	}{
		{"a", 3826002220, map[int]int{1: 0, 2: 0, 8: 4, 64: 44}},
		{"doc-1", 1510950043, map[int]int{1: 0, 2: 1, 8: 3, 64: 27}},
		{"sku:12345", 3877085179, map[int]int{1: 0, 2: 1, 8: 3, 64: 59}},
		{"café", 2821410889, map[int]int{1: 0, 2: 1, 8: 1, 64: 9}},
		{"日本", 2670128721, map[int]int{1: 0, 2: 1, 8: 1, 64: 17}},
	} {
		if got := fnv1a(c.id); got != c.hash {
			t.Fatalf("FNV-1a of %q is %d, the table says %d", c.id, got, c.hash)
		}
		for n, want := range c.of {
			if got := ShardFor(c.id, n); got != want {
				t.Errorf("ShardFor(%q, %d) = %d, want %d", c.id, n, got, want)
			}
		}
	}
}
