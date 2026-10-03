package shard

import (
	"container/list"
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// FilterCache is an LRU, bounded by bytes, of bitmaps computed for a query leaf over
// one segment (spec section 6). Segments never change, so an entry never goes stale:
// it is keyed by the segment's ID (unique for the shard's life) and a leaf key the
// searcher chooses (query.Canonical of the leaf, say), and dropped when its segment is
// closed for good. Deletes are not part of an entry: they change between generations,
// so a searcher applies the generation's deletes to what it gets.
//
// Entries are owned copies, never views of a segment's mapping, so they may outlive
// any generation. A bitmap Get returns is shared with every other caller: read it,
// never modify it.
//
// The cache is safe for concurrent use: it is split into shards by key hash, each
// with its own lock, so lookups on different keys rarely contend.
type FilterCache struct {
	shards [filterCacheShards]filterCacheShard
	seed   maphash.Seed

	hits, misses atomic.Int64

	lookups       metric.Int64Counter
	hitAttrs      metric.AddOption
	missAttrs     metric.AddOption
	shardMaxBytes int64
}

const filterCacheShards = 16

type filterCacheShard struct {
	mu    sync.Mutex
	lru   list.List // of *filterEntry, most recent first
	items map[filterKey]*list.Element
	bySeg map[string]map[string]*list.Element
	bytes int64
}

type filterKey struct{ seg, key string }

type filterEntry struct {
	key   filterKey
	bm    *roaring.Bitmap
	bytes int64
}

// NewFilterCache returns a cache holding up to maxBytes of bitmaps (at least 1 MiB)
// that counts its hits and misses on meter's lookups counter (nil: not counted).
func NewFilterCache(maxBytes int64, meter metric.Meter) *FilterCache {
	maxBytes = max(maxBytes, 1<<20)
	c := &FilterCache{seed: maphash.MakeSeed(), shardMaxBytes: maxBytes / filterCacheShards}
	for i := range c.shards {
		c.shards[i].items = map[filterKey]*list.Element{}
		c.shards[i].bySeg = map[string]map[string]*list.Element{}
	}
	if meter != nil {
		in := telemetry.NewInstruments(meter)
		c.lookups = in.Counter(telemetry.MetricSearchFilterCacheLooks)
		c.hitAttrs = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "hit")))
		c.missAttrs = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "miss")))
	}
	return c
}

func (c *FilterCache) shard(k filterKey) *filterCacheShard {
	var h maphash.Hash
	h.SetSeed(c.seed)
	h.WriteString(k.seg)
	h.WriteByte(0)
	h.WriteString(k.key)
	return &c.shards[h.Sum64()%filterCacheShards]
}

// Get returns segmentID's bitmap for key, counting a hit or a miss.
func (c *FilterCache) Get(segmentID, key string) (*roaring.Bitmap, bool) {
	k := filterKey{segmentID, key}
	sh := c.shard(k)
	sh.mu.Lock()
	el, ok := sh.items[k]
	var bm *roaring.Bitmap
	if ok {
		sh.lru.MoveToFront(el)
		bm = el.Value.(*filterEntry).bm //nolint:errcheck // the list only ever holds *filterEntry
	}
	sh.mu.Unlock()
	c.count(ok)
	return bm, ok
}

func (c *FilterCache) count(hit bool) {
	if hit {
		c.hits.Add(1)
		if c.lookups != nil {
			c.lookups.Add(context.Background(), 1, c.hitAttrs)
		}
		return
	}
	c.misses.Add(1)
	if c.lookups != nil {
		c.lookups.Add(context.Background(), 1, c.missAttrs)
	}
}

// Put stores a private copy of bm as segmentID's bitmap for key (replacing any), and
// returns that copy. A bitmap larger than an eighth of the cache's share is not kept
// (the copy is still returned).
func (c *FilterCache) Put(segmentID, key string, bm *roaring.Bitmap) *roaring.Bitmap {
	owned := bm.Clone()
	owned.RunOptimize()
	size := int64(owned.GetSizeInBytes()) + int64(len(segmentID)+len(key)) + 96 //nolint:gosec // a bitmap's size is far below 2^63
	if size > c.shardMaxBytes/8 {
		return owned
	}
	k := filterKey{segmentID, key}
	sh := c.shard(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if el, ok := sh.items[k]; ok {
		sh.unlink(el)
	}
	el := sh.lru.PushFront(&filterEntry{key: k, bm: owned, bytes: size})
	sh.items[k] = el
	keys := sh.bySeg[segmentID]
	if keys == nil {
		keys = map[string]*list.Element{}
		sh.bySeg[segmentID] = keys
	}
	keys[key] = el
	sh.bytes += size
	for sh.bytes > c.shardMaxBytes {
		sh.unlink(sh.lru.Back())
	}
	return owned
}

// GetOrCompute returns segmentID's cached bitmap for key, or computes, caches and
// returns it. compute may return a view over the segment: the cache keeps (and
// returns) a copy.
func (c *FilterCache) GetOrCompute(segmentID, key string, compute func() *roaring.Bitmap) *roaring.Bitmap {
	if bm, ok := c.Get(segmentID, key); ok {
		return bm
	}
	return c.Put(segmentID, key, compute())
}

// DropSegment removes every entry of segmentID; the shard calls it when the segment is
// closed for good.
func (c *FilterCache) DropSegment(segmentID string) {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		for _, el := range sh.bySeg[segmentID] {
			sh.unlink(el)
		}
		sh.mu.Unlock()
	}
}

// unlink removes el; the caller holds sh.mu.
func (sh *filterCacheShard) unlink(el *list.Element) {
	e := el.Value.(*filterEntry) //nolint:errcheck // the list only ever holds *filterEntry
	sh.lru.Remove(el)
	delete(sh.items, e.key)
	if keys := sh.bySeg[e.key.seg]; keys != nil {
		delete(keys, e.key.key)
		if len(keys) == 0 {
			delete(sh.bySeg, e.key.seg)
		}
	}
	sh.bytes -= e.bytes
}

// FilterCacheStats are a cache's counters.
type FilterCacheStats struct {
	Hits, Misses int64
	Entries      int
	Bytes        int64
}

// Stats returns the cache's counters.
func (c *FilterCache) Stats() FilterCacheStats {
	st := FilterCacheStats{Hits: c.hits.Load(), Misses: c.misses.Load()}
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		st.Entries += len(sh.items)
		st.Bytes += sh.bytes
		sh.mu.Unlock()
	}
	return st
}
