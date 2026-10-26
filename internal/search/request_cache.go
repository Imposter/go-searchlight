package search

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/reqcache"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// The shard request cache.
//
// Elasticsearch's shard request cache: the result a shard returns for a search with
// size 0 (a count, or aggregations only), before the coordinator reduces it, is kept
// and handed to the next identical search of the same shard contents. It is keyed by
// a canonical encoding of everything in the request that decides a size-0 shard
// result (the queries as [query.Canonical] encodes them, with date math already
// resolved by the coordinator; the aggregations; track_total), never by its timeout
// or its index label, and by what the shard holds ([shard.CacheIdentity]), so a
// refresh that writes, a delete, a merge or a remap changes the key and the old
// entries are never read again. Nothing is ever invalidated by hand.
//
// Two layers:
//
//   - The memory cache (L1), in process, keyed by the copy's own identity (the open
//     copy, its generation and its mapping version): rcParts LRUs, each under its own
//     lock and bounded by its part of an estimate of the results' bytes, an entry's
//     shard chosen by its copy and request (so every generation's entry of one search
//     of one copy lives in one shard). A copy's entries of an older generation in a
//     shard are dropped as soon as an entry of a newer one is stored there.
//   - An optional shared store (L2, [reqcache.Store]: Redis), keyed by the
//     copy-independent identity (the index incarnation, shard, mapping version and the
//     mark of the shard's newest change the copy holds, as
//     RequestCacheOptions.ContentMark reads it, plus this binary's version and the
//     encoding's format), so every copy of the shard on every node, and a node after
//     a restart, reads what one computed, however other shards and indexes write.
//     Its values are the shard result's peer wire form (JSON) compressed with zstd,
//     behind a format byte. Its failures never fail a search: they are logged and
//     counted, and the shard computes.
//
// The store is trusted as the database is: anyone who can write to it can forge the
// results searches return. What is read from it is bounded and checked (size, shape,
// precision against the request) so a corrupt value is a miss, never a panic.
//
// A hit is the result an identical search computed over the same documents: the same
// counts and buckets, though a float sum may differ in its last bits from one summed
// in another segment order, and Segments and Scanned are the computing copy's.
//
// Concurrent identical searches of one copy compute once: the others wait for the
// first's result, within their own timeout, and compute themselves when it cannot be
// cached (it timed out or failed) or their time runs out first. The time a search
// spends waiting or reading the store counts against its timeout, and a store read
// takes at most a tenth of what is left of it.
//
// Bounded staleness: a search that gives Request.MaxStaleness may be answered from the
// copy's newest entry for the same search under the same mapping version although the
// copy has refreshed since (an older generation's), when that entry's result was
// computed no longer than MaxStaleness ago. Under continuous writes every refresh makes
// a new generation, so without it a count or facet search rarely hits; with it, each
// one computes at most about once per MaxStaleness. The result then misses the writes
// of the last MaxStaleness at most (on top of the refresh interval every search
// misses). It is the memory cache's alone: the shared store is keyed by content.
//
// Never cached: a search with hits (size above 0, or a from or search_after), a
// request_cache: false one, one whose date math reads now unrounded (it resolves
// differently every millisecond: [ResolveQueries] turns its cache off), a result
// that timed out, and an error. A cached result is shared: its aggregations are read
// by every search it answers, and never changed (Reduce only reads the partials it
// merges).

// requestCacheFormat versions the request key and the shared store's values: a
// change to either raises it, so an upgraded node never reads an old encoding.
const requestCacheFormat = 4

// requestCacheWrites bounds the shared-store writes in flight; past it a result is
// not written (the next miss writes it).
const requestCacheWrites = 16

// decodedPerStored bounds a stored value's decoded size, as a multiple of the store's
// largest entry: a result whose wire form is larger is not stored.
const decodedPerStored = 8

// storeLookupShare and storeLookupFloor bound a store read by the search's time: at
// most a tenth of what is left of it, and none when that is under 2 ms.
const (
	storeLookupShare = 10
	storeLookupFloor = 2 * time.Millisecond
)

// unsizedDecodeLimit bounds the decoded size of a stored value whose frame does not
// say it (the encoder leaves it out of the smallest ones).
const unsizedDecodeLimit = 64 << 10

// RequestCacheOptions configure [NewRequestCache].
type RequestCacheOptions struct {
	// MaxBytes bounds the memory cache by its entries' estimated bytes; 0 keeps no
	// memory cache (the shared store alone, if any).
	MaxBytes int64
	// Store, when set, is the shared store behind the memory cache. The cache closes
	// it in Close.
	Store reqcache.Store
	// ContentMark names what a copy holds from its own mark as of a generation of
	// seq (replica.OwnContentMark), or nil when that is not known: a generation it
	// names nothing for uses the memory cache alone. Nil uses no store.
	ContentMark func(own json.RawMessage, seq int64) json.RawMessage
	// Version is this binary's version: shared-store entries are keyed by it, so
	// nodes of different releases never read each other's results.
	Version string
	// Meter counts the cache's lookups, errors and evictions and observes its size;
	// nil counts nothing.
	Meter metric.Meter
	// Logger logs the shared store's failures (rate-limited); nil is slog.Default.
	Logger *slog.Logger
}

// RequestCache is the shard request cache (see the package's request_cache.go). A nil
// *RequestCache caches nothing: its Search is [ExecuteShard]. It is safe for
// concurrent use.
type RequestCache struct {
	maxBytes int64
	store    reqcache.Store
	content  func(own json.RawMessage, seq int64) json.RawMessage
	version  string
	log      *slog.Logger

	shards [rcParts]rcPart

	closeMu sync.Mutex
	closed  bool // no more store writes start

	hits, staleHits, misses, evictions              atomic.Int64
	storeHits, storeMisses, storeErrors, storeSkips atomic.Int64
	dropped                                         atomic.Int64

	writeSlots chan struct{}
	writes     sync.WaitGroup
	warns      *warnLimiter

	in  *rcInstruments
	reg metric.Registration
}

// rcParts is how many parts (shards) the memory cache is cut into, each under its own lock:
// concurrent searches of other copies or requests rarely wait for each other.
const rcParts = 8

// rcPart is one part of the memory cache, bounded by its share of the bytes.
type rcPart struct {
	mu      sync.Mutex
	lru     list.List // of *rcEntry, most recent first
	items   map[string]*list.Element
	copies  map[uint64]*rcCopy
	latest  map[rcLatest]*list.Element // each search's newest entry, for MaxStaleness
	bytes   int64
	flights map[string]*rcFlight
}

// rcLatest names one search of one copy whatever the generation: the copy and the
// request's key.
type rcLatest struct {
	copy uint64
	rk   string
}

// rcEntry is one memory cache entry.
type rcEntry struct {
	key   string
	copy  uint64
	res   *ShardResult
	bytes int64
	// latest is the search the entry answers, mapping its view's mapping version, and
	// at when its computation began (a bound on how old its result is).
	latest  rcLatest
	mapping int64
	at      time.Time
	// shared is the shared-store key the result was last read or written under
	// ("" for none): a hit under another one (the copy's contents became nameable
	// after it was stored) writes it there too.
	shared string
}

// rcView is what a copy's generation shows: its Gen and mapping version, both of
// which only grow.
type rcView struct {
	gen     uint64
	mapping int64
}

func (v rcView) before(o rcView) bool {
	return v.gen < o.gen || (v.gen == o.gen && v.mapping < o.mapping)
}

// rcCopy is a copy's entries, all of its newest view the cache has stored.
type rcCopy struct {
	view rcView
	els  map[*list.Element]struct{}
}

// rcFlight is one search computing a result others wait for: res is set, before
// done closes, when it can be cached.
type rcFlight struct {
	done chan struct{}
	res  *ShardResult
}

// RequestCacheStats are a request cache's counters, and its memory cache's size.
type RequestCacheStats struct {
	// Hits and Misses count memory cache lookups (a search served by another's
	// computation in flight is a hit); StaleHits the hits of an older generation's
	// result within the search's MaxStaleness, which Hits counts too.
	Hits, StaleHits, Misses int64
	// Evictions counts memory entries dropped for room.
	Evictions int64
	// Bytes and Entries are the memory cache's size.
	Bytes   int64
	Entries int
	// StoreHits, StoreMisses and StoreErrors count the shared store's lookups and
	// failures (reads, writes, decodes and values refused); StoreSkips the lookups
	// not made while the store was failing (its breaker open).
	StoreHits, StoreMisses, StoreErrors, StoreSkips int64
	// DroppedWrites counts results not written to the store: too many writes in
	// flight, the store's breaker open, or the cache closing.
	DroppedWrites int64
}

// NewRequestCache returns a request cache, or nil (caching nothing) when o has
// neither a memory bound nor a store.
func NewRequestCache(o RequestCacheOptions) *RequestCache {
	if o.MaxBytes <= 0 && o.Store == nil {
		return nil
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	c := &RequestCache{
		maxBytes:   max(o.MaxBytes, 0),
		store:      o.Store,
		content:    o.ContentMark,
		version:    o.Version,
		log:        o.Logger,
		writeSlots: make(chan struct{}, requestCacheWrites),
		warns:      &warnLimiter{interval: 10 * time.Second, last: map[string]time.Time{}, dropped: map[string]int{}},
	}
	for i := range c.shards {
		sh := &c.shards[i]
		sh.items, sh.copies, sh.latest, sh.flights = map[string]*list.Element{}, map[uint64]*rcCopy{}, map[rcLatest]*list.Element{}, map[string]*rcFlight{}
	}
	if o.Meter != nil {
		c.in, c.reg = newRCInstruments(o.Meter, c)
	}
	return c
}

// Close stops new store writes, waits for those in flight, unregisters the metrics
// and closes the store. Searches after it still compute (uncached by the store).
func (c *RequestCache) Close() error {
	if c == nil {
		return nil
	}
	c.closeMu.Lock()
	already := c.closed
	c.closed = true
	c.closeMu.Unlock()
	if already {
		return nil
	}
	c.writes.Wait()
	if c.reg != nil {
		_ = c.reg.Unregister()
	}
	if c.store != nil {
		return c.store.Close()
	}
	return nil
}

// Stats returns the cache's counters.
func (c *RequestCache) Stats() RequestCacheStats {
	if c == nil {
		return RequestCacheStats{}
	}
	bytes, entries := c.size()
	return RequestCacheStats{
		Hits: c.hits.Load(), StaleHits: c.staleHits.Load(), Misses: c.misses.Load(), Evictions: c.evictions.Load(),
		Bytes: bytes, Entries: entries,
		StoreHits: c.storeHits.Load(), StoreMisses: c.storeMisses.Load(), StoreErrors: c.storeErrors.Load(),
		StoreSkips: c.storeSkips.Load(), DroppedWrites: c.dropped.Load(),
	}
}

// size is the memory cache's bytes and entries, every shard's together.
func (c *RequestCache) size() (bytes int64, entries int) {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		bytes += sh.bytes
		entries += sh.lru.Len()
		sh.mu.Unlock()
	}
	return bytes, entries
}

// shardFor is the memory cache shard of search rk of copy cp.
func (c *RequestCache) shardFor(cp uint64, rk []byte) *rcPart {
	h := fnv.New32a()
	_, _ = h.Write(binary.AppendUvarint(nil, cp))
	_, _ = h.Write(rk)
	return &c.shards[h.Sum32()%rcParts]
}

// Search is [ExecuteShard] through the cache: a cacheable search (size 0, its
// request cache not turned off) of g is answered from the cache when an identical
// one of the same contents was, or, with r.MaxStaleness, of the same copy and mapping
// at most that long ago; its complete result is cached otherwise. The caller holds g
// as ExecuteShard's does. The time spent waiting for another search's result or
// reading the store counts against r.Timeout. The result's aggregations may be shared
// with other searches: read them, never change them.
func (c *RequestCache) Search(ctx context.Context, g *shard.Generation, r *Request) (*ShardResult, error) {
	if c == nil || g == nil || r == nil {
		return ExecuteShard(ctx, g, r)
	}
	rk, ok := requestCacheKey(r)
	if !ok {
		return ExecuteShard(ctx, g, r)
	}
	start := time.Now()
	budget := ctx
	if r.Timeout > 0 {
		var cancel context.CancelFunc
		budget, cancel = context.WithTimeout(ctx, r.Timeout)
		defer cancel()
	}
	id := g.CacheIdentity()
	key := memoryKey(id, rk)
	shared := c.sharedKey(id, rk)
	c.closeMu.Lock()
	if c.closed {
		shared = ""
	}
	c.closeMu.Unlock()
	sh := c.shardFor(id.Copy, rk)
	sh.mu.Lock()
	if el, ok := sh.items[key]; ok {
		sh.lru.MoveToFront(el)
		e := el.Value.(*rcEntry) //nolint:forcetypeassert,errcheck // only entries
		res, reshare := e.res, shared != "" && shared != e.shared
		if reshare {
			e.shared = shared
		}
		sh.mu.Unlock()
		c.countMemory(ctx, true)
		if reshare {
			c.storeSet(ctx, shared, res, r.Index)
		}
		return shareResult(res), nil
	}
	if r.MaxStaleness > 0 {
		if el, ok := sh.latest[rcLatest{copy: id.Copy, rk: string(rk)}]; ok {
			e := el.Value.(*rcEntry) //nolint:forcetypeassert,errcheck // only entries
			if e.mapping == id.MappingVersion && time.Since(e.at) <= r.MaxStaleness {
				sh.lru.MoveToFront(el)
				res := e.res
				sh.mu.Unlock()
				c.countStale(ctx)
				return shareResult(res), nil
			}
		}
	}
	if f, ok := sh.flights[key]; ok {
		sh.mu.Unlock()
		select {
		case <-f.done:
			if f.res != nil {
				c.countMemory(ctx, true)
				return shareResult(f.res), nil
			}
		case <-budget.Done():
		}
		c.countMemory(ctx, false)
		return executeWithin(ctx, g, r, start)
	}
	f := &rcFlight{done: make(chan struct{})}
	sh.flights[key] = f
	sh.mu.Unlock()
	defer func() {
		sh.mu.Lock()
		delete(sh.flights, key)
		sh.mu.Unlock()
		close(f.done)
	}()
	c.countMemory(ctx, false)

	entry := &rcEntry{key: key, copy: id.Copy, latest: rcLatest{copy: id.Copy, rk: string(rk)}, mapping: id.MappingVersion, at: start}
	if shared != "" {
		if res := c.storeGet(budget, shared, r); res != nil {
			entry.res, entry.shared = res, shared
			c.put(ctx, sh, id, entry)
			f.res = res
			return shareResult(res), nil
		}
	}
	res, err := executeWithin(ctx, g, r, start)
	if err != nil || res == nil || res.TimedOut {
		return res, err
	}
	entry.res, entry.shared = res, shared
	c.put(ctx, sh, id, entry)
	f.res = res
	if shared != "" {
		c.storeSet(ctx, shared, res, r.Index)
	}
	return shareResult(res), nil
}

// executeWithin runs r on g with what is left of r.Timeout since start: a search out
// of time times out at once, as one that ran out of it would.
func executeWithin(ctx context.Context, g *shard.Generation, r *Request, start time.Time) (*ShardResult, error) {
	if r.Timeout > 0 {
		rest := max(r.Timeout-time.Since(start), time.Nanosecond)
		cp := *r
		cp.Timeout = rest
		r = &cp
	}
	return ExecuteShard(ctx, g, r)
}

// shareResult is a copy of res for one caller: its own fields, res's aggregations
// (read-only) and hits.
func shareResult(res *ShardResult) *ShardResult {
	cp := *res
	return &cp
}

// put stores e in sh, unless the memory cache is off, e is past half of sh's share of
// it, or id's view is older than one sh already holds of its copy. An entry of a newer
// view drops the copy's older ones in sh.
func (c *RequestCache) put(ctx context.Context, sh *rcPart, id shard.CacheIdentity, e *rcEntry) {
	if c.maxBytes <= 0 {
		return
	}
	budget := c.maxBytes / rcParts
	e.bytes = int64(len(e.key)) + resultBytes(e.res) + 128
	if e.bytes > budget/2 {
		return
	}
	view := rcView{gen: id.Gen, mapping: id.MappingVersion}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	cp := sh.copies[id.Copy]
	if cp != nil && view.before(cp.view) {
		return
	}
	if cp != nil && cp.view != view {
		for el := range cp.els {
			sh.unlink(el)
		}
		cp = nil
	}
	if el, ok := sh.items[e.key]; ok {
		sh.unlink(el)
	}
	if cp == nil || sh.copies[id.Copy] == nil {
		if cp == nil {
			cp = &rcCopy{view: view, els: map[*list.Element]struct{}{}}
		}
		sh.copies[id.Copy] = cp
	}
	el := sh.lru.PushFront(e)
	sh.items[e.key] = el
	sh.latest[e.latest] = el
	cp.els[el] = struct{}{}
	sh.bytes += e.bytes
	for sh.bytes > budget && sh.lru.Len() > 1 {
		sh.unlink(sh.lru.Back())
		c.evictions.Add(1)
		if c.in != nil {
			c.in.evictions.Add(ctx, 1)
		}
	}
}

// unlink drops el; the caller holds sh.mu.
func (sh *rcPart) unlink(el *list.Element) {
	e := el.Value.(*rcEntry) //nolint:forcetypeassert,errcheck // only entries
	sh.lru.Remove(el)
	delete(sh.items, e.key)
	if sh.latest[e.latest] == el {
		delete(sh.latest, e.latest)
	}
	sh.bytes -= e.bytes
	if cp := sh.copies[e.copy]; cp != nil {
		delete(cp.els, el)
		if len(cp.els) == 0 {
			delete(sh.copies, e.copy)
		}
	}
}

// countStale counts a hit of an older generation's result within MaxStaleness.
func (c *RequestCache) countStale(ctx context.Context) {
	c.hits.Add(1)
	c.staleHits.Add(1)
	if c.in != nil {
		c.in.lookupResult(ctx, "memory", "stale_hit")
	}
}

func (c *RequestCache) countMemory(ctx context.Context, hit bool) {
	if c.maxBytes <= 0 {
		return
	}
	if hit {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}
	if c.in != nil {
		c.in.lookup(ctx, "memory", hit)
	}
}

// sharedKey is id and the request's key in the shared store, "" when there is no
// store or id's contents cannot be named apart from the copy: readable up to the
// shard, then a hash of everything that decides the result.
func (c *RequestCache) sharedKey(id shard.CacheIdentity, rk []byte) string {
	if c.store == nil || c.content == nil || id.IndexUID == "" || id.MappingVersion <= 0 {
		return ""
	}
	content := c.content(id.OwnMark, id.Seq)
	if len(content) == 0 {
		return ""
	}
	var b []byte
	for _, s := range []string{c.version, id.Index, id.IndexUID, string(content)} {
		b = appendKeyBytes(b, []byte(s))
	}
	b = binary.AppendVarint(b, int64(id.Shard))
	b = binary.AppendVarint(b, id.MappingVersion)
	h := sha256.New()
	h.Write(b)
	h.Write(rk)
	return fmt.Sprintf("%d:%s:%d:%x", requestCacheFormat, id.Index, id.Shard, h.Sum(nil))
}

// storeGet reads key from the shared store for r: nil on a miss and on any failure,
// which is counted and logged.
//
// The read gets at most storeLookupShare of what is left of the search's time
// (ctx's deadline), and is skipped when that is below storeLookupFloor: a hung store
// never takes a search's whole budget. A read that runs out of that share while the
// search still has time is the store's failure (counted, and seen by its breaker);
// one the search gave up on is nobody's.
func (c *RequestCache) storeGet(ctx context.Context, key string, r *Request) *ShardResult {
	if ctx.Err() != nil {
		return nil
	}
	lookup := ctx
	if dl, ok := ctx.Deadline(); ok {
		share := time.Until(dl) / storeLookupShare
		if share < storeLookupFloor {
			c.skipStore(ctx)
			return nil
		}
		var cancel context.CancelFunc
		lookup, cancel = context.WithTimeout(ctx, share)
		defer cancel()
	}
	raw, err := c.store.Get(lookup, key)
	switch {
	case errors.Is(err, reqcache.ErrOpen):
		c.skipStore(ctx)
		return nil
	case err != nil && ctx.Err() != nil:
		// The search's own time ran out, or it was cancelled: nothing to say of the
		// store.
		return nil
	case err != nil:
		c.storeFailed(ctx, "get", r.Index, err)
		return nil
	case raw == nil:
		c.storeMisses.Add(1)
		if c.in != nil {
			c.in.lookup(ctx, c.store.Name(), false)
		}
		return nil
	}
	res, err := decodeCachedResult(raw, c.store.MaxEntryBytes(), r)
	if err != nil {
		c.storeFailed(ctx, "decode", r.Index, err)
		return nil
	}
	c.storeHits.Add(1)
	if c.in != nil {
		c.in.lookup(ctx, c.store.Name(), true)
	}
	return res
}

func (c *RequestCache) skipStore(ctx context.Context) {
	c.storeSkips.Add(1)
	if c.in != nil {
		c.in.lookupResult(ctx, c.store.Name(), "skipped")
	}
}

// storeSet writes res under key in the background, unless the cache is closing or
// requestCacheWrites writes are in flight already.
func (c *RequestCache) storeSet(ctx context.Context, key string, res *ShardResult, index string) {
	ctx = context.WithoutCancel(ctx)
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		c.drop(ctx, "closed")
		return
	}
	select {
	case c.writeSlots <- struct{}{}:
	default:
		c.closeMu.Unlock()
		c.drop(ctx, "busy")
		return
	}
	c.writes.Add(1)
	c.closeMu.Unlock()
	go func() {
		defer func() {
			<-c.writeSlots
			c.writes.Done()
		}()
		raw, err := encodeCachedResult(res, c.store.MaxEntryBytes())
		if errors.Is(err, reqcache.ErrTooLarge) {
			return
		}
		if err != nil {
			c.storeFailed(ctx, "encode", index, err)
			return
		}
		switch err := c.store.Set(ctx, key, raw); {
		case errors.Is(err, reqcache.ErrOpen):
			c.drop(ctx, "store_failing")
		case errors.Is(err, reqcache.ErrTooLarge):
		case err != nil:
			c.storeFailed(ctx, "set", index, err)
		case c.in != nil:
			c.in.stored.Add(ctx, int64(len(raw)), c.in.storeAttrs)
		}
	}()
}

func (c *RequestCache) drop(ctx context.Context, reason string) {
	c.dropped.Add(1)
	if c.in != nil {
		c.in.dropped.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("reason", reason))))
	}
}

func (c *RequestCache) storeFailed(ctx context.Context, op, index string, err error) {
	c.storeErrors.Add(1)
	if c.in != nil {
		c.in.failed(ctx, c.store.Name(), op)
	}
	if suppressed, ok := c.warns.allow(op); ok {
		c.log.WarnContext(ctx, "search: the request cache's store failed; the shard computes its result",
			slog.String("store", c.store.Name()), slog.String("operation", op),
			slog.String(telemetry.KeyIndex, index), slog.Int("suppressed", suppressed), slog.Any("error", err))
	}
}

// requestCacheKey is the canonical encoding of what decides r's shard result, and
// whether r may be cached at all: size 0 with no from or search_after (no hits),
// request_cache not off. The timeout, the index label, the sort, the bodies flag and
// the fields are left out: none changes a result without hits.
func requestCacheKey(r *Request) ([]byte, bool) {
	if r.Size != 0 || r.From != 0 || len(r.SearchAfter) > 0 || r.NoRequestCache {
		return nil, false
	}
	b := []byte{requestCacheFormat}
	b = appendKeyBytes(b, query.Canonical(r.Query))
	b = appendOptionalNode(b, r.PostFilter)
	b = binary.AppendVarint(b, int64(r.TrackTotal))
	b, ok := appendAggsKey(b, r.Aggs)
	return b, ok
}

func appendKeyBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func appendOptionalNode(b []byte, n query.Node) []byte {
	if n == nil {
		return append(b, 0)
	}
	return appendKeyBytes(append(b, 1), query.Canonical(n))
}

// appendAggsKey appends aggs by name: each one's settings, its filters' and where's
// canonical queries and its sub-aggregations.
func appendAggsKey(b []byte, aggs map[string]Agg) ([]byte, bool) {
	b = binary.AppendUvarint(b, uint64(len(aggs)))
	for _, name := range slices.Sorted(maps.Keys(aggs)) {
		a := aggs[name]
		b = appendKeyBytes(b, []byte(name))
		flat := a
		flat.Filter, flat.Filters, flat.Aggs, flat.Where = nil, nil, nil, nil
		settings, err := json.Marshal(flat)
		if err != nil {
			return nil, false
		}
		b = appendKeyBytes(b, settings)
		b = appendOptionalNode(b, a.Filter)
		b = appendOptionalNode(b, a.Where)
		b = binary.AppendUvarint(b, uint64(len(a.Filters)))
		for _, key := range slices.Sorted(maps.Keys(a.Filters)) {
			b = appendKeyBytes(b, []byte(key))
			b = appendOptionalNode(b, a.Filters[key])
		}
		var ok bool
		if b, ok = appendAggsKey(b, a.Aggs); !ok {
			return nil, false
		}
	}
	return b, true
}

// memoryKey is the memory cache's key: the copy's own identity and the request's.
func memoryKey(id shard.CacheIdentity, rk []byte) string {
	b := make([]byte, 0, len(rk)+30)
	b = binary.AppendUvarint(b, id.Copy)
	b = binary.AppendUvarint(b, id.Gen)
	b = binary.AppendVarint(b, id.MappingVersion)
	return string(append(b, rk...))
}

// readsNowUnrounded reports whether any of r's queries holds date math that reads
// now with no rounding: it resolves to another instant every millisecond, so caching
// its result would only fill the cache.
func readsNowUnrounded(r *Request) bool {
	found := false
	check := func(n query.Node) {
		query.Walk(n, func(_ string, node query.Node) bool {
			if l, ok := node.(*query.Leaf); ok && l.IsDateMath() && leafReadsNowUnrounded(l) {
				found = true
			}
			return !found
		})
	}
	check(r.Query)
	if r.PostFilter != nil {
		check(r.PostFilter)
	}
	for _, f := range r.Sort {
		if f.Where != nil {
			check(f.Where)
		}
	}
	var aggs func(map[string]Agg)
	aggs = func(m map[string]Agg) {
		for name := range m {
			a := m[name]
			if a.Filter != nil {
				check(a.Filter)
			}
			if a.Where != nil {
				check(a.Where)
			}
			for _, f := range a.Filters {
				check(f)
			}
			aggs(a.Aggs)
		}
	}
	aggs(r.Aggs)
	return found
}

// leafReadsNowUnrounded reports whether one of l's date math values reads now with
// no rounding step; a leaf whose value was never decoded counts as one.
func leafReadsNowUnrounded(l *query.Leaf) bool {
	scalars := l.Arg.List
	switch l.Arg.Kind {
	case query.ArgString:
		scalars = []query.Scalar{l.Arg.Scalar}
	case query.ArgList:
	case query.ArgBox:
		scalars = l.Arg.BoundScalars()
	default:
		return true
	}
	for i := range scalars {
		if scalars[i].Kind != query.ArgString {
			continue
		}
		d, why := query.ParseDateMath(scalars[i].Text)
		if why != "" || !d.Relative() {
			continue
		}
		if !slices.ContainsFunc(d.Steps(), func(s query.DateStep) bool { return s.Round }) {
			return true
		}
	}
	return false
}

// resultBytes estimates the heap res holds.
func resultBytes(res *ShardResult) int64 {
	n := int64(160)
	for i := range res.Hits {
		h := &res.Hits[i]
		n += 96 + int64(len(h.ID)+len(h.Body)) + 16*int64(len(h.Sort))
	}
	return n + partialsBytes(res.Aggs)
}

func partialsBytes(m map[string]*AggPartial) int64 {
	n := int64(48 * len(m))
	for name, p := range m {
		if p != nil {
			n += int64(len(name)) + partialBytes(p)
		}
	}
	return n
}

// partialBytes estimates one partial: its fixed part, buckets with their keys and
// sub-aggregations, stats, sketch, a min or max value, and a thresholded
// cardinality's distinct hashes (up to MaxThreshold, 8 bytes each). A field added
// to AggPartial that holds memory belongs here.
func partialBytes(p *AggPartial) int64 {
	n := int64(112)
	for _, b := range p.Buckets {
		if b == nil {
			continue
		}
		n += 72 + partialsBytes(b.Aggs)
		if s, ok := b.Key.(string); ok {
			n += int64(len(s))
		}
	}
	if p.Stats != nil {
		n += 32
	}
	if s := p.Sketch; s != nil {
		n += 64 + 8*int64(cap(s.Hashes)) + int64(cap(s.Registers))
	}
	if p.Value != nil {
		n += 8
	}
	n += 8 * int64(cap(p.Distinct))
	return n
}

// The shared store's values: a format byte, then the result's peer wire form (JSON)
// compressed with zstd, with its size in the frame header.
var (
	rcEncoder = sync.OnceValue(func() *zstd.Encoder {
		e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
		if err != nil {
			panic(err)
		}
		return e
	})
	rcDecoder = sync.OnceValue(func() *zstd.Decoder {
		d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(1<<30), zstd.WithDecoderConcurrency(0), zstd.WithDecodeAllCapLimit(true))
		if err != nil {
			panic(err)
		}
		return d
	})
)

// encodeCachedResult is res's stored value: reqcache.ErrTooLarge when its wire form
// is past decodedPerStored times maxEntry, which no read would decode.
func encodeCachedResult(res *ShardResult, maxEntry int64) ([]byte, error) {
	js, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	if int64(len(js)) > decodedPerStored*maxEntry {
		return nil, reqcache.ErrTooLarge
	}
	return rcEncoder().EncodeAll(js, []byte{requestCacheFormat}), nil
}

var errBadCachedResult = errors.New("search: a cached result that is not a shard result of its request")

// decodeCachedResult reads a stored value as a result of r: an error, never a panic,
// for a value past maxEntry, of another format, that decodes past decodedPerStored
// times maxEntry, or that is not a complete size-0 result of r's aggregations.
func decodeCachedResult(raw []byte, maxEntry int64, r *Request) (res *ShardResult, err error) {
	defer func() {
		if p := recover(); p != nil {
			res, err = nil, fmt.Errorf("%w: decoding it panicked: %v\n%s", errBadCachedResult, p, debug.Stack())
		}
	}()
	if int64(len(raw)) > maxEntry {
		return nil, reqcache.ErrTooLarge
	}
	if len(raw) == 0 || raw[0] != requestCacheFormat {
		return nil, errors.New("search: a cached result of another format")
	}
	var h zstd.Header
	if err := h.Decode(raw[1:]); err != nil {
		return nil, err
	}
	most := uint64(decodedPerStored * maxEntry) //nolint:gosec // maxEntry is positive
	limit := min(uint64(unsizedDecodeLimit), most)
	if h.HasFCS {
		limit = h.FrameContentSize
	}
	if limit > most {
		return nil, fmt.Errorf("%w: its decoded size is too large", errBadCachedResult)
	}
	// The decoder's cap limit holds the output to dst's capacity.
	js, err := rcDecoder().DecodeAll(raw[1:], make([]byte, 0, limit))
	if err != nil {
		return nil, err
	}
	var out ShardResult
	if err := json.Unmarshal(js, &out); err != nil {
		return nil, err
	}
	if err := validCachedResult(&out, r); err != nil {
		return nil, err
	}
	return &out, nil
}

// validCachedResult checks res is a result of r as a shard computes one: complete,
// with no hits, and each partial of an aggregation r has, of its type, with buckets
// and sketches Reduce can merge.
func validCachedResult(res *ShardResult, r *Request) error {
	if res.TimedOut || len(res.Hits) > 0 || res.Total < 0 || (res.TotalRelation != RelationEq && res.TotalRelation != RelationGte) {
		return errBadCachedResult
	}
	return validPartials(res.Aggs, r.Aggs, 0)
}

func validPartials(ps map[string]*AggPartial, aggs map[string]Agg, depth int) error {
	if depth > 4 && len(ps) > 0 {
		return errBadCachedResult
	}
	for name, p := range ps {
		a, ok := aggs[name]
		if !ok || p == nil || p.Type != a.Type || p.DocCountError < 0 || p.OtherDocCount < 0 {
			return fmt.Errorf("%w: aggregation %q", errBadCachedResult, name)
		}
		if err := validShape(p, &a); err != nil {
			return fmt.Errorf("%w: aggregation %q's %w", errBadCachedResult, name, err)
		}
		if s := p.Sketch; s != nil {
			prec := a.Precision
			if prec == 0 {
				prec = DefaultPrecision
			}
			if a.Type != AggCardinality || int(s.P) != prec || (s.Registers != nil && len(s.Registers) != 1<<s.P) ||
				len(s.Hashes) > s.exactLimit() || !slices.IsSorted(s.Hashes) ||
				slices.ContainsFunc(s.Registers, func(v uint8) bool { return int(v) > 65-prec }) {
				return fmt.Errorf("%w: aggregation %q's sketch", errBadCachedResult, name)
			}
		}
		if st := p.Stats; st != nil && (st.Count < 0 || (st.Count > 0 && st.Min > st.Max)) {
			return fmt.Errorf("%w: aggregation %q's stats", errBadCachedResult, name)
		}
		if a.Type == AggRange && p.Buckets != nil && len(p.Buckets) != len(a.Ranges) {
			return fmt.Errorf("%w: aggregation %q has %d range buckets, the request %d", errBadCachedResult, name, len(p.Buckets), len(a.Ranges))
		}
		keys := make(map[any]struct{}, len(p.Buckets))
		for _, b := range p.Buckets {
			if b == nil || b.DocCount < 0 {
				return fmt.Errorf("%w: aggregation %q's buckets", errBadCachedResult, name)
			}
			switch b.Key.(type) {
			case float64:
			case nil, string, bool:
				if a.Type == AggHistogram || a.Type == AggDateHistogram || a.Type == AggRange {
					return fmt.Errorf("%w: aggregation %q's bucket key is not a number", errBadCachedResult, name)
				}
			default:
				return fmt.Errorf("%w: aggregation %q's bucket key", errBadCachedResult, name)
			}
			if _, dup := keys[b.Key]; dup {
				return fmt.Errorf("%w: aggregation %q holds bucket %v twice", errBadCachedResult, name, b.Key)
			}
			keys[b.Key] = struct{}{}
			if err := validPartials(b.Aggs, a.Aggs, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// validShape checks the members p holds are those of a's type: a min or max holds
// at most its value; a cardinality with a threshold its distinct hashes (sorted,
// unique, at most the threshold) and no sketch, one without a threshold no hashes;
// no other type holds a value or hashes.
func validShape(p *AggPartial, a *Agg) error {
	switch a.Type {
	case AggMin, AggMax:
		if p.Buckets != nil || p.Stats != nil || p.Sketch != nil || p.Distinct != nil {
			return errors.New("min or max shape")
		}
		return nil
	case AggCardinality:
		if p.Value != nil || p.Buckets != nil {
			return errors.New("cardinality shape")
		}
		if a.Threshold == 0 {
			if p.Distinct != nil {
				return errors.New("distinct hashes without a threshold")
			}
			return nil
		}
		if p.Sketch != nil || len(p.Distinct) > a.Threshold {
			return errors.New("distinct hashes past the threshold")
		}
		for i := 1; i < len(p.Distinct); i++ {
			if p.Distinct[i] <= p.Distinct[i-1] {
				return errors.New("distinct hashes not sorted and unique")
			}
		}
		return nil
	}
	if p.Value != nil || p.Distinct != nil {
		return errors.New("value or distinct hashes")
	}
	return nil
}

// rcInstruments are the request cache's metrics.
type rcInstruments struct {
	lookups, errors, evictions, stored, dropped metric.Int64Counter
	storeAttrs                                  metric.AddOption
}

func newRCInstruments(m metric.Meter, c *RequestCache) (*rcInstruments, metric.Registration) {
	in := telemetry.NewInstruments(m)
	storeName := ""
	if c.store != nil {
		storeName = c.store.Name()
	}
	ri := &rcInstruments{
		lookups:    in.Counter(telemetry.MetricSearchRequestCacheLookups),
		errors:     in.Counter(telemetry.MetricSearchRequestCacheErrors),
		evictions:  in.Counter(telemetry.MetricSearchRequestCacheEvictions),
		stored:     in.Counter(telemetry.MetricSearchRequestCacheStored),
		dropped:    in.Counter(telemetry.MetricSearchRequestCacheDropped),
		storeAttrs: metric.WithAttributeSet(attribute.NewSet(attribute.String("store", storeName))),
	}
	size := in.ObservableGauge(telemetry.MetricSearchRequestCacheSize)
	entries := in.ObservableGauge(telemetry.MetricSearchRequestCacheEntries)
	open := in.ObservableGauge(telemetry.MetricSearchRequestCacheStoreOpen)
	breaker, _ := c.store.(interface{ Open() bool })
	openAttrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("store", storeName)))
	reg, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		bytes, n := c.size()
		o.ObserveFloat64(size, float64(bytes))
		o.ObserveFloat64(entries, float64(n))
		if breaker != nil {
			v := 0.0
			if breaker.Open() {
				v = 1
			}
			o.ObserveFloat64(open, v, openAttrs)
		}
		return nil
	}, size, entries, open)
	if err == nil {
		err = in.Err()
	}
	if err != nil {
		c.log.Error("request cache metrics unavailable", slog.Any("error", err))
	}
	return ri, reg
}

func (in *rcInstruments) lookup(ctx context.Context, store string, hit bool) {
	result := "miss"
	if hit {
		result = "hit"
	}
	in.lookupResult(ctx, store, result)
}

func (in *rcInstruments) lookupResult(ctx context.Context, store, result string) {
	in.lookups.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("store", store), attribute.String("result", result))))
}

func (in *rcInstruments) failed(ctx context.Context, store, op string) {
	in.errors.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("store", store), attribute.String("operation", op))))
}

// String renders the stats for logs and test failures.
func (s RequestCacheStats) String() string {
	return "hits=" + strconv.FormatInt(s.Hits, 10) + " stale_hits=" + strconv.FormatInt(s.StaleHits, 10) + " misses=" + strconv.FormatInt(s.Misses, 10) +
		" evictions=" + strconv.FormatInt(s.Evictions, 10) + " bytes=" + strconv.FormatInt(s.Bytes, 10) +
		" entries=" + strconv.Itoa(s.Entries) + " store_hits=" + strconv.FormatInt(s.StoreHits, 10) +
		" store_misses=" + strconv.FormatInt(s.StoreMisses, 10) + " store_errors=" + strconv.FormatInt(s.StoreErrors, 10) +
		" store_skips=" + strconv.FormatInt(s.StoreSkips, 10) + " dropped_writes=" + strconv.FormatInt(s.DroppedWrites, 10)
}
