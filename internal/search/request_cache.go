package search

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
// a canonical encoding of everything in the request that decides the shard's result
// (the queries as [query.Canonical] encodes them, with date math already resolved by
// the coordinator; the aggregations; track_total, from, sort and search_after), never
// by its timeout or its index label, and by what the shard holds
// ([shard.CacheIdentity]), so a refresh that writes, a delete, a merge or a remap
// changes the key and the old entries are never read again. Nothing is ever
// invalidated by hand.
//
// Two layers:
//
//   - The memory cache (L1), in process, keyed by the copy's own identity (the open
//     copy, its generation and its mapping version): an LRU bounded by an estimate of
//     the results' bytes. A copy's entries of an older generation are dropped as soon
//     as an entry of a newer one is stored.
//   - An optional shared store (L2, [reqcache.Store]: Redis), keyed by the
//     copy-independent identity (the index incarnation, shard, mapping version, seq and
//     the mark of the change at it, plus this binary's version and the encoding's
//     format), so every copy of the shard on every node, and a node after a restart,
//     reads what one computed. Its values are the shard result's peer wire form (JSON)
//     compressed with zstd, behind a format byte. Its failures never fail a search:
//     they are logged and counted, and the shard computes.
//
// Concurrent identical searches of one copy compute once: the others wait for the
// first's result, and compute themselves only when it cannot be cached (it timed out
// or failed) or their context ends first.
//
// Never cached: a search with hits (size above 0), a request_cache: false one, a
// result that timed out, and an error. A cached result is shared: its aggregations
// are read by every search it answers, and never changed (Reduce only reads the
// partials it merges).

// requestCacheFormat versions the request key and the shared store's values: a
// change to either raises it, so an upgraded node never reads an old encoding.
const requestCacheFormat = 1

// DefaultRequestCacheBytes is the memory cache's default bound.
const DefaultRequestCacheBytes = 128 << 20

// requestCacheWrites bounds the shared-store writes in flight; past it a result is
// not written (the next miss writes it).
const requestCacheWrites = 16

// RequestCacheOptions configure [NewRequestCache].
type RequestCacheOptions struct {
	// MaxBytes bounds the memory cache by its entries' estimated bytes; 0 keeps no
	// memory cache (the shared store alone, if any).
	MaxBytes int64
	// Store, when set, is the shared store behind the memory cache. The cache closes
	// it in Close.
	Store reqcache.Store
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
	version  string
	log      *slog.Logger

	mu      sync.Mutex
	lru     list.List // of *rcEntry, most recent first
	items   map[string]*list.Element
	copies  map[uint64]*rcCopy
	bytes   int64
	flights map[string]*rcFlight

	hits, misses, evictions             atomic.Int64
	storeHits, storeMisses, storeErrors atomic.Int64

	writeSlots chan struct{}
	writes     sync.WaitGroup
	warns      *warnLimiter

	in  *rcInstruments
	reg metric.Registration
}

// rcEntry is one memory cache entry.
type rcEntry struct {
	key   string
	copy  uint64
	res   *ShardResult
	bytes int64
	// shared is the shared-store key the result was last read or written under
	// ("" for none): a hit under another one (the copy's seq moved on with no change
	// to its contents) writes it there too, for the other copies at that seq.
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
	// computation in flight is a hit).
	Hits, Misses int64
	// Evictions counts memory entries dropped for room.
	Evictions int64
	// Bytes and Entries are the memory cache's size.
	Bytes   int64
	Entries int
	// StoreHits, StoreMisses and StoreErrors count the shared store's lookups and
	// failures (reads, writes and decodes).
	StoreHits, StoreMisses, StoreErrors int64
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
		version:    o.Version,
		log:        o.Logger,
		items:      map[string]*list.Element{},
		copies:     map[uint64]*rcCopy{},
		flights:    map[string]*rcFlight{},
		writeSlots: make(chan struct{}, requestCacheWrites),
		warns:      &warnLimiter{interval: 10 * time.Second, last: map[string]time.Time{}, dropped: map[string]int{}},
	}
	if o.Meter != nil {
		c.in, c.reg = newRCInstruments(o.Meter, c)
	}
	return c
}

// Close waits for the shared-store writes in flight, then closes the store.
func (c *RequestCache) Close() error {
	if c == nil {
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
	c.mu.Lock()
	bytes, entries := c.bytes, c.lru.Len()
	c.mu.Unlock()
	return RequestCacheStats{
		Hits: c.hits.Load(), Misses: c.misses.Load(), Evictions: c.evictions.Load(),
		Bytes: bytes, Entries: entries,
		StoreHits: c.storeHits.Load(), StoreMisses: c.storeMisses.Load(), StoreErrors: c.storeErrors.Load(),
	}
}

// Search is [ExecuteShard] through the cache: a cacheable search (size 0, its
// request cache not turned off) of g is answered from the cache when an identical
// one of the same contents was, and its complete result is cached otherwise. The
// caller holds g as ExecuteShard's does. The result's aggregations may be shared with
// other searches: read them, never change them.
func (c *RequestCache) Search(ctx context.Context, g *shard.Generation, r *Request) (*ShardResult, error) {
	if c == nil || g == nil || r == nil {
		return ExecuteShard(ctx, g, r)
	}
	rk, ok := requestCacheKey(r)
	if !ok {
		return ExecuteShard(ctx, g, r)
	}
	id := g.CacheIdentity()
	key := memoryKey(id, rk)
	var shared string
	if c.store != nil && id.Shareable() {
		shared = c.storeKey(id, rk)
	}
	c.mu.Lock()
	if el, ok := c.items[key]; ok {
		c.lru.MoveToFront(el)
		e := el.Value.(*rcEntry) //nolint:forcetypeassert,errcheck // only entries
		res, reshare := e.res, shared != "" && shared != e.shared
		if reshare {
			e.shared = shared
		}
		c.mu.Unlock()
		c.countMemory(ctx, true)
		if reshare {
			c.storeSet(ctx, shared, res, r.Index)
		}
		return shareResult(res), nil
	}
	if f, ok := c.flights[key]; ok {
		c.mu.Unlock()
		select {
		case <-f.done:
			if f.res != nil {
				c.countMemory(ctx, true)
				return shareResult(f.res), nil
			}
		case <-ctx.Done():
		}
		c.countMemory(ctx, false)
		return ExecuteShard(ctx, g, r)
	}
	f := &rcFlight{done: make(chan struct{})}
	c.flights[key] = f
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.flights, key)
		c.mu.Unlock()
		close(f.done)
	}()
	c.countMemory(ctx, false)

	if shared != "" {
		if res := c.storeGet(ctx, shared, r.Index); res != nil {
			c.put(ctx, key, id, res, shared)
			f.res = res
			return shareResult(res), nil
		}
	}
	res, err := ExecuteShard(ctx, g, r)
	if err != nil || res == nil || res.TimedOut {
		return res, err
	}
	c.put(ctx, key, id, res, shared)
	f.res = res
	if shared != "" {
		c.storeSet(ctx, shared, res, r.Index)
	}
	return shareResult(res), nil
}

// shareResult is a copy of res for one caller: its own fields, res's aggregations
// (read-only) and hits.
func shareResult(res *ShardResult) *ShardResult {
	cp := *res
	return &cp
}

// put stores res as key's entry, unless the memory cache is off, res is past an
// eighth of it, or id's view is older than one the cache already holds of its copy.
// An entry of a newer view drops the copy's older ones.
func (c *RequestCache) put(ctx context.Context, key string, id shard.CacheIdentity, res *ShardResult, shared string) {
	if c.maxBytes <= 0 {
		return
	}
	size := int64(len(key)) + resultBytes(res) + 128
	if size > c.maxBytes/8 {
		return
	}
	view := rcView{gen: id.Gen, mapping: id.MappingVersion}
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := c.copies[id.Copy]
	if cp != nil && view.before(cp.view) {
		return
	}
	if cp != nil && cp.view != view {
		for el := range cp.els {
			c.unlink(el)
		}
		cp = nil
	}
	if cp == nil {
		cp = &rcCopy{view: view, els: map[*list.Element]struct{}{}}
		c.copies[id.Copy] = cp
	}
	if el, ok := c.items[key]; ok {
		c.unlink(el)
		if c.copies[id.Copy] == nil {
			c.copies[id.Copy] = cp
		}
	}
	el := c.lru.PushFront(&rcEntry{key: key, copy: id.Copy, res: res, bytes: size, shared: shared})
	c.items[key] = el
	cp.els[el] = struct{}{}
	c.bytes += size
	for c.bytes > c.maxBytes && c.lru.Len() > 1 {
		c.unlink(c.lru.Back())
		c.evictions.Add(1)
		if c.in != nil {
			c.in.evictions.Add(ctx, 1)
		}
	}
}

// unlink drops el; the caller holds c.mu.
func (c *RequestCache) unlink(el *list.Element) {
	e := el.Value.(*rcEntry) //nolint:forcetypeassert,errcheck // only entries
	c.lru.Remove(el)
	delete(c.items, e.key)
	c.bytes -= e.bytes
	if cp := c.copies[e.copy]; cp != nil {
		delete(cp.els, el)
		if len(cp.els) == 0 {
			delete(c.copies, e.copy)
		}
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
		c.in.lookup(ctx, true, hit)
	}
}

// storeKey is id and the request's key in the shared store: readable up to the
// shard, then a hash of everything that decides the result.
func (c *RequestCache) storeKey(id shard.CacheIdentity, rk []byte) string {
	h := sha256.New()
	var b []byte
	for _, s := range []string{c.version, id.Index, id.IndexUID, string(id.SeqMark)} {
		b = appendKeyBytes(b, []byte(s))
	}
	b = binary.AppendVarint(b, int64(id.Shard))
	b = binary.AppendVarint(b, id.MappingVersion)
	b = binary.AppendVarint(b, id.Seq)
	h.Write(b)
	h.Write(rk)
	return fmt.Sprintf("%d:%s:%d:%x", requestCacheFormat, id.Index, id.Shard, h.Sum(nil))
}

// storeGet reads key from the shared store: nil on a miss and on any failure, which
// is counted and logged.
func (c *RequestCache) storeGet(ctx context.Context, key, index string) *ShardResult {
	if ctx.Err() != nil {
		return nil
	}
	raw, err := c.store.Get(ctx, key)
	if err != nil {
		c.storeFailed(ctx, "get", index, err)
		return nil
	}
	if raw == nil {
		c.storeMisses.Add(1)
		if c.in != nil {
			c.in.lookup(ctx, false, false)
		}
		return nil
	}
	res, err := decodeCachedResult(raw)
	if err != nil {
		c.storeFailed(ctx, "decode", index, err)
		return nil
	}
	c.storeHits.Add(1)
	if c.in != nil {
		c.in.lookup(ctx, false, true)
	}
	return res
}

// storeSet writes res under key in the background, unless requestCacheWrites writes
// are in flight already.
func (c *RequestCache) storeSet(ctx context.Context, key string, res *ShardResult, index string) {
	ctx = context.WithoutCancel(ctx)
	select {
	case c.writeSlots <- struct{}{}:
	default:
		return
	}
	c.writes.Add(1)
	go func() {
		defer func() {
			<-c.writeSlots
			c.writes.Done()
		}()
		raw, err := encodeCachedResult(res)
		if err != nil {
			c.storeFailed(ctx, "encode", index, err)
			return
		}
		if int64(len(raw)) > c.store.MaxEntryBytes() {
			return
		}
		if err := c.store.Set(ctx, key, raw); err != nil {
			c.storeFailed(ctx, "set", index, err)
			return
		}
		if c.in != nil {
			c.in.stored.Add(ctx, int64(len(raw)), c.in.storeAttrs)
		}
	}()
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
// whether r may be cached at all (size 0, request_cache not false). The timeout, the
// index label, the bodies flag and the fields are left out: none changes a size-0
// result.
func requestCacheKey(r *Request) ([]byte, bool) {
	if r.Size != 0 || r.NoRequestCache {
		return nil, false
	}
	b := []byte{requestCacheFormat}
	b = appendKeyBytes(b, query.Canonical(r.Query))
	b = appendOptionalNode(b, r.PostFilter)
	b = binary.AppendVarint(b, int64(r.TrackTotal))
	b = binary.AppendVarint(b, int64(r.From))
	order, err := json.Marshal([]any{r.Sort, r.SearchAfter})
	if err != nil {
		return nil, false
	}
	b = appendKeyBytes(b, order)
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

// appendAggsKey appends aggs by name: each one's settings, its filters' canonical
// queries and its sub-aggregations.
func appendAggsKey(b []byte, aggs map[string]Agg) ([]byte, bool) {
	b = binary.AppendUvarint(b, uint64(len(aggs)))
	for _, name := range slices.Sorted(maps.Keys(aggs)) {
		a := aggs[name]
		b = appendKeyBytes(b, []byte(name))
		flat := a
		flat.Filter, flat.Filters, flat.Aggs = nil, nil, nil
		settings, err := json.Marshal(flat)
		if err != nil {
			return nil, false
		}
		b = appendKeyBytes(b, settings)
		b = appendOptionalNode(b, a.Filter)
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
		if p == nil {
			continue
		}
		n += int64(len(name)) + 112
		for _, b := range p.Buckets {
			n += 72 + partialsBytes(b.Aggs)
			if s, ok := b.Key.(string); ok {
				n += int64(len(s))
			}
		}
		if p.Stats != nil {
			n += 32
		}
		if p.Sketch != nil {
			n += 64 + 8*int64(len(p.Sketch.Hashes)) + int64(len(p.Sketch.Registers))
		}
	}
	return n
}

// The shared store's values: a format byte, then the result's peer wire form (JSON)
// compressed with zstd.
var (
	rcEncoder = sync.OnceValue(func() *zstd.Encoder {
		e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
		if err != nil {
			panic(err)
		}
		return e
	})
	rcDecoder = sync.OnceValue(func() *zstd.Decoder {
		d, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(1<<30), zstd.WithDecoderConcurrency(0))
		if err != nil {
			panic(err)
		}
		return d
	})
)

func encodeCachedResult(res *ShardResult) ([]byte, error) {
	js, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	return rcEncoder().EncodeAll(js, []byte{requestCacheFormat}), nil
}

func decodeCachedResult(raw []byte) (*ShardResult, error) {
	if len(raw) == 0 || raw[0] != requestCacheFormat {
		return nil, errors.New("search: a cached result of another format")
	}
	js, err := rcDecoder().DecodeAll(raw[1:], nil)
	if err != nil {
		return nil, err
	}
	var res ShardResult
	if err := json.Unmarshal(js, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// rcInstruments are the request cache's metrics.
type rcInstruments struct {
	lookups, errors, evictions, stored metric.Int64Counter
	memoryHit, memoryMiss              metric.AddOption
	storeHit, storeMiss                metric.AddOption
	storeAttrs                         metric.AddOption
}

func newRCInstruments(m metric.Meter, c *RequestCache) (*rcInstruments, metric.Registration) {
	in := telemetry.NewInstruments(m)
	storeName := ""
	if c.store != nil {
		storeName = c.store.Name()
	}
	attrs := func(store, result string) metric.AddOption {
		return metric.WithAttributeSet(attribute.NewSet(attribute.String("store", store), attribute.String("result", result)))
	}
	ri := &rcInstruments{
		lookups:    in.Counter(telemetry.MetricSearchRequestCacheLookups),
		errors:     in.Counter(telemetry.MetricSearchRequestCacheErrors),
		evictions:  in.Counter(telemetry.MetricSearchRequestCacheEvictions),
		stored:     in.Counter(telemetry.MetricSearchRequestCacheStored),
		memoryHit:  attrs("memory", "hit"),
		memoryMiss: attrs("memory", "miss"),
		storeHit:   attrs(storeName, "hit"),
		storeMiss:  attrs(storeName, "miss"),
		storeAttrs: metric.WithAttributeSet(attribute.NewSet(attribute.String("store", storeName))),
	}
	size := in.ObservableGauge(telemetry.MetricSearchRequestCacheSize)
	entries := in.ObservableGauge(telemetry.MetricSearchRequestCacheEntries)
	reg, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		c.mu.Lock()
		bytes, n := c.bytes, c.lru.Len()
		c.mu.Unlock()
		o.ObserveFloat64(size, float64(bytes))
		o.ObserveFloat64(entries, float64(n))
		return nil
	}, size, entries)
	if err == nil {
		err = in.Err()
	}
	if err != nil {
		c.log.Error("request cache metrics unavailable", slog.Any("error", err))
	}
	return ri, reg
}

func (in *rcInstruments) lookup(ctx context.Context, memory, hit bool) {
	opt := in.storeMiss
	switch {
	case memory && hit:
		opt = in.memoryHit
	case memory:
		opt = in.memoryMiss
	case hit:
		opt = in.storeHit
	}
	in.lookups.Add(ctx, 1, opt)
}

func (in *rcInstruments) failed(ctx context.Context, store, op string) {
	in.errors.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("store", store), attribute.String("operation", op))))
}

// String renders the stats for logs and test failures.
func (s RequestCacheStats) String() string {
	return "hits=" + strconv.FormatInt(s.Hits, 10) + " misses=" + strconv.FormatInt(s.Misses, 10) +
		" evictions=" + strconv.FormatInt(s.Evictions, 10) + " bytes=" + strconv.FormatInt(s.Bytes, 10) +
		" entries=" + strconv.Itoa(s.Entries) + " store_hits=" + strconv.FormatInt(s.StoreHits, 10) +
		" store_misses=" + strconv.FormatInt(s.StoreMisses, 10) + " store_errors=" + strconv.FormatInt(s.StoreErrors, 10)
}
