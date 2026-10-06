// Package shard is one shard copy (spec section 6): an in-memory write buffer that
// [Shard.Refresh] turns into an immutable segment every refresh_interval, the set of
// segments published to readers as a [Generation] through an atomic pointer, deletes
// and updates kept as per-generation deletes sidecars, background tiered merges under a
// node-wide [MergeBudget], a [FilterCache] of per-segment leaf bitmaps, and a local
// manifest, written by [Shard.Flush], that records which segments and which changelog
// seq are durable on disk.
//
// # Writes
//
// [Shard.Apply] takes changes in changelog order (seq strictly increasing; the global
// changelog leaves gaps, which are fine). Each document or saved query id keeps only
// its newest version: within the buffer an upsert or delete replaces the entry; at
// refresh every id the buffer touched masks every older copy of that id in every
// segment, so a reader never sees two live copies of one id, nor a deleted one, and an
// update becomes visible at the same refresh that hides its old version.
//
// # Reads
//
// [Shard.Acquire] returns the current [Generation] with no lock and no allocation; the
// generation stays valid, with every segment it exposes mapped, until its
// [Generation.Release]. Its zero-copy postings, presence and truncated bitmaps are valid
// exactly that long.
//
// # Durability
//
// A refresh is visibility only: it publishes without an fsync. A flush, every
// flush_interval, at Close, at every merge commit and before a peer snapshot, is
// durability: it fsyncs what the published generation added, writes its deletes
// sidecars, swaps the manifest atomically (temp file, fsync, rename, directory fsync),
// and only then advances [Shard.CommittedSeq] to the manifest's seq (see flush.go).
// After a crash, [Open] reopens exactly the manifest's segments and the caller replays
// the changelog from there. Files the manifest does not reference (a crash's leftovers)
// are removed at Open.
package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// ChangeKind is what a [Change] does.
type ChangeKind uint8

// The change kinds.
const (
	// Upsert creates or replaces the document DocID with Doc.
	Upsert ChangeKind = iota + 1
	// Delete removes the document DocID.
	Delete
	// QueryUpsert creates or replaces the saved query QueryID with Query and Meta.
	QueryUpsert
	// QueryDelete removes the saved query QueryID.
	QueryDelete
	// Remap makes Mapping, at MappingVersion, the index mapping from this change on:
	// the changes after it were analyzed under it. The shard records it with the seq,
	// so a generation, and the manifest, carry the mapping as of the seq they cover.
	Remap
)

var kindNames = [...]string{Upsert: "upsert", Delete: "delete", QueryUpsert: "query_upsert", QueryDelete: "query_delete", Remap: "remap"}

func (k ChangeKind) String() string {
	if k >= Upsert && k <= Remap {
		return kindNames[k]
	}
	return fmt.Sprintf("ChangeKind(%d)", uint8(k))
}

// Change is one changelog entry for this shard, already analyzed.
type Change struct {
	// Seq is the change's position in the global changelog. Apply requires it to
	// increase strictly; gaps (other shards' changes) are expected.
	Seq  int64
	Kind ChangeKind
	// DocID is the document an Upsert or Delete targets. For an Upsert it may be left
	// empty, meaning Doc.ID.
	DocID string
	// Doc is an Upsert's analyzed document ([schema.Analyze]). The shard keeps the
	// pointer: the caller must not change the document after Apply.
	Doc *schema.Doc
	// QueryID is the saved query a QueryUpsert or QueryDelete targets.
	QueryID string
	// Query is a QueryUpsert's parsed query ([query.Parse]); the shard keeps it.
	Query query.Node
	// Meta is a QueryUpsert's opaque meta object, at most [MaxMetaBytes].
	Meta []byte
	// Mapping and MappingVersion are a Remap's mapping and its version (the store's
	// mapping version, increasing with every mapping change of the index).
	Mapping        *schema.Mapping
	MappingVersion int64
	// IndexUID is the incarnation of the index the change belongs to (store.Change's
	// IndexUID), or "" when unknown. The shard adopts the first one it sees, records
	// it in its manifest, and refuses a change for any other with [ErrIndexUID]:
	// wiping a copy whose index was dropped and recreated is the caller's job.
	IndexUID string
}

// id returns the document or query id the change targets.
func (c *Change) id() string {
	switch c.Kind {
	case Upsert, Delete:
		if c.DocID == "" && c.Doc != nil {
			return c.Doc.ID
		}
		return c.DocID
	default:
		return c.QueryID
	}
}

// MaxMetaBytes is the largest saved query meta object (spec section 3).
const MaxMetaBytes = 16 << 10

// Errors.
var (
	// ErrClosed is an operation on a closed (or closing) shard.
	ErrClosed = errors.New("shard: closed")
	// ErrFailed is an operation on a shard that hit a failure it cannot vouch its
	// durable state through (a directory fsync failing after a manifest swap, or a
	// test's simulated crash). Close it and reopen, or recover the copy.
	ErrFailed = errors.New("shard: failed; reopen or recover the copy")
	// ErrDocTooLarge is a document whose id and body exceed segment.MaxStoredBytes. It is
	// segment.ErrDocTooLarge, so errors.Is matches either.
	ErrDocTooLarge = segment.ErrDocTooLarge
	// ErrSeqOrder is a change whose seq is not above every seq the shard has applied.
	ErrSeqOrder = errors.New("shard: change seq does not increase")
	// ErrIndexUID is a change for another incarnation of the index than the shard's.
	ErrIndexUID = errors.New("shard: change belongs to another incarnation of the index")
	// ErrBackpressure is an Apply refused because the write buffer is over its limit
	// (Options.MaxBufferFactor times RefreshBytes) while refreshes catch up. Nothing of
	// the batch was applied; retry it after a short wait (an HTTP API answers 429).
	ErrBackpressure = errors.New("shard: the write buffer is full; retry")
	// ErrInvalidChange is a change that is malformed: an unknown kind, a missing
	// document or query, an id no document may have, an id that disagrees with its
	// document's, or a meta object over MaxMetaBytes.
	ErrInvalidChange = errors.New("shard: invalid change")
)

// ChangeError is one change of an [Shard.Apply] batch that was refused. Apply applies
// none of a batch with a refused change; Pos says which one to drop or fix.
type ChangeError struct {
	// Pos is the change's index in the batch.
	Pos int
	Seq int64
	// ID is the document or query id the change targets.
	ID  string
	Err error
}

func (e *ChangeError) Error() string {
	return fmt.Sprintf("shard: change %d (seq %d, id %q): %v", e.Pos, e.Seq, e.ID, e.Err)
}

func (e *ChangeError) Unwrap() error { return e.Err }

// Options configures a [Shard]. The zero value is usable: every field has a default.
type Options struct {
	// Index and Shard label the shard's logs, spans and metrics.
	Index string
	Shard int
	// RefreshInterval is how often the buffer is refreshed in the background
	// (refresh_interval), on a fixed grid from Open. 0 means one second; a negative value
	// disables background refresh (Refresh still works).
	RefreshInterval time.Duration
	// FlushInterval is how often the published generation is made durable
	// (flush_interval, [Shard.Flush]). 0 means DefaultFlushInterval; a negative value
	// disables background flushes, leaving those at Close, at merge commits, before a
	// Snapshot and on Flush.
	FlushInterval time.Duration
	// RefreshThreads bounds the goroutines a refresh's segment build uses; 0 means
	// GOMAXPROCS. A refresh is on the write-to-visible path, so it is not budgeted.
	RefreshThreads int
	// RefreshBytes triggers an early background refresh when the buffer's estimated
	// size passes it, bounding buffer memory. 0 means 64 MiB; negative disables it.
	RefreshBytes int64
	// MaxBufferFactor bounds the write buffer at this many times RefreshBytes: past it
	// Apply refuses with [ErrBackpressure] until a refresh drains the buffer. 0 means
	// 4; it is off when RefreshBytes is negative. A refresh in progress holds one more
	// buffer, frozen, so memory is bounded at about MaxBufferFactor+1 times RefreshBytes.
	MaxBufferFactor int
	// MergePolicy chooses background merges of document segments. Nil means
	// DefaultTieredPolicy().
	MergePolicy *TieredPolicy
	// QueryMergePolicy chooses background merges of query segments. Nil means
	// DefaultQueryTieredPolicy().
	QueryMergePolicy *TieredPolicy
	// DisableMerges turns background merges off; ForceMerge still merges.
	DisableMerges bool
	// MergeBudget bounds merge CPU and I/O. Share one across every shard on a node
	// (NewMergeBudget(cfg.MergeThreads, cfg.MergeBudget, clk)). Nil means the process-wide
	// [DefaultMergeBudget], so shards opened without one still share a budget.
	MergeBudget *MergeBudget
	// FilterCache caches leaf bitmaps per segment. Share one across every shard on a
	// node, made with the node's meter so its hits and misses are counted. Nil means
	// the process-wide [DefaultFilterCache], which counts them only in Stats.
	FilterCache *FilterCache
	// QueryIndex builds and opens the percolator's query segments. Nil means the
	// built-in [DefaultQueryIndex], which stores queries without anchors.
	QueryIndex QueryIndexBuilder
	// DeleteRetry is how often removing a file that is still in use (Windows) is
	// retried. 0 means five seconds.
	DeleteRetry time.Duration
	// Clock runs the shard's timers (background refresh and flush, the janitor's
	// retries) and times its operations. Nil means clock.Real.
	Clock clock.Clock
	// Logger, Tracer and Meter are the shard's telemetry; nil means slog.Default(), a
	// no-op tracer and no metrics.
	Logger *slog.Logger
	Tracer trace.Tracer
	Meter  metric.Meter

	// hooks are test kill points; nil outside tests.
	hooks *testHooks
}

// Defaults.
const (
	DefaultRefreshInterval  = time.Second
	DefaultFlushInterval    = 10 * time.Second
	DefaultRefreshBytes     = 64 << 20
	DefaultMaxBufferFactor  = 4
	DefaultDeleteRetry      = 5 * time.Second
	DefaultFilterCacheBytes = 64 << 20
)

// The process-wide defaults: every shard opened without its own shares them, so a
// node's shards never each get a private budget or cache by accident.
var (
	defaultMergeBudget = sync.OnceValue(func() *MergeBudget {
		return NewMergeBudget(max(1, runtime.GOMAXPROCS(0)/4), 0, clock.Real{})
	})
	defaultFilterCache = sync.OnceValue(func() *FilterCache {
		return NewFilterCache(DefaultFilterCacheBytes, nil)
	})
)

// DefaultMergeBudget returns the process-wide merge budget shards use when Options
// has none: GOMAXPROCS/4 threads (at least one) and no I/O limit.
func DefaultMergeBudget() *MergeBudget { return defaultMergeBudget() }

// DefaultFilterCache returns the process-wide filter cache shards use when Options has
// none: DefaultFilterCacheBytes, with no metrics.
func DefaultFilterCache() *FilterCache { return defaultFilterCache() }

func (o *Options) resolve() {
	if o.RefreshInterval == 0 {
		o.RefreshInterval = DefaultRefreshInterval
	}
	if o.FlushInterval == 0 {
		o.FlushInterval = DefaultFlushInterval
	}
	if o.RefreshThreads <= 0 {
		o.RefreshThreads = runtime.GOMAXPROCS(0)
	}
	if o.RefreshBytes == 0 {
		o.RefreshBytes = DefaultRefreshBytes
	}
	if o.MaxBufferFactor <= 0 {
		o.MaxBufferFactor = DefaultMaxBufferFactor
	}
	if o.MergePolicy == nil {
		p := DefaultTieredPolicy()
		o.MergePolicy = &p
	}
	if o.QueryMergePolicy == nil {
		p := DefaultQueryTieredPolicy()
		o.QueryMergePolicy = &p
	}
	if o.MergeBudget == nil {
		o.MergeBudget = DefaultMergeBudget()
	}
	if o.QueryIndex == nil {
		o.QueryIndex = DefaultQueryIndex{}
	}
	if o.DeleteRetry <= 0 {
		o.DeleteRetry = DefaultDeleteRetry
	}
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Tracer == nil {
		o.Tracer = tracenoop.NewTracerProvider().Tracer(telemetry.ScopeName)
	}
	if o.FilterCache == nil {
		o.FilterCache = DefaultFilterCache()
	}
}

// Shard is one shard copy. Its methods are safe for concurrent use. Readers
// ([Shard.Acquire]) never take a lock; writers (Apply) take a short buffer lock;
// refreshes and merges publish under a commit lock that readers never see; flushes run
// one at a time, taking the commit lock only briefly.
type Shard struct {
	dir  string
	opts Options
	log  *slog.Logger
	tr   trace.Tracer
	inst *instruments
	// spanAttrs label every span: the index and shard.
	spanAttrs []attribute.KeyValue

	// mapping is mapState's mapping, for lock-free reads.
	mapping atomic.Pointer[schema.Mapping]

	// cur is the published generation; the shard holds one reference on it.
	cur atomic.Pointer[Generation]

	// mu guards the write buffer and what has been applied to it.
	mu       sync.Mutex
	buf      *buffer
	bufBytes atomic.Int64 // buf.bytes, for Admit without the lock
	// marksUntyped is MarksUntyped: fixed at Open.
	marksUntyped bool
	applied      int64 // every change with seq <= applied is in a segment or buf
	maxChange    int64 // the newest change applied
	indexUID     string
	mapState     *mappingState // the mapping as of applied
	closing      bool

	// failed is set once the shard cannot vouch for its durable state.
	failed atomic.Pointer[error]

	// refreshMu makes refreshes one at a time.
	refreshMu sync.Mutex
	// commitMu serializes refresh and merge publishes: the generation counter, the
	// segment set, the segments published since the last flush and the merging set.
	commitMu sync.Mutex
	gen      uint64
	merging  map[*segRef]bool
	inflight int
	// unflushed are the segments published since the last flush took its generation.
	unflushed []*segRef
	// forceEligible is the running ForceMerge's segments (nil when none runs);
	// forceSem, a one-slot semaphore, runs ForceMerges one at a time.
	forceEligible map[*segRef]bool
	forceSem      chan struct{}
	mergeDone     chan struct{} // closed and replaced when a merge finishes

	// flushSem, a one-slot semaphore, makes flushes one at a time, and guards the
	// durable state below: the manifest's generation, index uid and mapping, the
	// segments and sidecars it lists (with the sidecars' sizes), and the sidecars
	// flushes wrote that it does not list.
	flushSem        chan struct{}
	committed       atomic.Int64
	durableGen      uint64
	committedUID    string
	committedMap    *mappingState
	durableSegs     map[*segRef]bool
	durableSidecars map[string]int64
	strays          map[string]bool
	// manifestBytes and sidecarBytes are the manifest's and its sidecars' sizes, for
	// the disk-size metric.
	manifestBytes atomic.Int64
	sidecarBytes  atomic.Int64

	waitMu sync.Mutex
	waitCh chan struct{} // closed and replaced at every publish

	bg          context.Context //nolint:containedctx // the background goroutines' lifetime, cancelled by Close
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	refreshWake chan struct{}
	mergeWake   chan struct{}
	closed      chan struct{}
	closeOnce   sync.Once
	// shutdownDone is closed when shutdown has finished: a second Close waits on it.
	shutdownDone chan struct{}
	jan          *janitor
}

// Open opens the shard copy in dir, creating dir if needed: it reads the manifest,
// reopens and verifies every segment it lists with its deletes, removes every file the
// manifest does not reference, and starts the background refresh and merges. A
// directory with no manifest is an empty shard. m is the index's mapping (see
// [Shard.SetMapping]); it may be nil.
func Open(ctx context.Context, dir string, m *schema.Mapping, opts Options) (*Shard, error) {
	opts.resolve()
	ctx, span := opts.Tracer.Start(ctx, "shard.open", trace.WithAttributes(
		attribute.String(telemetry.KeyIndex, opts.Index), attribute.Int(telemetry.KeyShard, opts.Shard)))
	defer span.End()

	s, err := open(ctx, dir, m, opts)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "open failed")
		return nil, err
	}
	return s, nil
}

func open(ctx context.Context, dir string, m *schema.Mapping, opts Options) (*Shard, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("shard: %w", err)
	}
	man, err := readManifest(dir, opts.Logger)
	if err != nil {
		return nil, err
	}
	s := &Shard{
		dir:         dir,
		opts:        opts,
		log:         opts.Logger.With(telemetry.KeyIndex, opts.Index, telemetry.KeyShard, opts.Shard),
		tr:          opts.Tracer,
		buf:         newBuffer(),
		merging:     map[*segRef]bool{},
		mergeDone:   make(chan struct{}),
		forceSem:    make(chan struct{}, 1),
		flushSem:    make(chan struct{}, 1),
		waitCh:      make(chan struct{}),
		refreshWake: make(chan struct{}, 1),
		mergeWake:   make(chan struct{}, 1),
		closed:      make(chan struct{}),

		shutdownDone: make(chan struct{}),
	}
	s.inst = newInstruments(opts.Meter, opts.Index, opts.Shard, s.log)
	s.inst.observeResident(opts.Meter, s)
	s.spanAttrs = []attribute.KeyValue{attribute.String(telemetry.KeyIndex, opts.Index), attribute.Int(telemetry.KeyShard, opts.Shard)}
	s.jan = newJanitor(s)
	mp, err := man.mappingState(dir, m)
	if err != nil {
		return nil, err
	}
	s.mapState, s.committedMap = mp, mp
	if mp.m != nil {
		s.mapping.Store(mp.m)
	}

	g, err := s.openGeneration(man, mp)
	if err != nil {
		return nil, err
	}
	if err := s.collectGarbage(ctx, man); err != nil {
		g.retireAll()
		s.jan.drain()
		return nil, err
	}
	// Never reuse a generation a crash left a sidecar of (I1): its file may still be
	// pending removal, or be removed later by a janitor that could not remove it now.
	topGen, err := maxSidecarGen(dir)
	if err != nil {
		g.retireAll()
		s.jan.drain()
		return nil, err
	}
	s.gen = max(man.Gen, topGen)
	s.durableGen = man.Gen
	s.durableSegs = map[*segRef]bool{}
	s.durableSidecars = map[string]int64{}
	s.strays = map[string]bool{}
	var sidecarBytes int64
	for _, list := range [][]segState{g.docs, g.queries} {
		for i := range list {
			st := &list[i]
			st.ref.synced = true
			s.durableSegs[st.ref] = true
			if st.delGen > 0 {
				path := filepath.Join(dir, deletesName(st.ref.id, st.delGen))
				var size int64
				if info, err := os.Stat(path); err == nil {
					size = info.Size()
				}
				s.durableSidecars[path] = size
				sidecarBytes += size
			}
		}
	}
	s.sidecarBytes.Store(sidecarBytes)
	// A shard with no segment yet has nothing unmarked: everything it builds is.
	s.marksUntyped = man.UntypedMarks == untypedMarksFormat || len(man.Segments) == 0
	s.applied = man.Seq
	s.maxChange = man.MaxSeq
	s.indexUID = man.IndexUID
	s.committedUID = man.IndexUID
	s.committed.Store(man.Seq)
	if info, err := os.Stat(filepath.Join(dir, manifestName)); err == nil {
		s.manifestBytes.Store(info.Size())
	}
	s.cur.Store(g)
	s.recordGeneration(ctx, g)

	s.bg, s.cancel = context.WithCancel(context.WithoutCancel(ctx))
	s.jan.start()
	s.wg.Add(2)
	go s.refreshLoop()
	go s.mergeLoop()
	if opts.FlushInterval > 0 {
		s.wg.Add(1)
		go s.flushLoop()
	}
	s.wakeMerges()
	s.log.InfoContext(ctx, "shard opened",
		slog.String("dir", dir), slog.Int64("seq", man.Seq), slog.Int("segments", len(g.Segments)),
		slog.Int("query_segments", len(g.QuerySegments)), slog.Uint64("documents", g.NumDocs()))
	return s, nil
}

// startSpan starts a span labelled with the shard's index and shard, and attrs.
func (s *Shard) startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	all := make([]attribute.KeyValue, 0, len(s.spanAttrs)+len(attrs))
	all = append(append(all, s.spanAttrs...), attrs...)
	return s.tr.Start(ctx, name, trace.WithAttributes(all...))
}

// Dir returns the shard's directory.
func (s *Shard) Dir() string { return s.dir }

// IndexUID returns the incarnation of the index the shard holds: the manifest's after
// Open, or the first one a change carried. "" when none has been seen.
func (s *Shard) IndexUID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.indexUID
}

// Mapping returns the index mapping as of the applied seq: the last Remap applied,
// the manifest's at Open, or the one Open or SetMapping was given.
func (s *Shard) Mapping() *schema.Mapping { return s.mapping.Load() }

// MarksUntyped reports whether every segment of the shard was committed by a shard
// that records untyped-value marks: in the segments of a shard that does, a field the
// mapping does not map has a Truncated bit exactly for the documents whose value of it
// would analyze as a typed value (its writer, the replica, sets GramsTruncated on
// them). A shard opened from a manifest written before the marks existed reports
// false until it is rebuilt: its older segments lack them.
func (s *Shard) MarksUntyped() bool { return s.marksUntyped }

// MappingVersion returns the version of Mapping: the last Remap's, the manifest's at
// Open, and 0 for a mapping only Open or SetMapping gave.
func (s *Shard) MappingVersion() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mapState.version
}

// SetMapping replaces the index mapping, keeping its version; the next refresh
// publishes it, and the flush after persists it. A copy that tails the changelog takes
// its mappings from Remap changes instead, so the mapping is recorded with the seq it
// holds from.
func (s *Shard) SetMapping(m *schema.Mapping) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mapState = &mappingState{m: m, version: s.mapState.version}
	s.mapping.Store(m)
}

// CommittedSeq returns the seq of the manifest that is durable on disk: every change
// with a seq at or below it is in the flushed segments. After a crash, replay the
// changelog from here. It trails RefreshedSeq until the next flush
// (Options.FlushInterval): a refresh publishes without making anything durable.
func (s *Shard) CommittedSeq() int64 { return s.committed.Load() }

// AppliedSeq returns the highest seq applied (or advanced to), refreshed or not.
func (s *Shard) AppliedSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

// RefreshedSeq returns the seq the current generation covers: every change with a
// seq at or below it is searchable.
func (s *Shard) RefreshedSeq() int64 {
	if g := s.cur.Load(); g != nil {
		return g.seq
	}
	return s.committed.Load()
}

// Err returns the failure that stopped the shard, or nil.
func (s *Shard) Err() error {
	if p := s.failed.Load(); p != nil {
		return *p
	}
	return nil
}

func (s *Shard) fail(err error) {
	wrapped := fmt.Errorf("%w: %w", ErrFailed, err)
	if s.failed.CompareAndSwap(nil, &wrapped) {
		s.notifyPublished() // a failed shard never refreshes: wake WaitRefreshed
	}
}

// usable returns the error any write-side operation fails with now, or nil.
func (s *Shard) usable() error {
	if err := s.Err(); err != nil {
		return err
	}
	select {
	case <-s.closed:
		return ErrClosed
	default:
		return nil
	}
}

// Apply adds changes to the write buffer: they are searchable after the next refresh.
// Every change is checked first, and if any is refused none is applied: the error is a
// *[ChangeError] naming the first refused change's position, wrapping [ErrDocTooLarge]
// (a document over segment.MaxStoredBytes, refused here so it can never fail a
// refresh), [ErrSeqOrder], [ErrIndexUID] or [ErrInvalidChange]. A buffer over its
// limit refuses the whole batch with [ErrBackpressure] (retryable).
func (s *Shard) Apply(ctx context.Context, changes []Change) error {
	ctx, span := s.startSpan(ctx, "shard.apply", attribute.Int("changes", len(changes)))
	defer span.End()
	buffered, err := s.apply(changes, false)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "apply refused")
		return err
	}
	s.inst.countChanges(ctx, changes)
	s.inst.recordBuffer(ctx, buffered)
	return nil
}

// Load adds a snapshot's records (the store's ScanShard) to a shard being rebuilt from
// it. It is Apply but for two things: the seqs need not increase, since a snapshot
// lists records by id, each with the seq of the change that last wrote it; and
// AppliedSeq does not move. Once every record is loaded, the caller Advances to the
// snapshot's seq, and tails the changelog from there.
//
// A Remap in a load is the snapshot's mapping: its seq is not checked (0 will do).
//
// Load is only for a shard that has applied nothing (AppliedSeq is 0): it refuses any
// other with [ErrSeqOrder]. Each id must appear once across the load. Until the
// Advance, a flush persists the loaded records under seq 0, so a copy whose
// CommittedSeq is 0 but which holds segments was interrupted mid-load: wipe it.
func (s *Shard) Load(ctx context.Context, changes []Change) error {
	ctx, span := s.startSpan(ctx, "shard.load", attribute.Int("changes", len(changes)))
	defer span.End()
	buffered, err := s.apply(changes, true)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "load refused")
		return err
	}
	s.inst.countChanges(ctx, changes)
	s.inst.recordBuffer(ctx, buffered)
	return nil
}

// apply checks and buffers changes, returning the buffer's size after. load is Load's
// variant: seqs in any order, into a shard that has applied nothing, and AppliedSeq
// left as it is.
func (s *Shard) apply(changes []Change, load bool) (int, error) {
	for i := range changes {
		if err := s.check(&changes[i]); err != nil {
			c := &changes[i]
			return 0, &ChangeError{Pos: i, Seq: c.Seq, ID: c.id(), Err: err}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return 0, ErrClosed
	}
	if err := s.usable(); err != nil {
		return 0, err
	}
	if limit := s.opts.RefreshBytes * int64(s.opts.MaxBufferFactor); s.opts.RefreshBytes > 0 && s.buf.bytes >= limit {
		wake(s.refreshWake)
		return 0, fmt.Errorf("%w: %d bytes buffered, limit %d", ErrBackpressure, s.buf.bytes, limit)
	}
	last, top, uid, mp := s.applied, s.maxChange, s.indexUID, s.mapState
	if load && last != 0 {
		return 0, fmt.Errorf("%w: a load into a shard that has applied seq %d", ErrSeqOrder, last)
	}
	for i := range changes {
		c := &changes[i]
		if c.IndexUID != "" {
			if uid == "" {
				uid = c.IndexUID
			} else if c.IndexUID != uid {
				return 0, &ChangeError{Pos: i, Seq: c.Seq, ID: c.id(), Err: fmt.Errorf("%w: %q, shard holds %q", ErrIndexUID, c.IndexUID, uid)}
			}
		}
		if c.Kind == Remap {
			if !load && c.MappingVersion <= mp.version {
				return 0, &ChangeError{Pos: i, Seq: c.Seq, Err: fmt.Errorf("%w: mapping version %d after %d", ErrInvalidChange, c.MappingVersion, mp.version)}
			}
			mp = &mappingState{m: c.Mapping, version: c.MappingVersion}
			if load {
				continue // a snapshot's mapping is not a changelog entry
			}
		}
		switch {
		case load && c.Seq <= 0:
			return 0, &ChangeError{Pos: i, Seq: c.Seq, ID: c.id(), Err: fmt.Errorf("%w: seq %d", ErrSeqOrder, c.Seq)}
		case load:
			top = max(top, c.Seq)
		case c.Seq <= last:
			return 0, &ChangeError{Pos: i, Seq: c.Seq, ID: c.id(), Err: fmt.Errorf("%w: seq %d after %d", ErrSeqOrder, c.Seq, last)}
		default:
			last, top = c.Seq, c.Seq
		}
	}
	if len(changes) == 0 {
		return s.buf.size(), nil
	}
	for i := range changes {
		c := &changes[i]
		switch c.Kind {
		case Upsert:
			s.buf.putDoc(c.id(), c.Seq, c.Doc)
		case Delete:
			s.buf.putDoc(c.id(), c.Seq, nil)
		case QueryUpsert:
			s.buf.putQuery(c.QueryID, c.Seq, &StoredQuery{ID: c.QueryID, Seq: c.Seq, Query: c.Query, Meta: c.Meta})
		case QueryDelete:
			s.buf.putQuery(c.QueryID, c.Seq, nil)
		}
	}
	s.applied, s.maxChange, s.indexUID = last, top, uid
	if mp != s.mapState {
		s.mapState = mp
		s.mapping.Store(mp.m)
	}
	s.bufBytes.Store(s.buf.bytes)
	if s.opts.RefreshBytes > 0 && s.buf.bytes >= s.opts.RefreshBytes {
		wake(s.refreshWake)
	}
	return s.buf.size(), nil
}

// CheckDocSize refuses a document a segment cannot store: its id and body together
// over segment.MaxStoredBytes, as [ErrDocTooLarge]. Apply checks every upsert with it;
// a writer checks with it before committing, so the two never disagree.
func CheckDocSize(id string, body []byte) error {
	if size := len(id) + len(body); size > segment.MaxStoredBytes {
		return fmt.Errorf("%w: %d bytes of id and body, over %d", ErrDocTooLarge, size, segment.MaxStoredBytes)
	}
	return nil
}

// Admit reports, without taking a lock, whether Apply would take more changes now:
// [ErrBackpressure] (and a refresh is woken) while the write buffer is over its
// limit, the failure or [ErrClosed] when the shard cannot take writes, else nil. A
// writer calls it before committing changes for this copy, so clients back off before
// the changelog outruns the copy.
func (s *Shard) Admit() error {
	if err := s.Err(); err != nil {
		return err
	}
	select {
	case <-s.closed:
		return ErrClosed
	default:
	}
	if limit := s.opts.RefreshBytes * int64(s.opts.MaxBufferFactor); s.opts.RefreshBytes > 0 {
		if n := s.bufBytes.Load(); n >= limit {
			wake(s.refreshWake)
			return fmt.Errorf("%w: %d bytes buffered, limit %d", ErrBackpressure, n, limit)
		}
	}
	return nil
}

// check refuses a change no refresh could store, before it is buffered.
func (s *Shard) check(c *Change) error {
	switch c.Kind {
	case Upsert:
		if c.Doc == nil {
			return fmt.Errorf("%w: an upsert without a document", ErrInvalidChange)
		}
		if c.DocID != "" && c.DocID != c.Doc.ID {
			return fmt.Errorf("%w: DocID %q but Doc.ID %q", ErrInvalidChange, c.DocID, c.Doc.ID)
		}
		if err := schema.ValidateID(c.Doc.ID); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidChange, err)
		}
		if err := CheckDocSize(c.Doc.ID, c.Doc.Body); err != nil {
			return err
		}
	case Delete:
		if err := schema.ValidateID(c.DocID); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidChange, err)
		}
	case QueryUpsert:
		if err := schema.ValidateID(c.QueryID); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidChange, err)
		}
		if c.Query == nil {
			return fmt.Errorf("%w: a query upsert without a query", ErrInvalidChange)
		}
		if len(c.Meta) > MaxMetaBytes {
			return fmt.Errorf("%w: meta is %d bytes, over %d", ErrInvalidChange, len(c.Meta), MaxMetaBytes)
		}
		if err := s.opts.QueryIndex.Check(&StoredQuery{ID: c.QueryID, Seq: c.Seq, Query: c.Query, Meta: c.Meta}); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidChange, err)
		}
	case QueryDelete:
		if err := schema.ValidateID(c.QueryID); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidChange, err)
		}
	case Remap:
		if c.Mapping == nil || c.MappingVersion <= 0 {
			return fmt.Errorf("%w: a remap without a mapping or a version", ErrInvalidChange)
		}
	default:
		return fmt.Errorf("%w: kind %v", ErrInvalidChange, c.Kind)
	}
	return nil
}

// Advance records that every change up to seq has been applied, when the changelog
// holds none for this shard past the last one applied: the next refresh then covers
// seq, so WaitRefreshed(seq) returns, and the flush after it moves CommittedSeq on
// without a replay. A seq at or below the applied one is a no-op.
func (s *Shard) Advance(seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return ErrClosed
	}
	if err := s.usable(); err != nil {
		return err
	}
	if seq > s.applied {
		s.applied = seq
	}
	return nil
}

// Acquire returns the current generation, with a reference the caller must drop with
// [Generation.Release]. It takes no lock and allocates nothing. It returns nil once
// the shard is closed.
func (s *Shard) Acquire() *Generation {
	for {
		g := s.cur.Load()
		if g == nil {
			return nil
		}
		if g.tryRef() {
			return g
		}
		// g was retired between the Load and tryRef: the shard drops its own reference
		// only after publishing g's successor, so the next Load sees that (or nil).
	}
}

// WaitRefreshed blocks until a generation covering seq is published (the first refresh
// whose applied seq is at or past seq; seq need not be one of this shard's changes),
// ctx ends, or the shard closes or fails (it returns [ErrClosed] or the failure).
func (s *Shard) WaitRefreshed(ctx context.Context, seq int64) error {
	for {
		s.waitMu.Lock()
		ch := s.waitCh
		s.waitMu.Unlock()
		g := s.cur.Load()
		if g == nil {
			return ErrClosed
		}
		if g.seq >= seq {
			return nil
		}
		if err := s.Err(); err != nil {
			return err
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closed:
			if g := s.cur.Load(); g != nil && g.seq >= seq {
				return nil
			}
			return ErrClosed
		}
	}
}

// notifyPublished wakes every WaitRefreshed.
func (s *Shard) notifyPublished() {
	s.waitMu.Lock()
	close(s.waitCh)
	s.waitCh = make(chan struct{})
	s.waitMu.Unlock()
}

// Close refreshes the buffer and flushes one last time (so the manifest covers every
// applied change), stops background refresh, flushes and merges (an in-flight merge is
// abandoned), and releases the shard's own reference on its generation. Generations
// readers still hold stay valid until released; their segments are unmapped then.
// Close is idempotent: a second call returns once the first has finished (or ctx
// ends).
func (s *Shard) Close(ctx context.Context) error {
	ctx, span := s.startSpan(ctx, "shard.close")
	defer span.End()
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		// Another Close (or a shutdown) is under way: return once it has finished.
		select {
		case <-s.shutdownDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.closing = true
	s.mu.Unlock()

	var err error
	if s.Err() == nil {
		err = s.Refresh(ctx)
		if err == nil {
			err = s.Flush(ctx)
		}
	}
	s.shutdown()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "final refresh or flush failed")
		return err
	}
	s.log.InfoContext(ctx, "shard closed", slog.Int64("seq", s.CommittedSeq()))
	return nil
}

// Abandon stops the shard at once, as if its process had died: there is no final
// refresh or flush, so whatever was applied since the last flush is lost and the
// directory keeps exactly what CommittedSeq covers. Generations readers hold stay valid
// until released. It is for a copy about to be wiped or reopened (a failed one, or one
// whose index was recreated), where a final flush is wasted work or impossible. A
// Close after it returns at once.
func (s *Shard) Abandon() { s.shutdown() }

// Options returns the options the shard was opened with, defaults filled in: open the
// directory again with them to get the same shard.
func (s *Shard) Options() Options { return s.opts }

// shutdown stops the background goroutines and drops the shard's generation, without
// any further flush.
func (s *Shard) shutdown() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		s.mu.Unlock()
		s.cancel()
		s.wg.Wait()
		// Taking the locks orders this after any flush, refresh or merge publish in
		// flight.
		s.flushSem <- struct{}{}
		s.refreshMu.Lock()
		s.commitMu.Lock()
		g := s.cur.Swap(nil)
		close(s.closed)
		s.commitMu.Unlock()
		s.refreshMu.Unlock()
		<-s.flushSem
		s.notifyPublished()
		if g != nil {
			g.Release()
		}
		s.jan.stop()
		s.inst.stop()
		close(s.shutdownDone)
	})
}

// rateLimitedWarn lets a recurring failure be logged at most once per every, counting
// the occurrences it held back.
type rateLimitedWarn struct {
	clock      clock.Clock
	every      time.Duration
	last       time.Time
	suppressed int
}

// allow reports whether to log this occurrence and, if so, how many were held back
// since the last one logged.
func (w *rateLimitedWarn) allow() (suppressed int, ok bool) {
	if now := w.clock.Now(); now.Sub(w.last) >= w.every {
		suppressed = w.suppressed
		w.last, w.suppressed = now, 0
		return suppressed, true
	}
	w.suppressed++
	return 0, false
}

// wake signals ch without blocking.
func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (s *Shard) refreshLoop() {
	defer s.wg.Done()
	warn := rateLimitedWarn{clock: s.opts.Clock, every: time.Minute}
	clock.GridLoop{
		Clock:  s.opts.Clock,
		Period: func() time.Duration { return s.opts.RefreshInterval },
		Now:    s.refreshWake,
		Task: func() {
			if s.Err() != nil {
				return
			}
			if err := s.Refresh(s.bg); err != nil && s.bg.Err() == nil && !errors.Is(err, ErrClosed) {
				if suppressed, ok := warn.allow(); ok {
					s.log.WarnContext(s.bg, "background refresh failed", slog.Any("error", err), slog.Int("suppressed", suppressed))
				}
			}
		},
	}.Run(s.bg)
}
