// Package store is Searchlight's SQL storage (spec §8): the changelog that is
// the write-ahead log, the documents and saved queries that are the system of
// record, the index catalogue, the cluster registry and shard-copy leases
// (spec §9), and optional segment blobs.
//
// The logic is written once, here, for every dialect (sqlite, postgres,
// mysql): transaction shapes and retries, seq allocation under the counter
// lock, lease and epoch fencing, the blob protocol and the Apply guards. Each
// dialect package owns its SQL, as the statement tables of package dialect,
// and spells each statement its engine's best way. Apply locks the single
// counter row, takes contiguous sequence numbers and commits, so commit order
// equals seq order and a tailer reading ChangesAfter never skips a change
// that commits later. GroupCommitter coalesces concurrent Apply calls on a
// node into one transaction.
package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
	"github.com/Imposter/go-searchlight/internal/store/mysql"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
	"github.com/Imposter/go-searchlight/internal/store/sqlite"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Store is the SQL store.
type Store interface {
	// Migrate brings the schema up to date. It runs under a lock, so every
	// node may call it at start; each migration is applied once.
	Migrate(ctx context.Context) error

	// Apply commits batch as one transaction and returns its sequence
	// numbers, firstSeq..lastSeq in batch order, and sets each applied
	// change's Seq in batch. Every change's index must exist. When an IfSeq
	// condition fails it returns a *ConflictError and applies nothing. A
	// change whose IfExists target does not exist is skipped instead: it
	// writes nothing, takes no seq (its Seq stays 0) and fails nothing else.
	Apply(ctx context.Context, batch []Change) (firstSeq, lastSeq int64, err error)

	// ChangesAfter returns up to limit changes of shard with seq > seq, in
	// seq order (limit <= 0 means DefaultChangesLimit). It returns ErrPruned
	// when some of those changes have been pruned.
	ChangesAfter(ctx context.Context, shard ShardID, seq int64, limit int) ([]Change, error)

	// ScanShard calls fn with the index's mapping (a RecordMapping; none when
	// the index does not exist), then every document and every saved query of
	// shard, from one consistent snapshot, and returns the seq that snapshot
	// reflects: replaying ChangesAfter(asOfSeq) on top of it is exact.
	ScanShard(ctx context.Context, shard ShardID, fn func(Record) error) (asOfSeq int64, err error)

	// HeadSeq returns the newest committed seq across every shard (0 before
	// the first change), and the database clock's time as it read it. Every
	// change with a seq at or below it has committed, so a ChangesAfter that
	// starts after HeadSeq returns and yields fewer than its limit holds every
	// change of its shard up to HeadSeq: a tailer may advance its copy to it.
	//
	// That holds only while HeadSeq and ChangesAfter read the same database
	// (the primary): a read replica lagging the primary would break it. Every
	// dialect reads from the primary (dialect.Pools.Read is the primary's).
	HeadSeq(ctx context.Context) (seq int64, now time.Time, err error)

	// Registry is the cluster registry: nodes, heartbeats and shard leases.
	Registry() RegistryStore
	// Blobs stores segment bundles for recovery without a peer.
	Blobs() BlobStore
	// Indexes is the index catalogue.
	Indexes() IndexStore

	// Prune deletes shard's changes with seq < belowSeq. Later ChangesAfter
	// calls that would need them return ErrPruned. The caller keeps belowSeq
	// behind every live copy's applied seq (spec §9).
	Prune(ctx context.Context, shard ShardID, belowSeq int64) error

	// Ping checks the database is reachable.
	Ping(ctx context.Context) error

	// Dialect names the SQL dialect: sqlite, postgres or mysql.
	Dialect() string

	// Close releases the connection pools.
	Close() error
}

// Applier is the part of Store that GroupCommitter needs.
type Applier interface {
	Apply(ctx context.Context, batch []Change) (firstSeq, lastSeq int64, err error)
}

// RegistryStore is the cluster registry in sl_nodes and sl_shard_copies.
//
// A shard copy is a slot (0 to target-1) of a shard held under a lease. Times
// are judged by the database clock, so nodes need not agree on the time.
type RegistryStore interface {
	// Heartbeat registers the node or refreshes its address, version,
	// capacity and heartbeat time.
	Heartbeat(ctx context.Context, n Node) error
	// RemoveNode deletes a node's registration (a clean shutdown).
	RemoveNode(ctx context.Context, nodeID string) error
	// Nodes lists every registered node, live or not.
	Nodes(ctx context.Context) ([]Node, error)

	// ClaimCopy gives nodeID a copy of shard when the shard has fewer than
	// target live copies: it takes a free slot below target, or steals one
	// whose lease has expired. A node that already holds a copy keeps it and
	// has its lease renewed. It reports whether nodeID holds a copy
	// afterwards, and that copy. A new copy starts recovering at seq 0.
	ClaimCopy(ctx context.Context, shard ShardID, nodeID string, target int, ttl time.Duration) (Copy, bool, error)
	// RenewLeases extends every unexpired lease nodeID holds to ttl from now
	// and returns those shards. A lease that has already expired is not
	// renewed; the node must claim it again, which succeeds unless the slot
	// was taken meanwhile.
	RenewLeases(ctx context.Context, nodeID string, ttl time.Duration) ([]ShardID, error)
	// ReleaseCopy gives up a copy. It returns ErrLeaseLost if the slot no
	// longer holds this incarnation (same node and epoch).
	ReleaseCopy(ctx context.Context, c Copy) error
	// Copies lists the copies of index's shards, or of every index when
	// index is "", expired leases included.
	Copies(ctx context.Context, index string) ([]Copy, error)
	// SetCopyState changes a copy's state. It returns ErrLeaseLost unless
	// the slot still holds this incarnation (same node and epoch) under an
	// unexpired lease.
	SetCopyState(ctx context.Context, c Copy, state CopyState) error
	// ReportApplied records the seq a copy has applied. It returns
	// ErrLeaseLost unless the slot still holds this incarnation.
	ReportApplied(ctx context.Context, c Copy, seq int64) error
}

// BlobStore keeps named blobs (segment bundles) in sl_blobs, split into
// chunks so neither side ever holds a whole bundle in memory. Uploads are
// tracked, so what a crash leaves behind is found and removed by Sweep.
type BlobStore interface {
	// Put stores r under name, replacing any blob of that name once the new
	// one is complete. Readers see the old blob or the new one, never a mix.
	// Put can fail with ErrAmbiguousCommit when the commit's own outcome
	// could not be learned (a cancelled context or a network blip racing
	// the server's decision): name may or may not have been replaced, and
	// the caller must Stat or Get to find out rather than assume either way.
	Put(ctx context.Context, name string, r io.Reader) (BlobInfo, error)
	// Get streams a blob. The reader fetches one chunk at a time and fails
	// with ErrChecksum if the content does not match; it fails with
	// ErrNotFound if the blob is replaced or deleted mid-read. Close it: the
	// operation's span ends then.
	Get(ctx context.Context, name string) (io.ReadCloser, BlobInfo, error)
	// Stat describes a blob.
	Stat(ctx context.Context, name string) (BlobInfo, error)
	// List describes the blobs whose names start with prefix, by name.
	List(ctx context.Context, prefix string) ([]BlobInfo, error)
	// Delete removes a blob; deleting a missing blob is not an error.
	Delete(ctx context.Context, name string) error
	// Sweep removes uploads abandoned by crashed or interrupted writers and
	// cleanups, judging staleness by the database clock: unreferenced uploads
	// untouched for olderThan, and chunks no upload owns. It works in short
	// statements and returns how many uploads it removed. olderThan must be
	// well beyond the time a Put takes to write blobTouchEvery (16) chunks;
	// an hour is ample. The cluster runs it periodically.
	Sweep(ctx context.Context, olderThan time.Duration) (int, error)
}

// IndexStore is the index catalogue in sl_indexes.
type IndexStore interface {
	// Create adds an index, or returns ErrExists. It returns the stored
	// entry (version 1), with a fresh IndexMeta.UID: creating an index under
	// a name a prior index once held, after it was Dropped, is a new
	// incarnation, distinct from the old one even though the name repeats.
	Create(ctx context.Context, m IndexMeta) (IndexMeta, error)
	// Get returns an index or ErrNotFound.
	Get(ctx context.Context, name string) (IndexMeta, error)
	// List returns every index by name.
	List(ctx context.Context) ([]IndexMeta, error)
	// Update replaces an index's mapping and settings if m.Version is still
	// current, returning the new entry; ErrConflict otherwise. When the
	// mapping changes (its bytes differ), MappingVersion moves on and, in the
	// same transaction, a KindMapping change carrying the new mapping is
	// logged to every shard of the index (Settings' shard count after the
	// update), with contiguous seqs: copies adopt it exactly at its seq. Every
	// mapping change must go through Update. Settings' shard count is fixed at
	// Create: an Update that changes it fails with ErrInvalid.
	Update(ctx context.Context, m IndexMeta) (IndexMeta, error)
	// Drop deletes an index with its documents, queries, changes and shard
	// copies, or returns ErrNotFound.
	Drop(ctx context.Context, name string) error
}

// Watcher is implemented by stores whose database pushes commit
// notifications (Postgres LISTEN/NOTIFY). Tailers use it to wake early;
// polling remains the safety net.
type Watcher interface {
	// Watch calls fn for each committed batch's shards until ctx ends or the
	// connection fails. ready, when not nil, is called once the subscription
	// is active: every change that commits after ready is announced, so a
	// tailer that polls from ready on misses nothing a lost connection
	// dropped. Neither fn nor ready may block.
	Watch(ctx context.Context, ready func(), fn func(Notification)) error
}

// DefaultChangesLimit is ChangesAfter's limit when the caller passes none.
const DefaultChangesLimit = 1000

// DefaultBlobChunkSize is the blob chunk size.
const DefaultBlobChunkSize = 1 << 20

// Option adjusts Open.
type Option func(*options)

type options struct {
	tracer    trace.Tracer
	meter     metric.Meter
	logger    *slog.Logger
	blobChunk int
}

// WithTracer sets the tracer for store spans (default: the global
// provider's, which telemetry.Setup installs).
func WithTracer(t trace.Tracer) Option { return func(o *options) { o.tracer = t } }

// WithMeter sets the meter for store metrics (default: the global
// provider's).
func WithMeter(m metric.Meter) Option { return func(o *options) { o.meter = m } }

// WithLogger sets the logger (default: slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

// WithBlobChunkSize sets the blob chunk size in bytes.
func WithBlobChunkSize(n int) Option { return func(o *options) { o.blobChunk = n } }

// Dialects maps each store_url scheme to its dialect.
func dialectFor(scheme string) (*dialect.Dialect, error) {
	switch strings.ToLower(scheme) {
	case "postgres", "postgresql":
		return postgres.Dialect(), nil
	case "mysql":
		return mysql.Dialect(), nil
	case "sqlite":
		return sqlite.Dialect(), nil
	}
	return nil, fmt.Errorf("%w: unsupported store URL scheme %q (want postgres, postgresql, mysql or sqlite)", ErrInvalid, scheme)
}

// Open connects to the store at rawURL, choosing the dialect by scheme, and
// checks the database answers. It does not migrate; call Migrate.
//
//	postgres://user:pass@host:5432/db?sslmode=disable (or postgresql://)
//	mysql://user:pass@host:3306/db
//	sqlite:///var/lib/searchlight/searchlight.db
func Open(ctx context.Context, rawURL string, opts ...Option) (Store, error) {
	o := options{blobChunk: DefaultBlobChunkSize}
	for _, opt := range opts {
		opt(&o)
	}
	if o.tracer == nil {
		o.tracer = otel.Tracer(telemetry.ScopeName)
	}
	if o.meter == nil {
		o.meter = otel.Meter(telemetry.ScopeName)
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	if o.blobChunk <= 0 {
		return nil, invalidf("blob chunk size %d", o.blobChunk)
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		// url.Error repeats the URL, which may hold a password.
		return nil, fmt.Errorf("%w: store URL is not a valid URL", ErrInvalid)
	}
	d, err := dialectFor(u.Scheme)
	if err != nil {
		return nil, err
	}
	pools, err := d.Open(u)
	if err != nil {
		return nil, fmt.Errorf("open %s store: %w", d.Name, err)
	}
	s, err := newSQLStore(d, pools, &o)
	if err != nil {
		_ = pools.Close()
		return nil, err
	}
	if err := s.Ping(ctx); err != nil {
		_ = pools.Close()
		return nil, fmt.Errorf("open %s store: %w", d.Name, err)
	}
	if d.Listen != nil {
		return &watchingStore{s}, nil
	}
	return s, nil
}

// IsTransient reports whether err is worth retrying later (the database is
// unreachable or a transaction lost a race), as opposed to a bad request.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range []error{ErrNotFound, ErrExists, ErrConflict, ErrPruned, ErrLeaseLost, ErrInvalid, ErrClosed, ErrChecksum, ErrNewerSchema, context.Canceled} {
		if errors.Is(err, e) {
			return false
		}
	}
	return true
}
