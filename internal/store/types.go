package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"
)

// Limits on keys, enforced the same way on every dialect.
const (
	MaxIndexName = 255 // bytes
	MaxID        = 512 // bytes; document and saved-query ids are 1-512 bytes (spec §3)
	MaxNodeID    = 255 // bytes
	MaxBlobName  = 512 // bytes
)

// ShardID names one shard of an index.
type ShardID struct {
	Index string
	Shard int
}

func (s ShardID) String() string { return s.Index + "/" + strconv.Itoa(s.Shard) }

// Kind is what a change does.
type Kind string

// Change kinds. Upserts carry a payload; deletes do not.
const (
	// KindUpsert creates or replaces a document; Payload is its JSON body.
	KindUpsert Kind = "upsert"
	// KindDelete removes a document.
	KindDelete Kind = "delete"
	// KindQueryUpsert creates or replaces a saved query; Payload is a
	// QueryPayload as JSON.
	KindQueryUpsert Kind = "query_upsert"
	// KindQueryDelete removes a saved query.
	KindQueryDelete Kind = "query_delete"
)

func (k Kind) valid() bool {
	switch k {
	case KindUpsert, KindDelete, KindQueryUpsert, KindQueryDelete:
		return true
	}
	return false
}

func (k Kind) isQuery() bool { return k == KindQueryUpsert || k == KindQueryDelete }

func (k Kind) isUpsert() bool { return k == KindUpsert || k == KindQueryUpsert }

// Conditions for Change.IfSeq.
const (
	// IfAbsent makes a change apply only when the document or query does not
	// exist.
	IfAbsent int64 = -1
)

// Change is one entry of the changelog: a document or saved-query upsert or
// delete on one shard. Apply assigns Seq and At; callers leave them zero.
type Change struct {
	// Seq is the change's position in the global changelog. Sequence numbers
	// are contiguous and commit in order.
	Seq   int64
	Index string
	Shard int
	Kind  Kind
	// ID is the document or saved-query id, 1-512 bytes of UTF-8.
	ID string
	// Payload is the document's JSON body (KindUpsert) or a QueryPayload
	// (KindQueryUpsert); nil for deletes.
	Payload []byte
	// At is when the change committed, by the database clock.
	At time.Time
	// IfSeq makes the change conditional, checked atomically inside Apply in
	// batch order: a positive IfSeq requires the target's current seq to equal
	// it; IfAbsent requires the target not to exist. Zero applies
	// unconditionally. It is not stored.
	IfSeq int64
}

// ShardID returns the shard the change belongs to.
func (c *Change) ShardID() ShardID { return ShardID{Index: c.Index, Shard: c.Shard} }

// QueryPayload is the payload of a saved-query upsert: the query DSL tree and
// its opaque meta object, stored in sl_queries' query and meta columns.
type QueryPayload struct {
	Query json.RawMessage `json:"query"`
	Meta  json.RawMessage `json:"meta,omitempty"`
}

// EncodeQueryPayload builds a KindQueryUpsert payload.
func EncodeQueryPayload(query, meta json.RawMessage) ([]byte, error) {
	return json.Marshal(QueryPayload{Query: query, Meta: meta})
}

// DecodeQueryPayload splits a KindQueryUpsert payload.
func DecodeQueryPayload(p []byte) (QueryPayload, error) {
	var q QueryPayload
	if err := json.Unmarshal(p, &q); err != nil {
		return q, fmt.Errorf("query payload: %w", err)
	}
	if len(q.Query) == 0 || string(q.Query) == "null" {
		return q, errors.New("query payload: no query")
	}
	return q, nil
}

// RecordKind says whether a record is a document or a saved query.
type RecordKind uint8

// Record kinds.
const (
	RecordDocument RecordKind = iota + 1
	RecordQuery
)

// Record is the current state of one document or saved query, as ScanShard
// reads it from sl_documents and sl_queries.
type Record struct {
	Kind  RecordKind
	Index string
	Shard int
	ID    string
	// Body is the document's JSON, or the saved query's DSL tree.
	Body []byte
	// Meta is the saved query's meta object (nil for documents).
	Meta []byte
	// Seq is the seq of the change that last wrote the record.
	Seq int64
}

// IndexMeta is an index's catalogue entry in sl_indexes.
type IndexMeta struct {
	Name     string
	Mapping  []byte // JSON
	Settings []byte // JSON
	// Version counts updates; Update succeeds only against the current one.
	Version   int64
	CreatedAt time.Time
}

// CopyState is a shard copy's lifecycle state.
type CopyState string

// Shard copy states (spec §8, §9).
const (
	CopyRecovering CopyState = "recovering"
	CopyServing    CopyState = "serving"
	CopyRetiring   CopyState = "retiring"
)

func (s CopyState) valid() bool {
	return s == CopyRecovering || s == CopyServing || s == CopyRetiring
}

// Node is a registered node.
type Node struct {
	ID       string
	Address  string
	Version  string
	Capacity int
	// HeartbeatAt is the last heartbeat by the database clock, and
	// HeartbeatAge how long ago that was, also by the database clock, so a
	// caller judges liveness without comparing clocks.
	HeartbeatAt  time.Time
	HeartbeatAge time.Duration
	StartedAt    time.Time
}

// Copy is one shard copy: a lease-holding slot of a shard.
type Copy struct {
	Shard ShardID
	// Slot is the copy's slot, 0 to target-1 when it was claimed.
	Slot       int
	NodeID     string
	State      CopyState
	AppliedSeq int64
	// Epoch is the copy's fencing token: it changes whenever the slot gets a
	// new owner and stays while the owner renews. Updates by the copy name
	// it, so a stale incarnation can never touch its successor's row.
	Epoch      int64
	LeaseUntil time.Time
	// LeaseLeft is the lease's remaining time by the database clock;
	// zero or negative means it has expired and the slot may be taken.
	LeaseLeft time.Duration
}

// Expired reports whether the copy's lease has run out.
func (c *Copy) Expired() bool { return c.LeaseLeft <= 0 }

// Notification announces that a shard's changelog advanced to Seq.
type Notification struct {
	Shard ShardID
	Seq   int64
}

// BlobInfo describes a stored blob.
type BlobInfo struct {
	Name      string
	Size      int64
	Chunks    int
	SHA256    string // hex
	CreatedAt time.Time
}

// Errors.
var (
	// ErrNotFound is returned for a missing index, blob or similar.
	ErrNotFound = errors.New("store: not found")
	// ErrExists is returned when creating something that already exists.
	ErrExists = errors.New("store: already exists")
	// ErrConflict is returned when a version or seq condition fails. Apply
	// returns a *ConflictError, which matches it.
	ErrConflict = errors.New("store: conflict")
	// ErrPruned is returned by ChangesAfter when changes the caller still
	// needs have been pruned; the copy must recover from ScanShard or a peer.
	ErrPruned = errors.New("store: changes pruned")
	// ErrLeaseLost is returned when a node acts on a shard copy it no longer
	// holds.
	ErrLeaseLost = errors.New("store: shard copy lease lost")
	// ErrInvalid is returned for malformed input; the error wraps it with
	// details.
	ErrInvalid = errors.New("store: invalid")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("store: closed")
	// ErrChecksum is returned when a blob does not match its checksum.
	ErrChecksum = errors.New("store: blob checksum mismatch")
	// ErrNewerSchema is returned by Migrate when the database has migrations
	// this binary does not know.
	ErrNewerSchema = errors.New("store: database schema is newer than this binary")
)

// ConflictError is returned by Apply when changes' IfSeq conditions fail.
// Nothing in the batch was applied.
type ConflictError struct {
	// Positions are the failing changes' indexes in the batch, ascending.
	Positions []int
	// Current is each failing target's seq at that point of the batch (0 when
	// it did not exist), parallel to Positions.
	Current []int64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("store: %d change(s) failed their seq condition (first at position %d, current seq %d)",
		len(e.Positions), e.Positions[0], e.Current[0])
}

// Is makes a ConflictError match ErrConflict.
func (e *ConflictError) Is(target error) bool { return target == ErrConflict }

// ChangeError reports an invalid change by its position in the caller's
// batch. It wraps ErrInvalid.
type ChangeError struct {
	Position int
	Err      error
}

func (e *ChangeError) Error() string { return fmt.Sprintf("change %d: %v", e.Position, e.Err) }

// Unwrap returns the reason.
func (e *ChangeError) Unwrap() error { return e.Err }

// IndexNotFoundError is returned by Apply when changes name indexes that do
// not exist. Nothing in the batch was applied.
type IndexNotFoundError struct {
	Indexes []string // the missing indexes, in first-use order
	// Positions are the changes naming them, ascending.
	Positions []int
}

func (e *IndexNotFoundError) Error() string {
	return fmt.Sprintf("store: index %q not found (%d change(s), first at position %d)", e.Indexes[0], len(e.Positions), e.Positions[0])
}

// Is makes an IndexNotFoundError match ErrNotFound.
func (e *IndexNotFoundError) Is(target error) bool { return target == ErrNotFound }

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// validKey checks a name or id: 1..max bytes of UTF-8 without NUL, which
// every dialect stores the same way.
func validKey(what, s string, maxLen int) error {
	switch {
	case s == "":
		return invalidf("%s is empty", what)
	case len(s) > maxLen:
		return invalidf("%s is %d bytes, more than %d", what, len(s), maxLen)
	case !utf8.ValidString(s):
		return invalidf("%s is not valid UTF-8", what)
	}
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return invalidf("%s contains NUL", what)
		}
	}
	return nil
}

func validShard(s ShardID) error {
	if err := validKey("index name", s.Index, MaxIndexName); err != nil {
		return err
	}
	if s.Shard < 0 || s.Shard > 1<<20 {
		return invalidf("shard %d out of range", s.Shard)
	}
	return nil
}

func validJSON(what string, b []byte) error {
	if !utf8.Valid(b) {
		return invalidf("%s is not valid UTF-8", what)
	}
	if !json.Valid(b) {
		return invalidf("%s is not valid JSON", what)
	}
	return nil
}

func millis(t int64) time.Time { return time.UnixMilli(t).UTC() }
