// Package api is Searchlight's public HTTP/JSON API (spec section 5): the index,
// document, bulk, search, count, saved-query, percolate, field-catalogue and cluster
// endpoints, with bearer-token auth, request limits, problem JSON errors, request
// deadlines and the read-your-writes controls (refresh, wait_for_seq, if_seq).
//
// The handlers are a thin layer over a [Coordinator], which owns the data: the
// single-node coordinator (internal/node) serves every shard from local copies, and
// the cluster coordinator routes to peers. The handlers parse and bound the request,
// call the coordinator under the request's deadline, and turn its answer, or its
// error, into JSON.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
)

// Coordinator serves the API's requests. Every method honours ctx's deadline and
// cancellation. Errors that are the caller's fault are *[Error] values; the API maps
// the rest (store, shard and context errors) itself (see [ProblemFor]).
type Coordinator interface {
	// CreateIndex creates an index; an existing one is a 409.
	CreateIndex(ctx context.Context, name string, spec IndexSpec) (*IndexInfo, error)
	// ListIndexes describes every index, by name.
	ListIndexes(ctx context.Context) ([]*IndexInfo, error)
	// GetIndex describes one index.
	GetIndex(ctx context.Context, name string) (*IndexInfo, error)
	// DeleteIndex drops an index with its documents and saved queries.
	DeleteIndex(ctx context.Context, name string) error
	// PatchMapping adds fields to an index's mapping (additive only).
	PatchMapping(ctx context.Context, name string, fields map[string]schema.FieldType) (*IndexInfo, error)
	// PatchSettings changes an index's refresh interval or replica target.
	PatchSettings(ctx context.Context, name string, patch SettingsPatch) (*IndexInfo, error)

	// Write commits ops to index in one transaction and returns each op's outcome
	// in order. An op refused on its own (an invalid document, a failed if_seq, a
	// delete of nothing) is reported in its item and the rest still commit; an
	// error is returned only when nothing could be written. Write may release the
	// ops' bodies once it holds its own copies.
	Write(ctx context.Context, index string, ops []WriteOp, opts WriteOptions) (*WriteResult, error)
	// GetDocument reads a document's current version (realtime).
	GetDocument(ctx context.Context, index, id string) (*Document, error)
	// GetQuery reads a saved query's current version (realtime).
	GetQuery(ctx context.Context, index, id string) (*SavedQuery, error)
	// ListQueries pages an index's saved queries by id: up to size of them with ids
	// above after.
	ListQueries(ctx context.Context, index, after string, size int) ([]*SavedQuery, error)

	// Search runs a search over every shard of index.
	Search(ctx context.Context, index string, r *search.Request, opts ReadOptions) (*SearchResult, error)
	// Percolate returns the saved queries of index each document matches.
	Percolate(ctx context.Context, index string, req *PercolateRequest, opts ReadOptions) (*PercolateResponse, error)
	// Fields describes an index's fields from its mapping, with each list field's
	// top entries (up to entries of them) when entries > 0.
	Fields(ctx context.Context, index string, entries int, opts ReadOptions) (*FieldCatalog, error)

	// Health is the cluster's health (green, yellow or red).
	Health(ctx context.Context) (*ClusterHealth, error)
	// Nodes lists the cluster's nodes.
	Nodes(ctx context.Context) ([]NodeInfo, error)
	// Shards lists every shard copy with its progress.
	Shards(ctx context.Context) ([]ShardInfo, error)
	// Ready reports why the node should not take traffic, or nil.
	Ready(ctx context.Context) error
	// Close stops the coordinator: it flushes and closes every shard copy. The API
	// calls it once its listener has drained.
	Close(ctx context.Context) error
}

// IndexSpec is a new index's mapping and settings.
type IndexSpec struct {
	Mapping  *schema.Mapping
	Settings IndexSettings
}

// Index settings defaults and bounds.
const (
	DefaultShards   = 1
	MaxShards       = 1024
	MaxReplicas     = 1024
	DisabledRefresh = time.Duration(-1)
)

// IndexSettings are an index's settings (spec section 3).
type IndexSettings struct {
	// Shards is the number of primary shards, fixed at creation.
	Shards int
	// ReplicasPerShard is the target number of copies of each shard; 0 means every
	// node holds every shard.
	ReplicasPerShard int
	// RefreshInterval is how often written documents become searchable; 0 means the
	// node's refresh_interval, DisabledRefresh never (refresh=true and wait_for
	// still refresh).
	RefreshInterval time.Duration
}

type settingsJSON struct {
	Shards           *int            `json:"shards,omitempty"`
	ReplicasPerShard *int            `json:"replicas_per_shard,omitempty"`
	RefreshInterval  json.RawMessage `json:"refresh_interval,omitempty"`
}

// MarshalJSON writes {"shards": n, "replicas_per_shard": n, "refresh_interval": "1s"},
// with refresh_interval omitted when it is the node's and -1 when disabled.
func (s IndexSettings) MarshalJSON() ([]byte, error) {
	out := settingsJSON{Shards: &s.Shards, ReplicasPerShard: &s.ReplicasPerShard}
	switch {
	case s.RefreshInterval == DisabledRefresh:
		out.RefreshInterval = json.RawMessage("-1")
	case s.RefreshInterval > 0:
		out.RefreshInterval, _ = json.Marshal(s.RefreshInterval.String())
	}
	return json.Marshal(out)
}

// UnmarshalJSON reads what MarshalJSON writes, refusing unknown keys and values out of
// bounds; a missing shards is DefaultShards.
func (s *IndexSettings) UnmarshalJSON(data []byte) error {
	var raw settingsJSON
	if err := strictDecode(data, &raw); err != nil {
		return err
	}
	out := IndexSettings{Shards: DefaultShards}
	if raw.Shards != nil {
		out.Shards = *raw.Shards
	}
	if raw.ReplicasPerShard != nil {
		out.ReplicasPerShard = *raw.ReplicasPerShard
	}
	if raw.RefreshInterval != nil {
		d, err := parseRefreshInterval(raw.RefreshInterval)
		if err != nil {
			return err
		}
		out.RefreshInterval = d
	}
	if err := out.Validate(); err != nil {
		return err
	}
	*s = out
	return nil
}

// Validate checks the settings' bounds.
func (s *IndexSettings) Validate() error {
	if s.Shards < 1 || s.Shards > MaxShards {
		return &FieldError{Loc: "shards", Message: fmt.Sprintf("shards is from 1 to %d", MaxShards)}
	}
	if s.ReplicasPerShard < 0 || s.ReplicasPerShard > MaxReplicas {
		return &FieldError{Loc: "replicas_per_shard", Message: fmt.Sprintf("replicas_per_shard is from 0 (every node) to %d", MaxReplicas)}
	}
	if s.RefreshInterval < 0 && s.RefreshInterval != DisabledRefresh {
		return &FieldError{Loc: "refresh_interval", Message: `refresh_interval is a positive duration ("1s") or -1`}
	}
	return nil
}

// parseRefreshInterval reads "1s", "500ms" or -1 (disabled).
func parseRefreshInterval(raw json.RawMessage) (time.Duration, error) {
	bad := &FieldError{Loc: "refresh_interval", Message: `refresh_interval is a positive duration ("1s") or -1`}
	if t := bytes.TrimSpace(raw); string(t) == "-1" || string(t) == `"-1"` {
		return DisabledRefresh, nil
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return 0, bad
	}
	d, err := time.ParseDuration(text)
	if err != nil || d <= 0 || d > 24*time.Hour {
		return 0, bad
	}
	return d, nil
}

// SettingsPatch changes the settings that may change after creation; nil leaves one
// as it is.
type SettingsPatch struct {
	RefreshInterval  *time.Duration
	ReplicasPerShard *int
}

// FieldError is a request value that is out of bounds, at Loc.
type FieldError struct {
	Loc, Message string
}

func (e *FieldError) Error() string { return e.Loc + ": " + e.Message }

// IndexInfo describes an index.
type IndexInfo struct {
	Name      string          `json:"name"`
	UID       string          `json:"uid"`
	Version   int64           `json:"version"`
	CreatedAt time.Time       `json:"created_at"`
	Mapping   *schema.Mapping `json:"mapping"`
	Settings  IndexSettings   `json:"settings"`
	// Docs and Queries count the live documents and saved queries searchable on
	// this node.
	Docs    uint64 `json:"docs"`
	Queries uint64 `json:"queries"`
}

// OpKind is what a write op does.
type OpKind uint8

// The write op kinds.
const (
	OpUpsert OpKind = iota + 1
	OpDelete
	OpQueryUpsert
	OpQueryDelete
)

var opNames = [...]string{OpUpsert: "upsert", OpDelete: "delete", OpQueryUpsert: "query_upsert", OpQueryDelete: "query_delete"}

func (k OpKind) String() string {
	if k >= OpUpsert && k <= OpQueryDelete {
		return opNames[k]
	}
	return fmt.Sprintf("OpKind(%d)", uint8(k))
}

// IfAbsent is an IfSeq that requires the document or query not to exist.
const IfAbsent int64 = -1

// WriteOp is one document or saved-query write.
type WriteOp struct {
	Kind OpKind
	ID   string
	// Body is an upsert's document, a JSON object.
	Body json.RawMessage
	// Query and Meta are a saved query's DSL tree and opaque meta object.
	Query json.RawMessage
	Meta  json.RawMessage
	// IfSeq makes the op conditional: a positive IfSeq requires the target's current
	// seq to equal it, IfAbsent requires it not to exist, 0 applies unconditionally.
	IfSeq int64
}

// RefreshMode is a write's refresh parameter.
type RefreshMode uint8

// The refresh modes.
const (
	// RefreshNone returns once the write is committed; it becomes searchable within
	// the refresh interval.
	RefreshNone RefreshMode = iota
	// RefreshTrue refreshes the written shards at once, then returns.
	RefreshTrue
	// RefreshWaitFor returns once the write is searchable on this node.
	RefreshWaitFor
)

// WriteOptions are a write's parameters.
type WriteOptions struct {
	Refresh RefreshMode
	// Percolate returns each upserted document's matching saved queries.
	Percolate bool
	// WaitForSeq, with Percolate, waits for this node's copies to have every change
	// up to it searchable before percolating, so saved queries written up to it are
	// seen.
	WaitForSeq int64
}

// WriteResult is the outcome of a Write.
type WriteResult struct {
	// Seq is the newest seq the write committed, 0 when it committed nothing. Pass
	// it as wait_for_seq to read the write back.
	Seq int64
	// Items are the ops' outcomes, in order.
	Items []ItemResult
	// TimedOut is set when the request's deadline passed while waiting for a
	// refresh: the writes are committed but may not be searchable yet.
	TimedOut bool
	// Percolated is set when WriteOptions.Percolate was asked and every upserted
	// document's Queries were filled. When it is not (a deadline passed waiting for
	// WaitForSeq, a copy closed), the writes are committed all the same.
	Percolated bool
}

// ItemResult is one op's outcome.
type ItemResult struct {
	// Seq is the op's seq when it committed.
	Seq int64
	// Err is why the op was refused (an *Error), nil when it committed.
	Err error
	// Queries are the ids of the saved queries the upserted document matches, with
	// WriteOptions.Percolate.
	Queries []string
}

// ReadOptions are a read's parameters.
type ReadOptions struct {
	// WaitForSeq waits until every change up to this seq is searchable on the
	// copies the read uses (read-your-writes).
	WaitForSeq int64
}

// Document is a stored document.
type Document struct {
	ID string `json:"id"`
	// Seq is the change that last wrote it; 0 (omitted) on a stale read.
	Seq  int64           `json:"seq,omitempty"`
	Body json.RawMessage `json:"body"`
	// Stale is set when the database could not be reached and the document was
	// read from this node's copy instead: it may trail the latest write.
	Stale bool `json:"stale,omitempty"`
}

// SavedQuery is a stored saved query.
type SavedQuery struct {
	ID    string          `json:"id"`
	Seq   int64           `json:"seq"`
	Query json.RawMessage `json:"query"`
	Meta  json.RawMessage `json:"meta"`
	// Stale is set when it was read from this node's copy because the database
	// could not be reached.
	Stale bool `json:"stale,omitempty"`
}

// SearchResult is a search's answer.
type SearchResult struct {
	*search.Response
	// Stale is set when the database cannot be reached: the copies serve what
	// they had, which may trail the latest writes (spec section 10).
	Stale bool
}

// PercolateResponse is a percolation's answer.
type PercolateResponse struct {
	Results []PercolateResult
	// Stale is as SearchResult's.
	Stale bool
}

// PercolateRequest are the documents to percolate: given in full, or by the ids of
// stored documents.
type PercolateRequest struct {
	Docs []json.RawMessage
	IDs  []string
}

// PercolateResult is one percolated document's matches. Results list Docs first,
// then IDs, each in request order.
type PercolateResult struct {
	// ID is the stored document's id (empty for a given document).
	ID string `json:"id,omitempty"`
	// Found is false for a stored id with no live document.
	Found   bool     `json:"found"`
	Queries []string `json:"queries"`
}

// FieldCatalog describes an index's fields.
type FieldCatalog struct {
	Dynamic schema.DynamicMode `json:"dynamic"`
	Fields  []FieldInfo        `json:"fields"`
}

// FieldInfo is one mapped field.
type FieldInfo struct {
	Name string           `json:"name"`
	Type schema.FieldType `json:"type"`
	// Entries are a keyword_list field's most frequent entries, by count.
	Entries []EntryCount `json:"entries,omitempty"`
}

// EntryCount is one list entry and the documents holding it.
type EntryCount struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

// Health statuses, as Elasticsearch defines them.
const (
	StatusGreen  = "green"
	StatusYellow = "yellow"
	StatusRed    = "red"
)

// ClusterHealth is the operator's view of the cluster.
type ClusterHealth struct {
	// Status is green (every shard copy serving current data), yellow (a shard
	// below its copy target, or a copy serving stale data) or red (a shard with no
	// serving copy).
	Status        string `json:"status"`
	Nodes         int    `json:"nodes"`
	Indexes       int    `json:"indexes"`
	Shards        int    `json:"shards"`
	ServingShards int    `json:"serving_shards"`
	// Unassigned counts shards with no serving copy.
	Unassigned int `json:"unassigned"`
}

// NodeInfo describes a node.
type NodeInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Version string `json:"version"`
	// Self marks the node answering.
	Self bool `json:"self"`
}

// Shard copy states.
const (
	ShardServing    = "serving"
	ShardRecovering = "recovering"
	ShardHalted     = "halted"
	// ShardRetiring is a cluster node's copy being shut down (a rolling restart):
	// it serves no reads, and a replacement may be placed meanwhile.
	ShardRetiring = "retiring"
)

// Drainer is implemented by a coordinator that prepares for shutdown when the API
// starts draining (the cluster node retires the copies others can stand in for).
type Drainer interface {
	Drain(ctx context.Context)
}

// ShardInfo describes one shard copy.
type ShardInfo struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
	Node  string `json:"node"`
	State string `json:"state"`
	// AppliedSeq, RefreshedSeq and CommittedSeq are what the copy has applied, made
	// searchable and made durable.
	AppliedSeq   int64 `json:"applied_seq"`
	RefreshedSeq int64 `json:"refreshed_seq"`
	CommittedSeq int64 `json:"committed_seq"`
	// Lag is how many seqs the copy trails the newest change this node knows of.
	Lag  int64  `json:"lag"`
	Docs uint64 `json:"docs"`
	// Error is why a halted copy stopped.
	Error string `json:"error,omitempty"`
	// Stale marks a serving copy whose reads are stale: it trails the changelog by
	// more than max_lag, or is being rebuilt aside (Rebuilding), serving its old
	// data until its replacement is swapped in.
	Stale      bool `json:"stale,omitempty"`
	Rebuilding bool `json:"rebuilding,omitempty"`
}

// strictDecode decodes one JSON value into v, refusing unknown keys and trailing data.
func strictDecode(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after the JSON value")
	}
	return nil
}
