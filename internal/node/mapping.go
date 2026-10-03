package node

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/store"
)

// DefaultMaxIndexFields is an index's field limit when max_index_fields is unset, as
// Elasticsearch's index.mapping.total_fields.limit.
const DefaultMaxIndexFields = 1000

// errReanalyze tells a writer to analyze its documents again under the current
// mapping: a field it typed was meanwhile given another type, or the fields it adds
// do not all fit under the field limit next to fields others added.
var errReanalyze = errors.New("node: analyze the write again under the current mapping")

// catalogLock serializes changes to an index's catalogue entry. It is a one-slot
// semaphore, so waiting for it honours a request's deadline.
type catalogLock chan struct{}

func (l catalogLock) lock(ctx context.Context) error {
	select {
	case l <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l catalogLock) unlock() { <-l }

// fieldsBatch queues the new dynamic fields of concurrent writes, so that one
// catalogue update stores them all (a group commit of mapping updates).
type fieldsBatch struct {
	mu      sync.Mutex
	pending []*fieldsRequest
}

// fieldsRequest is one write's new fields and where its outcome goes.
type fieldsRequest struct {
	fields map[string]schema.FieldType
	done   chan error // buffered: the flush never blocks on a writer that left
}

// maxIndexFields is the index field limit.
func (n *Single) maxIndexFields() int {
	if n.cfg.MaxIndexFields > 0 {
		return n.cfg.MaxIndexFields
	}
	return DefaultMaxIndexFields
}

// ensureFields makes the stored mapping hold fields, the new dynamic fields a
// write's documents bring, before the write commits. Fields the mapping already
// holds cost nothing. Otherwise the request joins the index's queue: the writer that
// takes the catalogue lock stores every queued request's fields in one version-checked
// update, and the others wait for its outcome (or their deadline). It returns
// errReanalyze when a field has another type now, or no longer fits the field limit.
func (n *Single) ensureFields(ctx context.Context, idx *index, fields map[string]schema.FieldType) error {
	switch missing, conflict := diffFields(idx.meta.Load().mapping, fields); {
	case conflict:
		return errReanalyze
	case !missing:
		return nil
	}
	req := &fieldsRequest{fields: fields, done: make(chan error, 1)}
	idx.fields.mu.Lock()
	idx.fields.pending = append(idx.fields.pending, req)
	idx.fields.mu.Unlock()
	select {
	case idx.catalog <- struct{}{}:
		n.flushFields(ctx, idx)
		idx.catalog.unlock()
		return <-req.done // served by this flush, or by an earlier one
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// diffFields reports whether m lacks one of fields, and whether it types one of them
// otherwise.
func diffFields(m *schema.Mapping, fields map[string]schema.FieldType) (missing, conflict bool) {
	for name, t := range fields {
		have, ok := m.Fields[name]
		switch {
		case !ok:
			missing = true
		case have != t:
			return missing, true
		}
	}
	return missing, false
}

// catalogUpdateTimeout bounds one flush's catalogue update, apart from the deadline
// of the writer that happens to lead it: the others wait on it too.
const catalogUpdateTimeout = 30 * time.Second

// flushFields stores every queued request's fields in one catalogue update and
// answers each request. The catalogue lock is held.
func (n *Single) flushFields(ctx context.Context, idx *index) {
	idx.fields.mu.Lock()
	reqs := idx.fields.pending
	idx.fields.pending = nil
	idx.fields.mu.Unlock()
	if len(reqs) == 0 {
		return
	}
	uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), catalogUpdateTimeout)
	defer cancel()
	limit := n.maxIndexFields()
	for range maxCatalogTries {
		cur := idx.meta.Load()
		acc := cur.mapping
		results := make([]error, len(reqs))
		for i, r := range reqs {
			merged, err := acc.Merge(schema.MappingUpdate{Fields: r.fields})
			if err != nil || (len(merged.Fields) > limit && len(merged.Fields) > len(acc.Fields)) {
				results[i] = errReanalyze
				continue
			}
			acc = merged
		}
		if len(acc.Fields) > len(cur.mapping.Fields) {
			_, err := n.storeCatalogLocked(uctx, idx, cur, acc, cur.settings)
			if errors.Is(err, store.ErrConflict) {
				// Another process changed the entry: read it and merge again.
				if rerr := n.reloadCatalog(uctx, idx); rerr != nil {
					answerAll(reqs, results, rerr)
					return
				}
				continue
			}
			if err != nil {
				answerAll(reqs, results, err)
				return
			}
		}
		answerAll(reqs, results, nil)
		return
	}
	answerAll(reqs, make([]error, len(reqs)), api.Unavailable(store.ErrConflict, "the index's catalogue entry kept changing; retry"))
}

// answerAll answers each request with its own result, or err when it has none.
func answerAll(reqs []*fieldsRequest, results []error, err error) {
	for i, r := range reqs {
		if results[i] != nil {
			r.done <- results[i]
		} else {
			r.done <- err
		}
	}
}

// overFieldLimit reports the first field grown adds past the limit, or "".
func overFieldLimit(base, grown *schema.Mapping, limit int) string {
	if len(grown.Fields) <= limit || len(grown.Fields) <= len(base.Fields) {
		return ""
	}
	added := slices.Sorted(maps.Keys(added(base, grown)))
	if len(added) == 0 {
		return ""
	}
	return added[0]
}
