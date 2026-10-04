package node

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"os"
	"slices"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/store"
)

// CreateIndex implements [api.Coordinator].
func (n *Single) CreateIndex(ctx context.Context, name string, spec api.IndexSpec) (*api.IndexInfo, error) {
	if e := api.ValidIndexName(name); e != nil {
		return nil, e
	}
	m := spec.Mapping
	if m == nil {
		m = &schema.Mapping{}
	}
	m = m.Clone()
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if limit := n.maxIndexFields(); len(m.Fields) > limit {
		return nil, api.InvalidAt("mapping.fields", "the mapping holds %d fields, over max_index_fields (%d)", len(m.Fields), limit)
	}
	if err := spec.Settings.Validate(); err != nil {
		return nil, err
	}
	mapping, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	settings, err := json.Marshal(spec.Settings)
	if err != nil {
		return nil, err
	}
	// The name is reserved under the node lock; the catalogue write and opening
	// the copies happen outside it, so other indexes are served meanwhile.
	n.mu.Lock()
	switch {
	case n.closed:
		n.mu.Unlock()
		return nil, api.Unavailable(store.ErrClosed, "the node is shutting down")
	case n.indexes[name] != nil || n.reserved[name]:
		n.mu.Unlock()
		return nil, api.Conflict(api.CodeIndexExists, "index %q already exists", name)
	}
	n.reserved[name] = true
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.reserved, name)
		n.mu.Unlock()
	}()

	meta, err := n.st.Indexes().Create(ctx, store.IndexMeta{Name: name, Mapping: mapping, Settings: settings})
	if errors.Is(err, store.ErrExists) {
		return nil, api.Conflict(api.CodeIndexExists, "index %q already exists", name)
	}
	if err != nil {
		return nil, storeError(err, name)
	}
	idx, err := n.openIndex(ctx, meta)
	if err != nil {
		// Leave no index the node cannot serve behind.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), n.dropTimeout())
		defer cancel()
		if derr := n.st.Indexes().Drop(dctx, name); derr != nil {
			n.log.ErrorContext(ctx, "dropping an index that failed to open failed", slog.String("index", name), slog.Any("error", derr))
		}
		return nil, err
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		_ = n.stopIndex(context.WithoutCancel(ctx), idx)
		return nil, api.Unavailable(store.ErrClosed, "the node is shutting down")
	}
	n.indexes[name] = idx
	n.mu.Unlock()
	if n.cl != nil {
		// This node claims its copies now, rather than at the allocator's next pass.
		if err := n.cl.Allocate(ctx, name); err != nil {
			n.log.WarnContext(ctx, "allocating a new index's copies failed; the allocator retries", slog.String("index", name), slog.Any("error", err))
		}
	}
	// A new copy recovers (from an empty snapshot) before it serves: answer once
	// it does, so the client's next request finds it serving.
	if err := n.waitServing(ctx, idx); err != nil {
		n.log.WarnContext(ctx, "a new index's copies are not serving yet", slog.String("index", name), slog.Any("error", err))
	}
	n.log.InfoContext(ctx, "index created", slog.String("index", name), slog.Int("shards", spec.Settings.Shards), slog.String("uid", meta.UID))
	return n.describe(ctx, idx), nil
}

// waitServing waits, under ctx and at most max_lag, until every copy of idx serves;
// a halted copy ends the wait at once.
func (n *Single) waitServing(ctx context.Context, idx *index) error {
	ctx, cancel := context.WithTimeout(ctx, max(n.cfg.MaxLag, time.Second))
	defer cancel()
	delay := time.Millisecond
	for {
		var err error
		for _, c := range idx.copies() {
			if err = c.notServing(); err != nil {
				break
			}
		}
		if err == nil {
			return nil
		}
		for _, c := range idx.copies() {
			if c.halted.Load() != nil || haltErr(c) != nil {
				return err
			}
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
		delay = min(2*delay, 20*time.Millisecond)
	}
}

// dropTimeout bounds dropping an index from the store.
func (n *Single) dropTimeout() time.Duration {
	if n.cfg.DropTimeout > 0 {
		return n.cfg.DropTimeout
	}
	return 10 * time.Minute
}

// ListIndexes implements [api.Coordinator].
func (n *Single) ListIndexes(ctx context.Context) ([]*api.IndexInfo, error) {
	n.mu.RLock()
	list := slices.Collect(maps.Values(n.indexes))
	n.mu.RUnlock()
	slices.SortFunc(list, func(a, b *index) int {
		switch {
		case a.name < b.name:
			return -1
		case a.name > b.name:
			return 1
		}
		return 0
	})
	out := make([]*api.IndexInfo, len(list))
	for i, idx := range list {
		out[i] = n.describe(ctx, idx)
	}
	return out, nil
}

// GetIndex implements [api.Coordinator].
func (n *Single) GetIndex(ctx context.Context, name string) (*api.IndexInfo, error) {
	idx, err := n.lookup(ctx, name)
	if err != nil {
		return nil, err
	}
	return n.describe(ctx, idx), nil
}

// describe builds an index's description, counting what its copies hold searchable:
// this node's, and on a cluster node a serving copy elsewhere of each shard it does
// not host (a shard no copy answers for is not counted).
func (n *Single) describe(ctx context.Context, idx *index) *api.IndexInfo {
	st := idx.meta.Load()
	info := &api.IndexInfo{
		Name:      st.meta.Name,
		UID:       st.meta.UID,
		Version:   st.meta.Version,
		CreatedAt: st.meta.CreatedAt,
		Mapping:   st.mapping,
		Settings:  st.settings,
	}
	for _, sl := range idx.shards {
		if c := sl.local.Load(); c != nil {
			if sh := c.shard(); sh != nil {
				if g := sh.Acquire(); g != nil {
					info.Docs += g.NumDocs()
					info.Queries += g.NumQueries()
					g.Release()
					continue
				}
			}
		}
		if n.cl != nil {
			if docs, queries, err := n.cl.Counts(ctx, sl.id); err == nil {
				info.Docs += docs
				info.Queries += queries
			}
		}
	}
	return info
}

// DeleteIndex implements [api.Coordinator]: the index is dropped from the store with
// its documents, saved queries and changes, then its copies are closed and removed.
func (n *Single) DeleteIndex(ctx context.Context, name string) error {
	n.mu.Lock()
	idx := n.indexes[name]
	if n.closed {
		n.mu.Unlock()
		return api.Unavailable(store.ErrClosed, "the node is shutting down")
	}
	if idx == nil {
		n.mu.Unlock()
		return indexNotFound(name)
	}
	// Marked first, so a tailer that sees the drop before it is stopped is not
	// taken for a halted copy, and out of the map, so the index answers 404 while
	// the store deletes it; the delete itself runs outside the node lock, under
	// its own deadline (drop_timeout), so a big index neither blocks other
	// indexes nor fails at the request's deadline.
	idx.dropped.Store(true)
	delete(n.indexes, name)
	n.reserved[name] = true
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.reserved, name)
		n.mu.Unlock()
	}()

	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), n.dropTimeout())
	defer cancel()
	if err := n.st.Indexes().Drop(sctx, name); err != nil {
		n.mu.Lock()
		idx.dropped.Store(false)
		if !n.closed {
			n.indexes[name] = idx
		}
		n.mu.Unlock()
		return storeError(err, name)
	}
	if err := n.stopIndex(sctx, idx); err != nil {
		n.log.WarnContext(ctx, "closing a dropped index's copies failed", slog.String("index", name), slog.Any("error", err))
	}
	if err := os.RemoveAll(idx.dir); err != nil {
		// Readers may still map a segment (Windows refuses to remove it): the next
		// start removes what is left.
		n.log.WarnContext(ctx, "removing a dropped index's copies failed", slog.String("index", name), slog.Any("error", err))
	}
	n.log.InfoContext(ctx, "index dropped", slog.String("index", name))
	return nil
}

// maxCatalogTries bounds the retries of a version-checked catalogue update.
const maxCatalogTries = 8

// updateCatalog applies change to the index's catalogue entry under its lock, with
// the store's version check, re-reading the entry and retrying when another writer got
// there first. change returns the new mapping and settings, or an error to give up.
func (n *Single) updateCatalog(ctx context.Context, idx *index, change func(st *indexState) (*schema.Mapping, api.IndexSettings, error)) (*indexState, error) {
	if err := idx.catalog.lock(ctx); err != nil {
		return nil, err
	}
	defer idx.catalog.unlock()
	for range maxCatalogTries {
		cur := idx.meta.Load()
		m, settings, err := change(cur)
		if err != nil {
			return nil, err
		}
		if m == nil {
			return cur, nil // nothing to change
		}
		next, err := n.storeCatalogLocked(ctx, idx, cur, m, settings)
		if errors.Is(err, store.ErrConflict) {
			if err := n.reloadCatalog(ctx, idx); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		return next, nil
	}
	return nil, api.Unavailable(store.ErrConflict, "the index's catalogue entry kept changing; retry")
}

// storeCatalogLocked writes mapping m and settings over cur, the index's entry, with
// the store's version check, and adopts the result. A version conflict is returned as
// it is (store.ErrConflict), for the caller to re-read and retry. The catalogue lock
// is held.
func (n *Single) storeCatalogLocked(ctx context.Context, idx *index, cur *indexState, m *schema.Mapping, settings api.IndexSettings) (*indexState, error) {
	// An unchanged mapping keeps its stored bytes exactly: the store logs a
	// mapping change (and copies may rebuild) whenever the bytes differ.
	mapping := cur.meta.Mapping
	if m != cur.mapping {
		var err error
		if mapping, err = json.Marshal(m); err != nil {
			return nil, err
		}
	}
	raw, err := json.Marshal(settings)
	if err != nil {
		return nil, err
	}
	meta := cur.meta
	meta.Mapping, meta.Settings = mapping, raw
	updated, err := n.st.Indexes().Update(ctx, meta)
	if errors.Is(err, store.ErrConflict) {
		return nil, err
	}
	if errors.Is(err, store.ErrInvalid) {
		// The only settings the store refuses to change: the shard count.
		return nil, api.InvalidAt("settings.shards", "%v", err)
	}
	if err != nil {
		return nil, storeError(err, idx.name)
	}
	// The entry as stored, with its new version and mapping version.
	next := &indexState{meta: updated, mapping: m, settings: settings}
	idx.meta.Store(next)
	return next, nil
}

// reloadCatalog re-reads the index's catalogue entry.
func (n *Single) reloadCatalog(ctx context.Context, idx *index) error {
	meta, err := n.st.Indexes().Get(ctx, idx.name)
	if err != nil {
		return storeError(err, idx.name)
	}
	if meta.UID != idx.meta.Load().meta.UID {
		return api.Conflict(api.CodeConflict, "index %q was dropped and recreated meanwhile", idx.name)
	}
	st, err := parseState(meta)
	if err != nil {
		return err
	}
	idx.meta.Store(st)
	n.adoptSettings(idx, st.settings)
	return nil
}

// PatchMapping implements [api.Coordinator]: fields are added; a field that exists
// with another type is a 400. The change goes through the store's Update, which logs
// it to every shard's changelog; a copy holding a live document with a field the
// change maps (a dynamic false index keeps such fields unindexed) is rebuilt at that
// seq, so tailed and rebuilt copies agree, and the document becomes searchable on
// the field.
func (n *Single) PatchMapping(ctx context.Context, name string, fields map[string]schema.FieldType) (*api.IndexInfo, error) {
	idx, err := n.lookup(ctx, name)
	if err != nil {
		return nil, err
	}
	_, err = n.updateCatalog(ctx, idx, func(st *indexState) (*schema.Mapping, api.IndexSettings, error) {
		merged, err := st.mapping.Merge(schema.MappingUpdate{Fields: fields})
		if err == nil {
			if field := overFieldLimit(st.mapping, merged, n.maxIndexFields()); field != "" {
				return nil, st.settings, api.InvalidAt("fields."+field, "the index would hold %d fields, over max_index_fields (%d)", len(merged.Fields), n.maxIndexFields())
			}
		}
		if err != nil {
			var ve *schema.ValidationError
			if errors.As(err, &ve) {
				return nil, st.settings, api.InvalidAt("fields."+ve.Field, "%s", ve.Message)
			}
			return nil, st.settings, err
		}
		if len(merged.Fields) == len(st.mapping.Fields) {
			return nil, st.settings, nil
		}
		return merged, st.settings, nil
	})
	if err != nil {
		return nil, err
	}
	return n.describe(ctx, idx), nil
}

// PatchSettings implements [api.Coordinator]. A new refresh interval applies at once.
func (n *Single) PatchSettings(ctx context.Context, name string, patch api.SettingsPatch) (*api.IndexInfo, error) {
	idx, err := n.lookup(ctx, name)
	if err != nil {
		return nil, err
	}
	st, err := n.updateCatalog(ctx, idx, func(st *indexState) (*schema.Mapping, api.IndexSettings, error) {
		s := st.settings
		if patch.RefreshInterval != nil {
			s.RefreshInterval = *patch.RefreshInterval
		}
		if patch.ReplicasPerShard != nil {
			s.ReplicasPerShard = *patch.ReplicasPerShard
		}
		if err := s.Validate(); err != nil {
			return nil, s, err
		}
		return st.mapping, s, nil
	})
	if err != nil {
		return nil, err
	}
	n.adoptSettings(idx, st.settings)
	return n.describe(ctx, idx), nil
}

// adoptSettings applies an index's settings to this node: its refresh interval.
func (n *Single) adoptSettings(idx *index, s api.IndexSettings) {
	if idx.refresh.Swap(int64(n.resolvedRefresh(s))) != int64(n.resolvedRefresh(s)) {
		select {
		case idx.refreshWake <- struct{}{}:
		default:
		}
	}
}
