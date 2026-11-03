package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
)

// The rolling upgrade to shard sequences (schema v7). A node from before them (1.x)
// takes every seq from one global sequence, sl_counter row 1, under its lock: it reads
// changes by seq alone, creates an index without sequence rows, and drops or remaps
// one without touching them. Migration 0007 therefore leaves sl_shard_seq empty, and
// until the cluster turns shard sequences on, this binary commits the way 1.x does:
// under row 1's lock, at row 1's next values, with the index's incarnation and mapping
// version read from sl_indexes. Every seq is then unique and every commit's seqs
// follow the one before, whichever version made it, and a shard with no sequence row
// reads its head from row 1, as 1.x does.
//
// The cluster's leader turns them on (EnableShardSeqs) once every live node runs this
// binary, in one transaction holding row 1's lock: it seeds a sequence row for every
// shard of every index sl_indexes holds, at row 1's value, with the index's current
// incarnation and mapping version (whatever 1.x created, dropped or remapped
// meanwhile), moves the floor to row 3 at that value, and deletes row 1. A 1.x node
// still running then fails every write and every read of the head, since each needs
// row 1, rather than writing outside the shard sequences; and it cannot start again
// (the schema is newer than it knows). Nodes of this binary learn of the switch from
// the missing row at their next write. Heads never move back: every shard stands at
// row 1's value at the switch, at or above anything read before it.

// FeatureShardSeqs is the sl_features row EnableShardSeqs records.
const FeatureShardSeqs = "shard_seqs"

// SeqScheme is how this binary's commits take their seqs, as a level its nodes
// heartbeat (sl_nodes.seq_scheme): 1 from shard sequences once they are on. A node
// from before them is at 0.
const SeqScheme = 1

// errShardSeqsOn is a write that found the global sequence gone: shard sequences are
// on, and the write runs again on them.
var errShardSeqsOn = errors.New("store: shard sequences are on")

// errShardSeqsOff is a write that found the floor gone where it expected shard
// sequences on: the database was restored to before they came on. The store forgets
// that it saw them on, and the write runs again on the global sequence.
var errShardSeqsOff = errors.New("store: shard sequences are off")

// SeedMissingShards implements [Store].
func (s *sqlStore) SeedMissingShards(ctx context.Context) (err error) {
	ctx, end := s.start(ctx, "seed_missing_shards")
	defer end(&err)
	if s.closed.Load() {
		return ErrClosed
	}
	on, err := s.ShardSeqs(ctx)
	if err != nil || !on {
		return err
	}
	return s.seedMissingShards(ctx)
}

// ShardSeqs implements [Store]. Once it has seen them on it answers from memory.
func (s *sqlStore) ShardSeqs(ctx context.Context) (on bool, err error) {
	if s.seqsOn.Load() {
		return true, nil
	}
	var n int
	if err := s.r.QueryRowContext(ctx, s.d.Maintenance.GlobalRows).Scan(&n); err != nil {
		return false, err
	}
	if n == 0 {
		s.seqsOn.Store(true)
	}
	return n == 0, nil
}

// lockGlobal locks the global sequence in tx and returns it as a head, with the
// database clock. It fails with errShardSeqsOn once its row is gone.
func (s *sqlStore) lockGlobal(ctx context.Context, tx *sql.Tx) (*shardHead, int64, error) {
	g := &shardHead{}
	var nowMs int64
	err := tx.QueryRowContext(ctx, s.d.Changelog.LockGlobal).Scan(&g.value, &nowMs)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		s.seqsOn.Store(true)
		return nil, 0, errShardSeqsOn
	case err != nil:
		return nil, 0, fmt.Errorf("lock the global sequence: %w", err)
	}
	g.last = g.value
	return g, nowMs, nil
}

// lockGlobalShards is lockShards while shard sequences are off: it locks the global
// sequence, and gives each shard batch names a head that takes its seqs from it, with
// its index's incarnation and mapping version as sl_indexes holds them under the lock
// (which 1.x's Update and Drop take too).
func (s *sqlStore) lockGlobalShards(ctx context.Context, tx *sql.Tx, batch []Change) (map[ShardID]*shardHead, int64, error) {
	g, nowMs, err := s.lockGlobal(ctx, tx)
	if err != nil {
		return nil, 0, err
	}
	type state struct {
		uid    string
		mv     int64
		shards int // 0: the index does not exist
	}
	indexes := map[string]state{}
	heads := make(map[ShardID]*shardHead, 1)
	missing := false
	for i := range batch {
		id := batch[i].ShardID()
		if _, ok := heads[id]; ok {
			continue
		}
		st, read := indexes[id.Index]
		if !read {
			var mapping, settings []byte
			var version int64
			err := tx.QueryRowContext(ctx, s.d.Indexes.Current, id.Index).Scan(&mapping, &settings, &version, &st.mv, &st.uid)
			switch {
			case errors.Is(err, sql.ErrNoRows):
			case err != nil:
				return nil, 0, err
			default:
				if st.shards, err = settingsShards(settings); err != nil {
					return nil, 0, err
				}
			}
			indexes[id.Index] = st
		}
		if id.Shard >= st.shards {
			heads[id] = nil
			missing = true
			continue
		}
		heads[id] = &shardHead{value: g.value, last: g.value, uid: st.uid, mappingVersion: st.mv, global: g}
	}
	if err := s.checkShards(ctx, tx, batch, heads, missing); err != nil {
		return nil, 0, err
	}
	return heads, nowMs, nil
}

// setGlobal moves the global sequence on to the newest seq taken from it, if any.
func (s *sqlStore) setGlobal(ctx context.Context, tx *sql.Tx, g *shardHead) error {
	if g == nil || g.last == g.value {
		return nil
	}
	if _, err := tx.ExecContext(ctx, s.d.Changelog.SetGlobal, g.last); err != nil {
		return fmt.Errorf("move the global sequence on: %w", err)
	}
	return nil
}

// globalOf is the global sequence heads take their seqs from, nil when they have their
// own.
func globalOf(heads map[ShardID]*shardHead) *shardHead {
	for _, h := range heads {
		if h != nil {
			return h.global
		}
	}
	return nil
}

// EnableShardSeqs implements [Store].
func (s *sqlStore) EnableShardSeqs(ctx context.Context) (err error) {
	ctx, end := s.start(ctx, "enable_shard_seqs")
	defer end(&err)
	if s.closed.Load() {
		return ErrClosed
	}
	for attempt := 1; ; attempt++ {
		err = s.enableShardSeqs(ctx)
		if err == nil || attempt == applyAttempts || !s.retryable(err) || ctx.Err() != nil {
			return err
		}
	}
}

func (s *sqlStore) enableShardSeqs(ctx context.Context) error {
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	g, _, err := s.lockGlobal(ctx, tx)
	if errors.Is(err, errShardSeqsOn) {
		if _, err := tx.ExecContext(ctx, s.d.Registry.EnableFeature, FeatureShardSeqs); err != nil {
			return err
		}
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, s.d.Indexes.List)
	if err != nil {
		return err
	}
	type index struct {
		name, uid string
		settings  []byte
		mv        int64
	}
	var indexes []index
	for rows.Next() {
		var x index
		var mapping []byte
		var version, created int64
		if err := rows.Scan(&x.name, &mapping, &x.settings, &version, &created, &x.uid, &x.mv); err != nil {
			rows.Close()
			return err
		}
		indexes = append(indexes, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, s.d.Changelog.ClearShardSeqs); err != nil {
		return fmt.Errorf("clear the sequence rows: %w", err)
	}
	seeded := 0
	for _, x := range indexes {
		shards, err := settingsShards(x.settings)
		if err != nil {
			return fmt.Errorf("index %q: %w", x.name, err)
		}
		for sh := range shards {
			if _, err := tx.ExecContext(ctx, s.d.Changelog.InsertShardSeq, x.name, sh, g.value, x.uid, x.mv); err != nil {
				return fmt.Errorf("seed %s/%d: %w", x.name, sh, err)
			}
		}
		seeded += shards
	}
	if _, err := tx.ExecContext(ctx, s.d.Changelog.SeedFloor, g.value); err != nil {
		return fmt.Errorf("seed the floor: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.d.Changelog.RetireGlobal); err != nil {
		return fmt.Errorf("retire the global sequence: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.d.Registry.EnableFeature, FeatureShardSeqs); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	s.seqsOn.Store(true)
	s.log.InfoContext(ctx, "shard sequences are on", slog.Int64("seq", g.value),
		slog.Int("indexes", len(indexes)), slog.Int("shards", seeded))
	return nil
}

// seedMissingShards gives every index with no sequence row at all a row per shard at
// the floor, and fails naming any index with some of its rows but not all. Only an
// index a 1.x node created after shard sequences came on lacks them all: that node
// could create it (1.x's Create takes no lock) but write nothing to it.
func (s *sqlStore) seedMissingShards(ctx context.Context) error {
	rows, err := s.r.QueryContext(ctx, s.d.Maintenance.ShardRows)
	if err != nil {
		return fmt.Errorf("check the shards' sequence rows: %w", err)
	}
	var bad, bare []string
	for rows.Next() {
		var name string
		var settings []byte
		var n int
		if err := rows.Scan(&name, &settings, &n); err != nil {
			rows.Close()
			return err
		}
		shards, err := settingsShards(settings)
		if err != nil {
			rows.Close()
			return fmt.Errorf("index %q: %w", name, err)
		}
		switch {
		case n == 0:
			bare = append(bare, name)
		case n != shards:
			bad = append(bad, fmt.Sprintf("%q has %d shard(s) and %d sequence row(s)", name, shards, n))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(bad) > 0 {
		slices.Sort(bad)
		return fmt.Errorf("store: the shards' sequence rows do not match their indexes: %s", strings.Join(bad, "; "))
	}
	for _, name := range bare {
		if err := s.seedIndex(ctx, name); err != nil {
			return fmt.Errorf("seed the sequence rows of index %q: %w", name, err)
		}
	}
	return nil
}

// seedIndex seeds the sequence rows of an index that has none, under the floor's
// lock, at the floor, with its index's incarnation and mapping version. It does nothing
// to an index that has some, or is gone. With no floor, shard sequences are off (the
// database was restored to before they came on): it forgets having seen them on and
// fails with errShardSeqsOff.
func (s *sqlStore) seedIndex(ctx context.Context, name string) error {
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var floor int64
	switch err := tx.QueryRowContext(ctx, s.d.Changelog.LockFloor).Scan(&floor); {
	case errors.Is(err, sql.ErrNoRows):
		s.seqsOn.Store(false)
		return errShardSeqsOff
	case err != nil:
		return fmt.Errorf("lock the floor: %w", err)
	}
	var mapping, settings []byte
	var version, mv int64
	var uid string
	switch err := tx.QueryRowContext(ctx, s.d.Indexes.Current, name).Scan(&mapping, &settings, &version, &mv, &uid); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	var have int
	if err := tx.QueryRowContext(ctx, s.d.Maintenance.IndexShardRows, name).Scan(&have); err != nil {
		return err
	}
	if have > 0 {
		return nil
	}
	shards, err := settingsShards(settings)
	if err != nil {
		return err
	}
	for sh := range shards {
		if _, err := tx.ExecContext(ctx, s.d.Changelog.InsertShardSeq, name, sh, floor, uid, mv); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.log.WarnContext(ctx, "seeded the sequence rows of an index created without them (by a node from before shard sequences)",
		slog.String("index", name), slog.Int("shards", shards))
	return nil
}
