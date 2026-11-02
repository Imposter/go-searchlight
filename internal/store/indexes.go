package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// indexStore implements IndexStore over sl_indexes.
type indexStore struct{ s *sqlStore }

// newIndexUID mints a fresh incarnation id for a created or recreated index.
func newIndexUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func validMeta(m *IndexMeta) error {
	if err := validKey("index name", m.Name, MaxIndexName); err != nil {
		return err
	}
	if len(m.Mapping) == 0 {
		m.Mapping = []byte("{}")
	}
	if len(m.Settings) == 0 {
		m.Settings = []byte("{}")
	}
	if err := validJSON("mapping", m.Mapping); err != nil {
		return err
	}
	if err := validJSON("settings", m.Settings); err != nil {
		return err
	}
	_, err := m.Shards()
	return err
}

// Create implements [IndexStore]. It inserts the index and a sequence row for each of
// its shards in one transaction, every shard starting at the floor: above any seq a
// dropped index of the same name reached. While shard sequences are off it inserts
// the index alone, under the global sequence's lock, so it commits wholly before or
// after they come on, and they seed its rows. An insert that returns its row in the same
// statement answers with that row; a failed insert is told apart from an existing
// index by looking the name up, since a key violation looks different on every
// dialect.
func (x *indexStore) Create(ctx context.Context, m IndexMeta) (out IndexMeta, err error) {
	s := x.s
	ctx, end := s.start(ctx, "index_create", attribute.String("index", m.Name))
	defer end(&err)
	if err := validMeta(&m); err != nil {
		return out, err
	}
	uid, err := newIndexUID()
	if err != nil {
		return out, err
	}
	out, err = x.createOnce(ctx, &m, uid)
	if err == nil {
		return out, nil
	}
	if _, gerr := x.get(ctx, m.Name); gerr == nil {
		return IndexMeta{}, fmt.Errorf("index %q: %w", m.Name, ErrExists)
	}
	return IndexMeta{}, err
}

func (x *indexStore) createOnce(ctx context.Context, m *IndexMeta, uid string) (IndexMeta, error) {
	s := x.s
	shards, err := m.Shards()
	if err != nil {
		return IndexMeta{}, err
	}
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return IndexMeta{}, err
	}
	defer rollback(tx)
	global := !s.seqsOn.Load()
	if global {
		if _, _, err := s.lockGlobal(ctx, tx); errors.Is(err, errShardSeqsOn) {
			rollback(tx)
			return x.createOnce(ctx, m, uid)
		} else if err != nil {
			return IndexMeta{}, err
		}
		shards = 0
	}
	row, err := returningRow(ctx, tx, s.d.Indexes.Create, []any{m.Name, string(m.Mapping), string(m.Settings), uid}, []any{m.Name})
	if err != nil {
		return IndexMeta{}, err
	}
	out, err := scanIndex(row, m.Name)
	if err != nil {
		return IndexMeta{}, err
	}
	for sh := range shards {
		if _, err := tx.ExecContext(ctx, s.d.Changelog.SeedShard, m.Name, sh, uid); err != nil {
			return IndexMeta{}, fmt.Errorf("seed shard %d: %w", sh, err)
		}
	}
	return out, tx.Commit()
}

// scanIndex reads the dialect's IndexColumns.
func scanIndex(row interface{ Scan(...any) error }, name string) (IndexMeta, error) {
	m := IndexMeta{Name: name}
	var created int64
	err := row.Scan(&m.Mapping, &m.Settings, &m.Version, &created, &m.UID, &m.MappingVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return m, fmt.Errorf("index %q: %w", name, ErrNotFound)
	}
	m.CreatedAt = millis(created)
	return m, err
}

func (x *indexStore) get(ctx context.Context, name string) (IndexMeta, error) {
	return scanIndex(x.s.r.QueryRowContext(ctx, x.s.d.Indexes.Get, name), name)
}

func (x *indexStore) Get(ctx context.Context, name string) (m IndexMeta, err error) {
	ctx, end := x.s.start(ctx, "index_get", attribute.String("index", name))
	defer end(&err)
	return x.get(ctx, name)
}

func (x *indexStore) List(ctx context.Context) (out []IndexMeta, err error) {
	s := x.s
	ctx, end := s.start(ctx, "index_list")
	defer end(&err)
	rows, err := s.r.QueryContext(ctx, s.d.Indexes.List)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var m IndexMeta
		var created int64
		if err := rows.Scan(&m.Name, &m.Mapping, &m.Settings, &m.Version, &created, &m.UID, &m.MappingVersion); err != nil {
			return nil, err
		}
		m.CreatedAt = millis(created)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (x *indexStore) Update(ctx context.Context, m IndexMeta) (out IndexMeta, err error) {
	s := x.s
	ctx, end := s.start(ctx, "index_update", attribute.String("index", m.Name))
	defer end(&err)
	if err := validMeta(&m); err != nil {
		return out, err
	}
	if s.closed.Load() {
		return out, ErrClosed
	}
	for attempt := 1; ; attempt++ {
		err = x.updateOnce(ctx, &m)
		if err == nil || attempt == applyAttempts || !s.retryable(err) && !errors.Is(err, errShardSeqsOn) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return out, err
	}
	return x.get(ctx, m.Name)
}

// indexShard is one of an index's sequence rows, locked.
type indexShard struct {
	shard int
	head  shardHead
}

// lockIndex locks every sequence row of the index name, in shard order, which is key
// order: an Apply holding some of them finishes first, and one waiting for one waits
// until this transaction ends. It returns them with the database clock as it read
// them.
func (s *sqlStore) lockIndex(ctx context.Context, tx *sql.Tx, name string) ([]indexShard, int64, error) {
	rows, err := tx.QueryContext(ctx, s.d.Changelog.LockIndexShards, name)
	if err != nil {
		return nil, 0, fmt.Errorf("lock index %q: %w", name, err)
	}
	defer rows.Close()
	var out []indexShard
	var nowMs int64
	for rows.Next() {
		var r indexShard
		if err := rows.Scan(&r.shard, &r.head.value, &r.head.uid, &r.head.mappingVersion, &nowMs); err != nil {
			return nil, 0, err
		}
		r.head.last = r.head.value
		out = append(out, r)
	}
	return out, nowMs, rows.Err()
}

// updateOnce runs one attempt of Update. It locks every sequence row of the index
// before it reads the index, so a mapping change commits wholly before or after any
// Apply to the index (every change committed after it carries its mapping version
// and follows it in its shard's log), and two updates of one index take turns. While
// shard sequences are off it locks the global sequence instead, as 1.x does, and
// logs the mapping changes at its next values. The
// shard count is fixed at creation (spec section 2): documents are placed by it, so
// changing it would orphan them.
func (x *indexStore) updateOnce(ctx context.Context, m *IndexMeta) error {
	s := x.s
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			rollback(tx)
		}
	}()
	var locked []indexShard
	var global *shardHead
	var nowMs int64
	if s.seqsOn.Load() {
		locked, nowMs, err = s.lockIndex(ctx, tx, m.Name)
	} else {
		global, nowMs, err = s.lockGlobal(ctx, tx)
	}
	if err != nil {
		return err
	}
	var oldMapping, oldSettings []byte
	var version, mappingVersion int64
	var uid string
	err = tx.QueryRowContext(ctx, s.d.Indexes.Current, m.Name).
		Scan(&oldMapping, &oldSettings, &version, &mappingVersion, &uid)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("index %q: %w", m.Name, ErrNotFound)
	}
	if err != nil {
		return err
	}
	if version != m.Version {
		return fmt.Errorf("index %q version %d: %w", m.Name, m.Version, ErrConflict)
	}
	oldShards, err := settingsShards(oldSettings)
	if err != nil {
		return err
	}
	if newShards, err := m.Shards(); err != nil {
		return err
	} else if newShards != oldShards {
		return invalidf("settings: shards is %d and cannot change (to %d)", oldShards, newShards)
	}
	if global != nil {
		for sh := range oldShards {
			locked = append(locked, indexShard{shard: sh, head: shardHead{value: global.value, last: global.value, global: global}})
		}
	}
	if len(locked) != oldShards {
		return fmt.Errorf("store: index %q has %d shards and %d sequence rows", m.Name, oldShards, len(locked))
	}
	remap := !bytes.Equal(oldMapping, m.Mapping)
	if remap {
		mappingVersion++
	}
	if _, err := tx.ExecContext(ctx, s.d.Indexes.Update, string(m.Mapping), string(m.Settings), mappingVersion, m.Name, m.Version); err != nil {
		return err
	}
	if !remap {
		return tx.Commit()
	}
	heads := make(map[ShardID]*shardHead, len(locked))
	w := &dialect.Write{Changes: make([]dialect.ChangeRow, len(locked))}
	for i := range locked {
		r := &locked[i]
		seqs := r.head.seqs()
		seqs.last++
		r.head.last = seqs.last
		r.head.mappingVersion = mappingVersion
		heads[ShardID{Index: m.Name, Shard: r.shard}] = &r.head
		w.Changes[i] = dialect.ChangeRow{
			Seq: r.head.last, Index: m.Name, Shard: r.shard, Kind: string(KindMapping), ID: MappingChangeID,
			Payload: string(m.Mapping), At: nowMs, IndexUID: uid, MappingVersion: mappingVersion, Nonce: newNonce(),
		}
	}
	w.Counters = counters(heads)
	if err := s.write(ctx, tx, w); err != nil {
		return fmt.Errorf("log mapping changes: %w", err)
	}
	if err := s.setGlobal(ctx, tx, global); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	s.announce(heads)
	return nil
}

// Drop implements [IndexStore]. It locks every sequence row of the index, which orders
// the drop with Apply: no change for the index commits after it. It raises the floor
// to the newest seq of any of the index's shards, so an index created under the name
// later starts above every seq this one took. While shard sequences are off it locks
// the global sequence instead, as 1.x does: the global sequence is above every seq
// already, and becomes the floor when they come on.
func (x *indexStore) Drop(ctx context.Context, name string) (err error) {
	s := x.s
	ctx, end := s.start(ctx, "index_drop", attribute.String("index", name))
	defer end(&err)
	if err := validKey("index name", name, MaxIndexName); err != nil {
		return err
	}
	err = x.dropOnce(ctx, name)
	if errors.Is(err, errShardSeqsOn) {
		err = x.dropOnce(ctx, name)
	}
	return err
}

func (x *indexStore) dropOnce(ctx context.Context, name string) error {
	s := x.s
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var locked []indexShard
	global := !s.seqsOn.Load()
	if global {
		_, _, err = s.lockGlobal(ctx, tx)
	} else {
		locked, _, err = s.lockIndex(ctx, tx, name)
	}
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, s.d.Indexes.Drop, name)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("index %q: %w", name, ErrNotFound)
	}
	if !global {
		var top int64
		for _, r := range locked {
			top = max(top, r.head.value)
		}
		if _, err := tx.ExecContext(ctx, s.d.Changelog.RaiseFloor, top); err != nil {
			return fmt.Errorf("drop index %q: %w", name, err)
		}
	}
	for _, q := range s.d.Indexes.DropData {
		if _, err := tx.ExecContext(ctx, q, name); err != nil {
			return fmt.Errorf("drop index %q: %w", name, err)
		}
	}
	return tx.Commit()
}
