package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// migration is one embedded NNNN_name.sql file, with the statements of its
// NNNN_name.concurrent.sql companion, if it has one: those run before it, outside any
// transaction, once every migration before it has committed (dialects with
// Maintenance.ConcurrentLock only). They build what would otherwise hold a table's
// lock for as long as it takes, such as an index built CONCURRENTLY, and must be safe to
// run again: a migration interrupted between them and its commit runs them again.
type migration struct {
	version    int
	name       string
	stmts      []string
	concurrent []string
}

// concurrentSuffix names a migration's companion of statements run outside a
// transaction.
const concurrentSuffix = ".concurrent.sql"

// loadMigrations reads a dialect's migrations in version order.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, err
	}
	out := make([]migration, 0, len(names))
	seen := make(map[int]string, len(names))
	concurrent := map[string][]string{}
	for _, name := range names {
		if base, ok := strings.CutSuffix(path.Base(name), concurrentSuffix); ok {
			body, err := fs.ReadFile(fsys, name)
			if err != nil {
				return nil, err
			}
			concurrent[base] = splitStatements(string(body))
			continue
		}
		base := strings.TrimSuffix(path.Base(name), ".sql")
		num, _, ok := strings.Cut(base, "_")
		v, err := strconv.Atoi(num)
		if !ok || err != nil || v <= 0 {
			return nil, fmt.Errorf("migration %s: name must be NNNN_description.sql", name)
		}
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("migrations %s and %s share version %d", prev, name, v)
		}
		seen[v] = name
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, err
		}
		out = append(out, migration{version: v, name: base, stmts: splitStatements(string(body))})
	}
	for i := range out {
		if st, ok := concurrent[out[i].name]; ok {
			out[i].concurrent = st
			delete(concurrent, out[i].name)
		}
	}
	for base := range concurrent {
		return nil, fmt.Errorf("migration %s%s has no migration %s.sql", base, concurrentSuffix, base)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// splitStatements splits a migration on semicolons that end a line, after
// dropping -- comment lines.
func splitStatements(body string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "--") {
			continue
		}
		if strings.HasSuffix(t, ";") {
			cur.WriteString(strings.TrimSuffix(line, ";"))
			if st := strings.TrimSpace(cur.String()); st != "" {
				out = append(out, st)
			}
			cur.Reset()
			continue
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
	}
	if st := strings.TrimSpace(cur.String()); st != "" {
		out = append(out, st)
	}
	return out
}

// execer is what migrations run on: a transaction or a connection.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Migration retries: a migration whose DDL timed out waiting for a table lock
// (Maintenance.MigrateLockTimeout, 1 s, which is as long as it holds the table's other
// queries behind it) is tried again, up to migrateAttempts times, after a pause
// growing from migrateRetryBase to migrateRetryCap: about three minutes in all.
const (
	migrateAttempts  = 40
	migrateRetryBase = 500 * time.Millisecond
	migrateRetryCap  = 5 * time.Second
)

func (s *sqlStore) Migrate(ctx context.Context) (err error) {
	ctx, end := s.start(ctx, "migrate")
	defer end(&err)
	if s.closed.Load() {
		return ErrClosed
	}
	ms, err := loadMigrations(s.d.Migrations)
	if err != nil {
		return err
	}
	if s.schema > 0 {
		ms = slices.DeleteFunc(ms, func(m migration) bool { return m.version > s.schema })
	}
	timedOut := s.d.Maintenance.LockTimedOut
	wait := migrateRetryBase
	for attempt := 1; ; attempt++ {
		var created bool
		err = s.migrateOnce(ctx, ms, &created)
		if err == nil {
			err = s.afterMigrate(ctx, ms, created)
		}
		if err == nil || timedOut == nil || !timedOut(err) || attempt == migrateAttempts {
			return err
		}
		s.log.WarnContext(ctx, "a migration timed out waiting for a table lock that queries hold; retrying",
			slog.Int("attempt", attempt), slog.Duration("wait", wait), slog.Any("error", err))
		if err := s.clock.Sleep(ctx, wait); err != nil {
			return err
		}
		wait = min(2*wait, migrateRetryCap)
	}
}

// shardSeqSchema is the schema version that brings shard sequences (upgrade.go).
const shardSeqSchema = 7

// afterMigrate turns shard sequences on for a database this call created, which no node
// from before them can be writing (one that had migrated it first would have made it
// not new; one that comes after finds its schema too new and stops). On a database
// with shard sequences on, it seeds the rows of an index a 1.x node created without
// them, and fails naming an index whose rows do not match its shard count.
func (s *sqlStore) afterMigrate(ctx context.Context, ms []migration, created bool) error {
	if len(ms) == 0 || ms[len(ms)-1].version < shardSeqSchema {
		return nil
	}
	if created {
		if err := s.EnableShardSeqs(ctx); err != nil {
			return fmt.Errorf("turn shard sequences on: %w", err)
		}
	}
	on, err := s.ShardSeqs(ctx)
	if err != nil || !on {
		return err
	}
	return s.seedMissingShards(ctx)
}

// migrateOnce applies the pending migrations under the migration lock. A connection
// whose lock timeout cannot be reset is not put back into the pool.
func (s *sqlStore) migrateOnce(ctx context.Context, ms []migration, created *bool) error {
	m := &s.d.Maintenance
	if m.MigrateInTx && m.ConcurrentLock != "" && slices.ContainsFunc(ms, func(x migration) bool { return len(x.concurrent) > 0 }) {
		return s.migrateInSteps(ctx, ms, created)
	}
	if m.MigrateInTx {
		tx, err := s.w.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer rollback(tx)
		if m.MigrateLock != "" {
			if _, err := tx.ExecContext(ctx, m.MigrateLock); err != nil {
				return fmt.Errorf("migration lock: %w", err)
			}
		}
		if m.MigrateLockTimeout != "" {
			if _, err := tx.ExecContext(ctx, m.MigrateLockTimeout); err != nil {
				return fmt.Errorf("migration lock timeout: %w", err)
			}
		}
		if err := s.migrateOn(ctx, tx, ms, knownVersion(ms), false, created); err != nil {
			return err
		}
		return tx.Commit()
	}

	conn, err := s.w.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, m.SessionLock).Scan(&got); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		return errors.New("migration lock: timed out waiting for another node's migration")
	}
	defer func() {
		var released sql.NullInt64
		_ = conn.QueryRowContext(context.WithoutCancel(ctx), m.SessionUnlock).Scan(&released)
	}()
	if m.MigrateLockTimeout != "" {
		if _, err := conn.ExecContext(ctx, m.MigrateLockTimeout); err != nil {
			return fmt.Errorf("migration lock timeout: %w", err)
		}
		defer func() {
			if _, err := conn.ExecContext(context.WithoutCancel(ctx), m.MigrateLockReset); err != nil {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}()
	}
	return s.migrateOn(ctx, conn, ms, knownVersion(ms), true, created)
}

// knownVersion is the newest migration of ms.
func knownVersion(ms []migration) int {
	if len(ms) == 0 {
		return 0
	}
	return ms[len(ms)-1].version
}

// concurrentLockPoll is how often a node waiting for another's migration tries its
// lock again. It never waits inside a statement: CREATE INDEX CONCURRENTLY waits for
// every transaction running, a waiting lock request included, which would deadlock.
const concurrentLockPoll = 250 * time.Millisecond

// migrateInSteps applies ms where DDL is transactional but some migration has
// statements to run outside a transaction: on one connection holding
// Maintenance.ConcurrentLock throughout, the migrations before each such one commit
// in a transaction of their own, then its concurrent statements run if it is still
// pending, then it and those after it up to the next such one commit together.
func (s *sqlStore) migrateInSteps(ctx context.Context, ms []migration, created *bool) error {
	m := &s.d.Maintenance
	conn, err := s.w.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	for {
		var got bool
		if err := conn.QueryRowContext(ctx, m.ConcurrentLock).Scan(&got); err != nil {
			return fmt.Errorf("migration lock: %w", err)
		}
		if got {
			break
		}
		if err := s.clock.Sleep(ctx, concurrentLockPoll); err != nil {
			return err
		}
	}
	defer func() {
		if _, err := conn.ExecContext(context.WithoutCancel(ctx), m.ConcurrentUnlock); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	known := knownVersion(ms)
	fresh, err := s.pending(ctx, conn, ms[0].version)
	if err != nil {
		return err
	}
	if fresh {
		return s.migrateTxRetrying(ctx, conn, inline(ms), known, created)
	}
	for start := 0; start < len(ms); {
		end := start + 1
		for end < len(ms) && len(ms[end].concurrent) == 0 {
			end++
		}
		step := ms[start:end]
		if first := step[0]; len(first.concurrent) > 0 {
			pending, err := s.pending(ctx, conn, first.version)
			if err != nil {
				return err
			}
			if pending {
				if err := s.runConcurrent(ctx, conn, first); err != nil {
					return err
				}
			}
		}
		var stepCreated bool
		if err := s.migrateTxRetrying(ctx, conn, step, known, &stepCreated); err != nil {
			return err
		}
		if start == 0 {
			*created = stepCreated
		}
		start = end
	}
	return nil
}

// concurrentAttempts bounds how often a migration's concurrent statements are run
// again after a deadlock. CREATE INDEX CONCURRENTLY waits for every transaction
// running when it starts, so it deadlocks with one that waits, inside its
// transaction, for something the build holds (the table's lock, by an ANALYZE), and
// Postgres may cancel either.
const concurrentAttempts = 5

// runConcurrent runs m's concurrent statements, all of them again after a deadlock
// (they are written to be run again).
func (s *sqlStore) runConcurrent(ctx context.Context, conn *sql.Conn, m migration) error {
	t0 := s.clock.Now()
	for attempt := 1; ; attempt++ {
		var err error
		for i, st := range m.concurrent {
			if _, err = conn.ExecContext(ctx, st); err != nil {
				err = fmt.Errorf("migration %s%s statement %d: %w", m.name, concurrentSuffix, i+1, err)
				break
			}
		}
		if err == nil {
			s.log.InfoContext(ctx, "ran a migration's statements outside its transaction",
				slog.Int("version", m.version), slog.Duration("took", s.clock.Now().Sub(t0)))
			return nil
		}
		if attempt == concurrentAttempts || !s.retryable(err) {
			return err
		}
		s.log.WarnContext(ctx, "a migration's concurrent statement deadlocked; running them again",
			slog.Int("attempt", attempt), slog.Any("error", err))
		if err := s.clock.Sleep(ctx, time.Duration(attempt)*time.Second); err != nil {
			return err
		}
	}
}

// pending reports whether migration version is yet to be applied.
func (s *sqlStore) pending(ctx context.Context, conn *sql.Conn, version int) (bool, error) {
	m := &s.d.Maintenance
	if _, err := conn.ExecContext(ctx, m.VersionTable); err != nil {
		return false, fmt.Errorf("create version table: %w", err)
	}
	rows, err := conn.QueryContext(ctx, m.AppliedMigrations)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return false, err
		}
		if v == version {
			return false, nil
		}
	}
	return true, rows.Err()
}

// inline is ms with each migration's concurrent statements run first inside it, without
// CONCURRENTLY: the run that creates a database runs every migration in one
// transaction, so that no node of an earlier release finds the schema part way (it
// waits for the migration lock, then finds the schema newer than it knows), and the
// tables it builds on are empty. A concurrent statement must therefore also be valid
// inside a transaction once CONCURRENTLY is taken out.
func inline(ms []migration) []migration {
	out := make([]migration, len(ms))
	for i, m := range ms {
		out[i] = m
		if len(m.concurrent) == 0 {
			continue
		}
		stmts := make([]string, 0, len(m.concurrent)+len(m.stmts))
		for _, st := range m.concurrent {
			stmts = append(stmts, strings.ReplaceAll(st, " CONCURRENTLY", ""))
		}
		stmts = append(stmts, m.stmts...)
		out[i].stmts = stmts
		out[i].concurrent = nil
	}
	return out
}

// migrateTxRetrying is migrateTx, run again when its DDL times out waiting for a table
// lock, as Migrate runs a whole migration again, so that what ran outside the
// transaction before it is not run again.
func (s *sqlStore) migrateTxRetrying(ctx context.Context, conn *sql.Conn, ms []migration, known int, created *bool) error {
	timedOut := s.d.Maintenance.LockTimedOut
	wait := migrateRetryBase
	for attempt := 1; ; attempt++ {
		err := s.migrateTx(ctx, conn, ms, known, created)
		if err == nil || timedOut == nil || !timedOut(err) || attempt == migrateAttempts {
			return err
		}
		s.log.WarnContext(ctx, "a migration timed out waiting for a table lock that queries hold; retrying",
			slog.Int("attempt", attempt), slog.Duration("wait", wait), slog.Any("error", err))
		if err := s.clock.Sleep(ctx, wait); err != nil {
			return err
		}
		wait = min(2*wait, migrateRetryCap)
	}
}

// migrateTx applies ms in one transaction on conn.
func (s *sqlStore) migrateTx(ctx context.Context, conn *sql.Conn, ms []migration, known int, created *bool) error {
	m := &s.d.Maintenance
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if m.MigrateLock != "" {
		if _, err := tx.ExecContext(ctx, m.MigrateLock); err != nil {
			return fmt.Errorf("migration lock: %w", err)
		}
	}
	if m.MigrateLockTimeout != "" {
		if _, err := tx.ExecContext(ctx, m.MigrateLockTimeout); err != nil {
			return fmt.Errorf("migration lock timeout: %w", err)
		}
	}
	if err := s.migrateOn(ctx, tx, ms, known, false, created); err != nil {
		return err
	}
	return tx.Commit()
}

// migrateOn applies the pending migrations of ms, recording each version as it
// completes, and sets created when the first was among them. It refuses a database
// with a migration newer than known. resumable says a migration may have been interrupted part way (DDL is
// not transactional): a statement failing only because it was applied then is
// passed over.
func (s *sqlStore) migrateOn(ctx context.Context, ex execer, ms []migration, known int, resumable bool, created *bool) error {
	if _, err := ex.ExecContext(ctx, s.d.Maintenance.VersionTable); err != nil {
		return fmt.Errorf("create version table: %w", err)
	}
	rows, err := ex.QueryContext(ctx, s.d.Maintenance.AppliedMigrations)
	if err != nil {
		return err
	}
	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for v := range applied {
		if v > known {
			return fmt.Errorf("%w: it has migration %d, this binary knows up to %d", ErrNewerSchema, v, known)
		}
	}
	*created = len(ms) > 0 && !applied[ms[0].version]
	record := s.d.Maintenance.RecordMigration
	for _, m := range ms {
		if applied[m.version] {
			continue
		}
		applied := s.d.Maintenance.AlreadyApplied
		for i, st := range m.stmts {
			if _, err := ex.ExecContext(ctx, st); err != nil && (!resumable || applied == nil || !applied(err)) {
				return fmt.Errorf("migration %s statement %d: %w", m.name, i+1, err)
			}
		}
		if _, err := ex.ExecContext(ctx, record, m.version, m.name); err != nil {
			return fmt.Errorf("record migration %s: %w", m.name, err)
		}
		s.log.InfoContext(ctx, "applied migration", slog.Int("version", m.version), slog.String("name", m.name))
	}
	return nil
}
