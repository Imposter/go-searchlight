// Package sqlite is the SQLite dialect of the SQL store, on the pure-Go
// modernc.org/sqlite driver.
//
// The database runs in WAL mode so readers never block the writer. Writes go
// through a one-connection pool whose transactions begin with BEGIN
// IMMEDIATE, taking SQLite's write lock up front: that lock is the counter
// lock, so sequence numbers are assigned and committed in order, and no
// writer ever blocks mid-transaction trying to upgrade a deferred lock to a
// write lock (the SQLITE_BUSY livelock that class of locking invites). Other
// connections to the same file — another node's Store, or SQLite's own
// internal busy handler within one connection — wait on the busy timeout;
// past it, SQLITE_BUSY and SQLITE_LOCKED (and their extended codes) are
// retried like a Postgres serialization failure or a MySQL deadlock: the
// whole transaction was never committed, so running it again is safe.
//
// A batch is written one row a statement, not as multi-row VALUES lists:
// the store prepares each statement text once per batch and steps it for
// every row. SQLite runs in process, so there are no round trips for long
// VALUES lists to save, while compiling a list of hundreds of rows costs more
// than stepping a prepared statement per row. Measured with fixed iterations,
// Apply of 1000 1 KB documents runs about 2.1 times as fast as with VALUES
// lists sized to the variable limit, and about 1.8 times as fast as main's
// 500-row lists (BenchmarkApply in package store). The store's limits are
// therefore unused here. Writes that are read back use RETURNING (SQLite 3.35
// and later).
package sqlite

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strings"
	"time"

	sqlitedriver "modernc.org/sqlite" // registers the "sqlite" database/sql driver
	sqlite3 "modernc.org/sqlite/lib"  // SQLITE_BUSY, SQLITE_LOCKED result codes

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

//go:embed migrations/*.sql
var migrations embed.FS

// CheckpointEvery is how often the store checks whether the WAL needs a checkpoint.
const CheckpointEvery = 500 * time.Millisecond

// JournalSizeLimit is the size the WAL file is cut back to when SQLite restarts it,
// and the log a checkpoint waits for.
const JournalSizeLimit = 4 << 20

// DefaultBusyTimeoutMS is how long a connection waits for another process's
// lock before failing with SQLITE_BUSY, unless the URL sets _busy_timeout.
const DefaultBusyTimeoutMS = 30000

// Dialect returns the SQLite dialect.
func Dialect() *dialect.Dialect {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		panic(err) // the embedded directory always exists
	}
	return &dialect.Dialect{
		Name:       "sqlite",
		Open:       open,
		Migrations: sub,
		Retryable:  retryable,
		// The writer's automatic checkpoints are off (DSNs): a PASSIVE checkpoint on
		// a read connection keeps the log short instead, off every commit's path.
		Checkpoint:         "PRAGMA wal_checkpoint(PASSIVE)",
		TruncateCheckpoint: "PRAGMA wal_checkpoint(TRUNCATE)",
		TruncateAbove:      4 * JournalSizeLimit,
		CheckpointEvery:    CheckpointEvery,
		CheckpointMinLog:   JournalSizeLimit,
		PendingLog:         PendingLog,
		LogWarnBytes:       64 << 20,
		Changelog:          changelog,
		Records:            records,
		Registry:           registry,
		Blobs:              blobs,
		Indexes:            indexes,
		Maintenance:        maintenance,
	}
}

// retryable reports SQLITE_BUSY and SQLITE_LOCKED, including their extended
// variants (SQLITE_BUSY_RECOVERY, _SNAPSHOT, _TIMEOUT; SQLITE_LOCKED_
// SHAREDCACHE, _VTAB): an extended code's low byte is always its primary
// code, so masking catches all of them without naming each one. Both mean
// the transaction's BEGIN IMMEDIATE (or a statement within it) could not get
// the lock it needed within the busy timeout; SQLite guarantees neither
// return partway through a write, so the transaction never committed and
// running it again cannot double-apply it.
func retryable(err error) bool {
	var se *sqlitedriver.Error
	if !errors.As(err, &se) {
		return false
	}
	switch se.Code() & 0xff {
	case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
		return true
	default:
		return false
	}
}

// Path extracts the database file path from a sqlite URL:
// sqlite:///abs/path.db, sqlite:///C:/dir/x.db (Windows), sqlite://rel/x.db
// or sqlite:rel.db.
func Path(u *url.URL) (string, error) {
	var p string
	switch {
	case u.Opaque != "":
		p = u.Opaque
	default:
		p = u.Host + u.Path
		// On Windows the URL's path holds the drive after a slash; drop it.
		if len(p) >= 3 && p[0] == '/' && p[2] == ':' && isLetter(p[1]) {
			p = p[1:]
		}
	}
	if p == "" {
		return "", errors.New("sqlite store URL has no database path")
	}
	if strings.Contains(p, ":memory:") || strings.HasPrefix(p, "file:") {
		return "", errors.New("sqlite store URL must name a database file (in-memory databases cannot use WAL)")
	}
	return p, nil
}

func isLetter(b byte) bool { return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') }

// DSNs returns the driver DSNs for the writer and reader pools. Parameters in
// the URL are passed through; journal mode, transaction locking and
// (unless given) the busy timeout and synchronous level are set here.
func DSNs(u *url.URL) (write, read string, err error) {
	path, err := Path(u)
	if err != nil {
		return "", "", err
	}
	q := u.Query()
	for _, k := range []string{"_txlock", "_journal_mode", "_journal"} {
		if q.Has(k) {
			return "", "", fmt.Errorf("sqlite store URL may not set %s; the store manages it", k)
		}
	}
	q.Set("_journal_mode", "WAL")
	if !q.Has("_busy_timeout") && !q.Has("_timeout") {
		q.Set("_busy_timeout", fmt.Sprint(DefaultBusyTimeoutMS))
	}
	if !q.Has("_synchronous") && !q.Has("_sync") {
		// FULL: a write is acknowledged only once it is durable (spec §8).
		q.Set("_synchronous", "FULL")
	}
	q.Add("_pragma", fmt.Sprintf("journal_size_limit(%d)", JournalSizeLimit))
	w := cloneValues(q)
	w.Set("_txlock", "immediate")
	w.Add("_pragma", "wal_autocheckpoint(0)") // the store checkpoints off the write path
	r := cloneValues(q)
	r.Set("_txlock", "deferred")
	return path + "?" + w.Encode(), path + "?" + r.Encode(), nil
}

func cloneValues(v url.Values) url.Values {
	out := make(url.Values, len(v))
	for k, vs := range v {
		out[k] = append([]string(nil), vs...)
	}
	return out
}

func open(u *url.URL) (dialect.Pools, error) {
	wdsn, rdsn, err := DSNs(u)
	if err != nil {
		return dialect.Pools{}, err
	}
	w, err := sql.Open("sqlite", wdsn)
	if err != nil {
		return dialect.Pools{}, err
	}
	// One writer connection: writers in this process queue in the pool
	// instead of spinning in SQLite's busy handler.
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	r, err := sql.Open("sqlite", rdsn)
	if err != nil {
		_ = w.Close()
		return dialect.Pools{}, err
	}
	r.SetMaxOpenConns(16)
	r.SetMaxIdleConns(16)
	return dialect.Pools{Write: w, Read: r}, nil
}
