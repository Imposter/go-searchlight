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
package sqlite

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strings"

	sqlitedriver "modernc.org/sqlite" // registers the "sqlite" database/sql driver
	sqlite3 "modernc.org/sqlite/lib"  // SQLITE_BUSY, SQLITE_LOCKED result codes

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

//go:embed migrations/*.sql
var migrations embed.FS

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
		VersionTable: `CREATE TABLE IF NOT EXISTS sl_schema_migrations (
	version INTEGER NOT NULL PRIMARY KEY,
	name TEXT NOT NULL,
	applied_at INTEGER NOT NULL
)`,
		MigrateInTx: true, // BEGIN IMMEDIATE excludes every other writer
		ForUpdate:   "",
		Now:         "CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER)",
		Upsert:      onConflict,
		Claim:       claim,
		Greatest:    greatest,
		MaxParams:   32766,
		Retryable:   retryable,
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
	w := cloneValues(q)
	w.Set("_txlock", "immediate")
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

func onConflict(keys, update []string) string {
	var b strings.Builder
	b.WriteString(" ON CONFLICT (")
	b.WriteString(strings.Join(keys, ", "))
	b.WriteString(") DO UPDATE SET ")
	for i, c := range update {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c + " = excluded." + c)
	}
	return b.String()
}

const claimSQL = `INSERT INTO sl_shard_copies (index_name, shard, slot, node_id, state, applied_seq, lease_until, epoch)
VALUES (?1, ?2, ?3, ?4, 'recovering', 0, (NOW) + ?5, ?6)
ON CONFLICT (index_name, shard, slot) DO UPDATE SET
	state = CASE WHEN sl_shard_copies.node_id = excluded.node_id THEN sl_shard_copies.state ELSE excluded.state END,
	applied_seq = CASE WHEN sl_shard_copies.node_id = excluded.node_id THEN sl_shard_copies.applied_seq ELSE 0 END,
	epoch = CASE WHEN sl_shard_copies.node_id = excluded.node_id THEN sl_shard_copies.epoch ELSE excluded.epoch END,
	node_id = excluded.node_id,
	lease_until = excluded.lease_until
WHERE sl_shard_copies.node_id = excluded.node_id OR sl_shard_copies.lease_until < (NOW)`

var claimQuery = strings.ReplaceAll(claimSQL, "NOW", "CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER)")

func claim(a dialect.ClaimArgs) (string, []any) {
	return claimQuery, []any{a.Index, a.Shard, a.Slot, a.Node, a.TTLms, a.Epoch}
}

// greatest uses SQLite's multi-argument MAX, which (unlike single-argument
// MAX) is the scalar function, not the aggregate.
func greatest(a, b string) string { return "MAX(" + a + ", " + b + ")" }
