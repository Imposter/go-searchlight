// Package dialect describes one database engine to the SQL store: how a store
// URL becomes connection pools, how transient failures look, how migrations
// are locked, the embedded migrations themselves, and every SQL statement the
// store runs, grouped by area (see statements.go).
//
// The store (package store) holds the logic that must be the same on every
// engine: transaction shapes, retries, seq allocation under the counter lock,
// lease fencing, the blob protocol and the Apply guards. Each engine package
// (sqlite, postgres, mysql) owns its SQL, written in its own spelling. Keeping
// the description in its own package lets the store import every engine
// without an import cycle.
package dialect

import (
	"context"
	"database/sql"
	"io/fs"
	"net/url"
	"time"
)

// Pools are the connection pools a dialect opens. Write runs every
// transaction that changes data; Read runs plain reads and snapshot scans.
// They are the same pool except on SQLite, whose writer pool begins every
// transaction with BEGIN IMMEDIATE.
//
// Read must reach the same database as Write (the primary), never a replica
// that lags it: a tailer reads HeadSeq and then ChangesAfter and trusts that
// every seq at or below the head is visible to the second read.
type Pools struct {
	Write *sql.DB
	Read  *sql.DB
}

// Close closes both pools once.
func (p Pools) Close() error {
	err := p.Write.Close()
	if p.Read != p.Write {
		if rerr := p.Read.Close(); err == nil {
			err = rerr
		}
	}
	return err
}

// Dialect is one SQL engine.
type Dialect struct {
	// Name is the dialect's name in metrics and errors: sqlite, postgres or
	// mysql.
	Name string

	// Open turns a store URL into connection pools. It does not contact the
	// database; the store pings afterwards.
	Open func(u *url.URL) (Pools, error)

	// Migrations holds the dialect's NNNN_name.sql files at its root, applied
	// in version order. Statements are separated by a semicolon at the end of
	// a line.
	Migrations fs.FS

	// ApplyTx and SnapshotTx are the transaction options for the writes that
	// take the counter lock (or another row lock) and for consistent
	// multi-statement reads.
	ApplyTx    *sql.TxOptions
	SnapshotTx *sql.TxOptions

	// Checkpoint, when set, is SQLite's WAL checkpoint, returning (busy, log frames,
	// checkpointed frames). The store runs it on a read connection, off the write
	// connection (whose automatic checkpoints are off), so a commit never stalls every
	// other writer while it copies the log into the database. Every CheckpointEvery
	// the store asks PendingLog how many bytes of the log no checkpoint has copied
	// yet, and checkpoints once that reaches CheckpointMinLog: as SQLite's automatic
	// checkpoint does (1,000 pages), with no fsync while little is pending.
	//
	// A log copied back in full is restarted by the next write, but under a steady
	// stream of writes that moment never comes, so once the log file has grown past
	// TruncateAbove the store empties it with TruncateCheckpoint, which blocks every
	// writer while it runs. It does so on the write connection: when the writers are
	// idle and at most TruncateMaxPending is left to copy; or, when they have not been
	// idle for TruncateAfterTicks checks or the file has grown past twice
	// TruncateAbove, next in line after the commit in flight (whatever the commits
	// since the last checkpoint left to copy).
	Checkpoint         string
	TruncateCheckpoint string
	TruncateAbove      int64
	TruncateMaxPending int64
	TruncateAfterTicks int
	CheckpointEvery    time.Duration
	CheckpointMinLog   int64
	PendingLog         func(dbPath string) (int64, error)
	// LogWarnBytes is the write-ahead log size past which, when a checkpoint could
	// not copy every frame (a long reader pins the log), the store warns.
	LogWarnBytes int64

	// Retryable reports a transient error (deadlock, serialization failure,
	// lock wait timeout) after which the whole transaction, rolled back by the
	// engine, may simply be run again. Nil means none are.
	Retryable func(error) bool

	// Listen, when set, subscribes conn to the commit notifications that
	// Changelog.Write sends with Write.Notify, calls ready (when not nil) once
	// it is subscribed, and calls fn with each payload until ctx ends. It is
	// nil where the engine has no notifications, and then the store never
	// fills Write.Notify.
	Listen func(ctx context.Context, conn *sql.Conn, ready func(), fn func(payload string)) error

	// The statements, by area.
	Changelog   Changelog
	Records     Records
	Registry    Registry
	Blobs       Blobs
	Indexes     Indexes
	Maintenance Maintenance
}
