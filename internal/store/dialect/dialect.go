// Package dialect describes how the SQL store talks to one database engine:
// how a store URL becomes connection pools, the SQL spellings that differ
// (placeholders, row locks, upserts, the database clock), how migrations are
// locked, and the embedded migrations themselves.
//
// The store engine (package store) is written once against this description;
// the sqlite, postgres and mysql packages each provide one Dialect. Keeping
// the description in its own package lets the store import every dialect
// without an import cycle.
package dialect

import (
	"context"
	"database/sql"
	"io/fs"
	"net/url"
)

// Pools are the connection pools a dialect opens. Write runs every
// transaction that changes data; Read runs plain reads and snapshot scans.
// They are the same pool except on SQLite, whose writer pool begins every
// transaction with BEGIN IMMEDIATE.
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

// ClaimArgs are the inputs of the conditional slot claim (see Dialect.Claim).
type ClaimArgs struct {
	Index string
	Shard int
	Slot  int
	Node  string
	TTLms int64
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

	// VersionTable creates the migration version table if it is missing.
	VersionTable string

	// MigrateInTx says DDL is transactional: every pending migration runs in
	// one transaction that first executes MigrateLock (which may be empty when
	// the transaction itself excludes other writers, as on SQLite). When false,
	// migrations run on one connection holding SessionLock until
	// SessionUnlock, and each file is recorded as it completes.
	MigrateInTx   bool
	MigrateLock   string
	SessionLock   string // must return one row whose first column is 1 on success
	SessionUnlock string

	// Dollar says placeholders are $1, $2, ...; the engine writes ? and
	// rebinds.
	Dollar bool

	// ForUpdate is appended to a SELECT to lock the rows it reads until the
	// transaction ends ("" where the transaction already holds the write lock).
	ForUpdate string

	// Now is an SQL expression for the database clock in Unix milliseconds.
	// Leases and heartbeats are judged by this one clock, so node clock skew
	// does not matter.
	Now string

	// Upsert returns the clause that follows INSERT ... VALUES ... to turn a
	// key conflict into an update of the update columns from the new row.
	Upsert func(keys, update []string) string

	// Claim returns the conditional insert-or-steal of one shard-copy slot:
	// insert the slot for Node, or take it over when its lease has expired, or
	// renew it when Node already holds it; otherwise leave it unchanged. A new
	// owner starts in state "recovering" with applied_seq 0.
	Claim func(a ClaimArgs) (query string, args []any)

	// ApplyTx and SnapshotTx are the transaction options for Apply (the
	// counter-locking write) and for consistent multi-statement reads.
	ApplyTx    *sql.TxOptions
	SnapshotTx *sql.TxOptions

	// MaxParams bounds the bind parameters in one statement.
	MaxParams int

	// Retryable reports a transient error (deadlock, serialization failure,
	// lock wait timeout) after which the whole transaction, rolled back by the
	// engine, may simply be run again. Nil means none are.
	Retryable func(error) bool

	// Notify, when set, is a statement run inside Apply with (channel,
	// payload) to announce committed changes; Listen receives them. Both are
	// nil where the engine has no notifications.
	Notify string
	Listen func(ctx context.Context, conn *sql.Conn, channel string, fn func(payload string)) error
}
