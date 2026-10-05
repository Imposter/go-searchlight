package sqlite

import (
	"errors"

	sqlitedriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Unreachable reports a database another connection holds locked for now
// (SQLITE_BUSY, SQLITE_LOCKED). Waiting may cure it.
func Unreachable(err error) bool { return retryable(err) }

// Misconfigured reports a database file that cannot be used as it is: it cannot be
// opened (SQLITE_CANTOPEN: a missing directory, a path that is not a file), access is
// denied (SQLITE_PERM, SQLITE_AUTH), it is read-only (SQLITE_READONLY) or it is not a
// database (SQLITE_NOTADB). Retrying cannot cure it.
func Misconfigured(err error) bool {
	var se *sqlitedriver.Error
	if !errors.As(err, &se) {
		return false
	}
	switch se.Code() & 0xff {
	case sqlite3.SQLITE_CANTOPEN, sqlite3.SQLITE_PERM, sqlite3.SQLITE_AUTH, sqlite3.SQLITE_READONLY, sqlite3.SQLITE_NOTADB:
		return true
	}
	return false
}
