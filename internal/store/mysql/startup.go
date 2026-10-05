package mysql

import (
	"database/sql/driver"
	"errors"

	"github.com/go-sql-driver/mysql"
)

// Unreachable reports a server that takes no connection for now: a connection the
// driver lost or found broken, a refused or failed connection (client errors 2002,
// 2003, 2013), too many connections (1040) or a server shutting down (1053). Waiting
// may cure it.
func Unreachable(err error) bool {
	if errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		return false
	}
	switch me.Number {
	case 2002, 2003, 2013, 1040, 1053:
		return true
	}
	return false
}

// Misconfigured reports a refusal retrying cannot cure: the credentials (1045), the
// user's access to the database (1044) or a database that does not exist (1049).
func Misconfigured(err error) bool {
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		return false
	}
	switch me.Number {
	case 1044, 1045, 1049:
		return true
	}
	return false
}
