package api

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// scope is what a route needs of a request's token.
type scope uint8

const (
	// scopeNone routes (liveness, readiness) need no token.
	scopeNone scope = iota
	// scopeRead routes need a read-only or read-write token.
	scopeRead
	// scopeWrite routes need a read-write token.
	scopeWrite
)

// MinTokenBytes is the shortest token a tokens file may hold.
const MinTokenBytes = 16

// TokenSum is a token's SHA-256. Tokens are compared by sum, so a comparison takes the
// same time whatever their lengths.
type TokenSum [sha256.Size]byte

// SumToken returns token's sum.
func SumToken(token string) TokenSum { return sha256.Sum256([]byte(token)) }

// Match returns 1 when s and o are equal and 0 otherwise, in constant time.
func (s *TokenSum) Match(o *TokenSum) int { return subtle.ConstantTimeCompare(s[:], o[:]) }

// BearerToken returns the token r's Authorization header presents under the Bearer
// scheme (in any case), trimmed of white space, and whether the header uses that scheme.
func BearerToken(r *http.Request) (string, bool) {
	scheme, presented, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	return strings.TrimSpace(presented), true
}

// token is one API token.
type token struct {
	sum   TokenSum
	write bool
}

// authenticator checks bearer tokens against a tokens file's.
type authenticator struct {
	tokens []token
}

// loadTokens reads a tokens file: one token per line, optionally followed by its
// scope, "read" (read-only) or "write" (read-write, the default). Blank lines and
// lines starting with # are skipped. A token is at least MinTokenBytes long and holds
// no whitespace.
func loadTokens(path string) (*authenticator, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tokens_file: %w", err)
	}
	a := &authenticator{}
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Fields(text)
		t := token{write: true}
		switch {
		case len(fields) == 1:
		case len(fields) == 2 && fields[1] == "read":
			t.write = false
		case len(fields) == 2 && fields[1] == "write":
		default:
			return nil, fmt.Errorf(`tokens_file: line %d: want "<token>" or "<token> read|write"`, line)
		}
		if len(fields[0]) < MinTokenBytes {
			return nil, fmt.Errorf("tokens_file: line %d: a token is at least %d bytes", line, MinTokenBytes)
		}
		t.sum = SumToken(fields[0])
		a.tokens = append(a.tokens, t)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("tokens_file: %w", err)
	}
	if len(a.tokens) == 0 {
		return nil, fmt.Errorf("tokens_file: %s holds no token", path)
	}
	return a, nil
}

// check authorizes r for need: nil, or a 401 (no or unknown token) or 403 (a
// read-only token on a write route). The presented token is compared against every
// token, with no early exit, so the time taken says nothing about which token, if any,
// matched.
func (a *authenticator) check(r *http.Request, need scope) *Error {
	if a == nil || need == scopeNone {
		return nil
	}
	presented, ok := BearerToken(r)
	if !ok || presented == "" {
		return &Error{Status: http.StatusUnauthorized, Code: CodeUnauthorized, Detail: "a bearer token is required"}
	}
	sum := SumToken(presented)
	found, write := 0, 0
	for i := range a.tokens {
		eq := sum.Match(&a.tokens[i].sum)
		found |= eq
		if a.tokens[i].write {
			write |= eq
		}
	}
	switch {
	case found == 0:
		return &Error{Status: http.StatusUnauthorized, Code: CodeUnauthorized, Detail: "the bearer token is not valid"}
	case need == scopeWrite && write == 0:
		return &Error{Status: http.StatusForbidden, Code: CodeForbidden, Detail: "the token is read-only"}
	}
	return nil
}
