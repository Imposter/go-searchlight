package api

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// A Token is a position in an index's changelog: for each shard it names, a seq of
// that shard, with the index's incarnation (its uid). Every shard's seqs are its own,
// so a position is a seq per shard. A write returns the token of what it committed; a
// read given one (wait_for) waits until every shard it reads holds its shard's seq,
// and only those shards. Tokens merge by the per-shard maximum, so a client that
// writes several times may pass the merge of their tokens, or the last one per shard.
//
// On the wire a token is opaque: base64url, unpadded, of
//
//	byte     version (1)
//	16 bytes the index's uid
//	uvarint  the number of entries, n
//	n times  uvarint shard minus the previous entry's shard plus one (shards ascend),
//	         uvarint seq (at least 1)
//
// It is at most MaxTokenBytes long, which an index of MaxTokenShards shards fits.
type Token struct {
	// UID is the index's incarnation, 32 hex digits.
	UID string
	// Seqs maps a shard to the seq to wait for on it; a shard it does not name is not
	// waited for.
	Seqs map[int]int64
}

const (
	tokenVersion = 1
	// MaxTokenShards is the most shards a token names: an index's shard limit.
	MaxTokenShards = 1024
	// MaxTokenBytes bounds a token's text: 17 bytes, then up to 1,024 entries of at
	// most 2 bytes of shard and 9 of seq, in base64.
	MaxTokenBytes = 16 << 10
)

var tokenEncoding = base64.RawURLEncoding

// Seq is the token's seq for shard, 0 when it names none (or t is nil).
func (t *Token) Seq(shard int) int64 {
	if t == nil {
		return 0
	}
	return t.Seqs[shard]
}

// Empty reports whether t waits for nothing.
func (t *Token) Empty() bool { return t == nil || len(t.Seqs) == 0 }

// Note raises shard's seq in t to seq.
func (t *Token) Note(shard int, seq int64) {
	if seq <= 0 {
		return
	}
	if t.Seqs == nil {
		t.Seqs = map[int]int64{}
	}
	t.Seqs[shard] = max(t.Seqs[shard], seq)
}

// Merge raises t's seqs to o's, shard by shard. Tokens of different incarnations do not
// merge: it reports false.
func (t *Token) Merge(o *Token) bool {
	if o == nil {
		return true
	}
	if t.UID != o.UID {
		return false
	}
	for s, seq := range o.Seqs {
		t.Note(s, seq)
	}
	return true
}

// String encodes t; "" for a token that names no shard.
func (t *Token) String() string {
	if t.Empty() {
		return ""
	}
	uid, err := hex.DecodeString(t.UID)
	if err != nil || len(uid) != 16 {
		uid = make([]byte, 16) // never: the store mints every uid as 32 hex digits
	}
	b := make([]byte, 0, 18+len(t.Seqs)*6)
	b = append(b, tokenVersion)
	b = append(b, uid...)
	b = binary.AppendUvarint(b, uint64(len(t.Seqs)))
	prev := -1
	for _, s := range slices.Sorted(maps.Keys(t.Seqs)) {
		b = binary.AppendUvarint(b, uint64(s-prev))    //nolint:gosec // shards ascend, so the delta is positive
		b = binary.AppendUvarint(b, uint64(t.Seqs[s])) //nolint:gosec // Note keeps only positive seqs
		prev = s
	}
	return tokenEncoding.EncodeToString(b)
}

// MarshalText encodes t.
func (t *Token) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

// errToken is every malformed token's error.
var errToken = errors.New("not a token a write returned")

// ParseToken decodes a token. It fails for anything a write did not return: another
// version, a truncated or padded token, shards out of order or past MaxTokenShards, a
// seq of 0, or more than MaxTokenBytes.
func ParseToken(s string) (*Token, error) {
	if len(s) > MaxTokenBytes {
		return nil, fmt.Errorf("%w: %d bytes, more than %d", errToken, len(s), MaxTokenBytes)
	}
	b, err := tokenEncoding.DecodeString(s)
	if err != nil || len(b) < 18 || b[0] != tokenVersion {
		return nil, errToken
	}
	t := &Token{UID: hex.EncodeToString(b[1:17]), Seqs: map[int]int64{}}
	b = b[17:]
	n, k := binary.Uvarint(b)
	if k <= 0 || n == 0 || n > MaxTokenShards {
		return nil, errToken
	}
	b = b[k:]
	prev := -1
	for range n {
		d, k := binary.Uvarint(b)
		if k <= 0 || d == 0 || d > MaxTokenShards {
			return nil, errToken
		}
		b = b[k:]
		seq, k := binary.Uvarint(b)
		if k <= 0 || seq == 0 || seq > 1<<62 {
			return nil, errToken
		}
		b = b[k:]
		shard := prev + int(d)
		if shard >= MaxTokenShards {
			return nil, errToken
		}
		t.Seqs[shard] = int64(seq)
		prev = shard
	}
	if len(b) != 0 {
		return nil, errToken
	}
	return t, nil
}
