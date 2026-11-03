package api

import (
	"fmt"
	"strings"
	"testing"
)

const testUID = "0123456789abcdef0123456789abcdef"

func TestTokenRoundTrip(t *testing.T) {
	for _, shards := range []int{1, 8, 64, MaxTokenShards} {
		tok := &Token{UID: testUID}
		for s := range shards {
			tok.Note(s, int64(1_000_000+s*7))
		}
		text := tok.String()
		if len(text) > MaxTokenBytes {
			t.Fatalf("%d shards: %d bytes", shards, len(text))
		}
		got, err := ParseToken(text)
		if err != nil || got.UID != testUID || fmt.Sprint(got.Seqs) != fmt.Sprint(tok.Seqs) {
			t.Fatalf("%d shards: %+v %v", shards, got, err)
		}
		t.Logf("%d shards, seqs near a million: %d bytes", shards, len(text))
	}
	sparse := &Token{UID: testUID}
	sparse.Note(3, 9)
	sparse.Note(1023, 1)
	if got, err := ParseToken(sparse.String()); err != nil || got.Seq(3) != 9 || got.Seq(1023) != 1 || got.Seq(0) != 0 {
		t.Fatalf("sparse: %+v %v", got, err)
	}
	if (&Token{UID: testUID}).String() != "" {
		t.Fatal("an empty token encodes")
	}
}

func TestTokenMerge(t *testing.T) {
	a := &Token{UID: testUID}
	a.Note(0, 5)
	a.Note(2, 7)
	b := &Token{UID: testUID}
	b.Note(0, 3)
	b.Note(1, 4)
	if !a.Merge(b) || fmt.Sprint(a.Seqs) != "map[0:5 1:4 2:7]" {
		t.Fatalf("merged %v", a.Seqs)
	}
	if a.Merge(&Token{UID: strings.Repeat("f", 32), Seqs: map[int]int64{0: 9}}) || a.Seq(0) != 5 {
		t.Fatal("tokens of two incarnations merged")
	}
}

func TestTokenRefusesMalformed(t *testing.T) {
	good := &Token{UID: testUID}
	good.Note(0, 5)
	good.Note(3, 6)
	text := good.String()
	for name, s := range map[string]string{
		"empty":      "",
		"not b64":    "!!!",
		"padded":     text + "=",
		"truncated":  text[:len(text)-2],
		"trailing":   text + "AA",
		"version":    tokenEncoding.EncodeToString(append([]byte{2}, make([]byte, 19)...)),
		"no shards":  tokenEncoding.EncodeToString(append(append([]byte{1}, make([]byte, 16)...), 0)),
		"seq 0":      tokenEncoding.EncodeToString(append(append([]byte{1}, make([]byte, 16)...), 1, 1, 0)),
		"shard 1024": tokenEncoding.EncodeToString(append(append([]byte{1}, make([]byte, 16)...), 1, 0x81, 0x08, 1)),
		"too long":   strings.Repeat("A", MaxTokenBytes+1),
	} {
		if tok, err := ParseToken(s); err == nil {
			t.Errorf("%s: parsed %+v", name, tok)
		}
	}
}

// FuzzParseToken: no input panics, and a token that parses names 1 to MaxTokenShards
// shards in range, each at a seq in range, and survives a round trip.
func FuzzParseToken(f *testing.F) {
	good := &Token{UID: testUID}
	good.Note(0, 5)
	good.Note(9, 1<<40)
	f.Add(good.String())
	f.Add("")
	f.Add("AQAAAAAAAAAAAAAAAAAAAAABAQE")
	f.Add("AQAAAAAAAAAAAAAAAAAAAAD_____________________AQ")
	f.Fuzz(func(t *testing.T, s string) {
		tok, err := ParseToken(s)
		if err != nil {
			return
		}
		if len(tok.Seqs) == 0 || len(tok.Seqs) > MaxTokenShards {
			t.Fatalf("%q names %d shards", s, len(tok.Seqs))
		}
		for sh, seq := range tok.Seqs {
			if sh < 0 || sh >= MaxTokenShards || seq < 1 || seq > 1<<62 {
				t.Fatalf("%q names shard %d at seq %d", s, sh, seq)
			}
		}
		again, err := ParseToken(tok.String())
		if err != nil || fmt.Sprint(again.Seqs) != fmt.Sprint(tok.Seqs) || again.UID != tok.UID {
			t.Fatalf("%q reencodes as %q: %v", s, tok.String(), err)
		}
	})
}
