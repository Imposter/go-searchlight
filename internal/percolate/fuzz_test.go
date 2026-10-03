package percolate

import (
	"context"
	"testing"
)

// FuzzOpenQuerySegment feeds arbitrary bytes to the query segment reader. The input is
// the file body (everything before the checksum): it is stamped with the current magic,
// version and section count and given a valid checksum, so the fuzzer reaches the
// structural checks rather than stopping at the checksum. Every input must be refused
// with a *CorruptError, or open and survive every accessor without a panic or a hang.
//
// Run it with: go test -run '^$' -fuzz FuzzOpenQuerySegment ./internal/percolate/
func FuzzOpenQuerySegment(f *testing.F) {
	full, err := encodeSegment(context.Background(), sampleStored(f), nil)
	if err != nil {
		f.Fatal(err)
	}
	empty, err := encodeSegment(context.Background(), nil, nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(full[:len(full)-4])
	f.Add(empty[:len(empty)-4])
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, body []byte) {
		seg, err := openData("fuzz", restamp(body))
		if err != nil {
			if !isCorrupt(err) {
				t.Fatalf("refused with %T (%v), want *CorruptError", err, err)
			}
			return
		}
		exercise(seg)
	})
}
