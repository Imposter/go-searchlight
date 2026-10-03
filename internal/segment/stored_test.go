package segment

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// TestCompressBlocksParallelEncoderFailureNoLeak pins N6: when every worker fails to
// make its encoder, compressBlocksParallel must return the error without leaving the
// goroutine that hands out block indices blocked forever on a channel nobody reads.
func TestCompressBlocksParallelEncoderFailureNoLeak(t *testing.T) {
	errBoom := errors.New("no encoder for you")
	orig := newBlockEncoder
	newBlockEncoder = func() (*zstd.Encoder, error) { return nil, errBoom }
	origPool := blockEncoders
	blockEncoders = &sync.Pool{} // no idle encoder to fall back on
	defer func() { newBlockEncoder, blockEncoders = orig, origPool }()

	payloads := make([]storedPayload, 64)
	for i := range payloads {
		payloads[i] = storedPayload{firstOrd: uint32(i), count: 1, data: []byte("x")}
	}
	before := runtime.NumGoroutine()
	for range 5 {
		if _, err := compressBlocksParallel(payloads, 4); !errors.Is(err, errBoom) {
			t.Fatalf("compressBlocksParallel error = %v, want %v", err, errBoom)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines: %d before, %d after five failed calls (leaked)", before, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
