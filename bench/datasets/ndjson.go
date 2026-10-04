package datasets

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
)

// chunk is how many lines a generator worker renders at once.
const chunk = 4096

// LineFunc appends item i's NDJSON line to dst.
type LineFunc func(dst []byte, i int64) ([]byte, error)

// ProductLines renders product lines under seed.
func ProductLines(seed uint64) LineFunc {
	return func(dst []byte, i int64) ([]byte, error) { return AppendProductLine(dst, seed, i), nil }
}

// SearchLines renders saved-search lines under seed.
func SearchLines(seed uint64) LineFunc {
	return func(dst []byte, i int64) ([]byte, error) { return AppendSearchLine(dst, seed, i) }
}

// Write streams items [start, start+n) to w in order, rendered by workers goroutines
// (GOMAXPROCS when 0) in chunks; at most 2*workers chunks are held at once, so memory
// stays constant whatever n is.
func Write(ctx context.Context, w io.Writer, lines LineFunc, start, n int64, workers int) error {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	nChunks := (n + chunk - 1) / chunk
	type result struct {
		k   int64
		buf []byte
		err error
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int64)
	results := make(chan result, workers)
	tokens := make(chan struct{}, 2*workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for k := range jobs {
				lo := start + k*chunk
				hi := min(lo+chunk, start+n)
				buf := make([]byte, 0, (hi-lo)*1024)
				var err error
				for i := lo; i < hi && err == nil; i++ {
					buf, err = lines(buf, i)
				}
				select {
				case results <- result{k, buf, err}:
				case <-ctx.Done():
					return
				}
			}
		})
	}
	go func() {
		defer close(jobs)
		for k := range nChunks {
			select {
			case tokens <- struct{}{}:
			case <-ctx.Done():
				return
			}
			select {
			case jobs <- k:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()

	pending := map[int64][]byte{}
	next := int64(0)
	for res := range results {
		if res.err != nil {
			return res.err
		}
		pending[res.k] = res.buf
		for {
			buf, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			if _, err := w.Write(buf); err != nil {
				return err
			}
			<-tokens
			next++
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if next != nChunks {
		return fmt.Errorf("datasets: wrote %d of %d chunks", next, nChunks)
	}
	return nil
}

// MaxLine bounds one NDJSON line read back.
const MaxLine = 4 << 20

// ReadLines calls fn with each non-blank line of r (the slice is reused after fn
// returns), stopping at the first error.
func ReadLines(r io.Reader, fn func(line []byte) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), MaxLine)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := fn(line); err != nil {
			return err
		}
	}
	return sc.Err()
}

// SplitProductLine reads a dataset line, {"id": ..., "doc": {...}}, without decoding
// the document: it returns the id and the document's bytes (a sub-slice of line).
func SplitProductLine(line []byte) (string, []byte, error) {
	const head, mid = `{"id":"`, `","doc":`
	if bytes.HasPrefix(line, []byte(head)) && bytes.HasSuffix(line, []byte("}")) {
		rest := line[len(head):]
		if j := bytes.Index(rest, []byte(mid)); j > 0 && bytes.IndexByte(rest[:j], '\\') < 0 {
			return string(rest[:j]), rest[j+len(mid) : len(rest)-1], nil
		}
	}
	var v struct {
		ID  string          `json:"id"`
		Doc json.RawMessage `json:"doc"`
	}
	if err := json.Unmarshal(line, &v); err != nil {
		return "", nil, fmt.Errorf("datasets: a product line: %w", err)
	}
	if v.ID == "" || len(v.Doc) == 0 {
		return "", nil, errors.New(`datasets: a product line needs "id" and "doc"`)
	}
	return v.ID, v.Doc, nil
}
