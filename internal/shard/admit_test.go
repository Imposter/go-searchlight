package shard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// Admit, the lock-free check a writer makes before committing, refuses while the
// buffer is over its limit and admits once a refresh drains it.
func TestAdmit(t *testing.T) {
	opts := testOptions()
	opts.FlushBytes = 4 << 10
	opts.MaxBufferFactor = 2
	h := newHarness(t, opts)
	if err := h.s.Admit(); err != nil {
		t.Fatalf("an empty shard: %v", err)
	}
	refused := 0
	for i := range 200 {
		if errors.Is(h.s.Admit(), ErrBackpressure) {
			refused++
		}
		h.seq++
		id := fmt.Sprintf("d%03d", i)
		c := []Change{{Seq: h.seq, Kind: Upsert, Doc: analyze(t, id, body(id, h.seq))}}
		for {
			err := h.s.Apply(context.Background(), c)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrBackpressure) {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	if refused == 0 {
		t.Error("Admit never refused a writer far ahead of refreshes")
	}
	h.refresh()
	if err := h.s.Admit(); err != nil {
		t.Errorf("after a refresh: %v", err)
	}
	if err := h.s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.s.Admit(); !errors.Is(err, ErrClosed) {
		t.Errorf("a closed shard: %v", err)
	}
}

func TestCheckDocSize(t *testing.T) {
	if err := CheckDocSize("id", []byte(strings.Repeat("x", segment.MaxStoredBytes-2))); err != nil {
		t.Errorf("at the limit: %v", err)
	}
	if err := CheckDocSize("id", []byte(strings.Repeat("x", segment.MaxStoredBytes-1))); !errors.Is(err, ErrDocTooLarge) {
		t.Errorf("over the limit: %v", err)
	}
}
