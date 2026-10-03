package shard

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A commit while another reader (a backup, a peer recovery, a test) has the manifest
// open the way os.Open opens it - without FILE_SHARE_DELETE, so on Windows nothing can
// rename over it - waits the reader out (a bounded retry) instead of failing.
func TestCommitWaitsOutAReaderHoldingTheManifest(t *testing.T) {
	var f *os.File
	opts := testOptions()
	opts.hooks = &testHooks{at: func(point string) error {
		if point == pointManifestWritten && f != nil {
			// The reader lets go 60 ms after manifest.tmp is ready to be renamed.
			held := f
			f = nil
			go func() {
				time.Sleep(60 * time.Millisecond)
				_ = held.Close()
			}()
		}
		return nil
	}}
	h := newHarness(t, opts)
	h.upsert("a")
	h.refresh()
	var err error
	f, err = os.Open(filepath.Join(h.dir, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	h.upsert("b")
	if err := h.s.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh while a reader held the manifest for 60 ms: %v", err)
	}
	h.check()
	h.reopen()
	h.check()
}

// The shard's own manifest reader (openShared) never stands in a commit's way: holding
// it open across a whole refresh, the commit still swaps the manifest, and the held
// handle goes on reading the manifest it opened (POSIX semantics, on Windows too).
func TestOurManifestReaderNeverBlocksACommit(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	path := filepath.Join(h.dir, manifestName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := openShared(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h.upsert("b")
	start := time.Now()
	h.refresh()
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Logf("refresh took %v with our reader open (slow disk?)", d)
	}
	held, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(held, before) {
		t.Fatal("the held handle does not read the manifest it opened")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(after, before) {
		t.Fatal("the manifest was not replaced")
	}
	h.check()
}
