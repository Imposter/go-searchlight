package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeApplier assigns seqs like the store and records each transaction.
type fakeApplier struct {
	mu      sync.Mutex
	next    int64
	batches [][]Change
	fail    func(batch []Change) error
	block   chan struct{} // when set, Apply waits for it or for ctx
	ended   int           // Apply calls that have returned
}

func (f *fakeApplier) Apply(ctx context.Context, batch []Change) (int64, int64, error) {
	defer func() {
		f.mu.Lock()
		f.ended++
		f.mu.Unlock()
	}()
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, append([]Change(nil), batch...))
	if f.fail != nil {
		if err := f.fail(batch); err != nil {
			return 0, 0, err
		}
	}
	first := f.next + 1
	f.next += int64(len(batch))
	return first, f.next, nil
}

func (f *fakeApplier) txCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func changes(tag string, n int) []Change {
	out := make([]Change, n)
	for i := range out {
		out[i] = Change{Index: "i", Kind: KindUpsert, ID: fmt.Sprintf("%s-%d", tag, i), Payload: []byte("{}")}
	}
	return out
}

func TestGroupCommitBatches(t *testing.T) {
	f := &fakeApplier{}
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: 20 * time.Millisecond})
	defer g.Close()

	type res struct {
		tag         string
		first, last int64
	}
	var mu sync.Mutex
	var got []res
	var wg sync.WaitGroup
	for c := range 50 {
		wg.Go(func() {
			tag := fmt.Sprintf("c%d", c)
			first, last, err := g.Apply(context.Background(), changes(tag, 1+c%4))
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			got = append(got, res{tag, first, last})
			mu.Unlock()
		})
	}
	wg.Wait()
	if n := f.txCount(); n >= 50 || int64(n) != g.Flushes() {
		t.Fatalf("%d transactions (flushes %d) for 50 requests", n, g.Flushes())
	}
	// Each caller's range holds exactly its own changes, and the ranges
	// tile the sequence.
	all := map[int64]string{}
	for _, b := range f.batches {
		for _, c := range b {
			all[int64(len(all)+1)] = c.ID
		}
	}
	sort.Slice(got, func(i, j int) bool { return got[i].first < got[j].first })
	next := int64(1)
	for _, r := range got {
		if r.first != next {
			t.Fatalf("range %d..%d, want start %d", r.first, r.last, next)
		}
		for s := r.first; s <= r.last; s++ {
			if want := fmt.Sprintf("%s-%d", r.tag, s-r.first); all[s] != want {
				t.Fatalf("seq %d holds %s, want %s", s, all[s], want)
			}
		}
		next = r.last + 1
	}
}

func TestGroupCommitMaxChanges(t *testing.T) {
	f := &fakeApplier{}
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: 50 * time.Millisecond, MaxChanges: 1000})
	defer g.Close()
	var wg sync.WaitGroup
	for c := range 6 {
		wg.Go(func() {
			if _, _, err := g.Apply(context.Background(), changes(fmt.Sprint(c), 400)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Go(func() {
		if _, _, err := g.Apply(context.Background(), changes("big", 1500)); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
	total := 0
	for _, b := range f.batches {
		if len(b) > 1000 && len(b) != 1500 {
			t.Fatalf("a transaction of %d changes", len(b))
		}
		total += len(b)
	}
	if total != 6*400+1500 {
		t.Fatalf("committed %d changes", total)
	}
}

func TestGroupCommitFlushesAfterDelay(t *testing.T) {
	f := &fakeApplier{}
	g := NewGroupCommitter(f, GroupCommitOptions{})
	defer g.Close()
	start := time.Now()
	if _, _, err := g.Apply(context.Background(), changes("solo", 1)); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a lone request waited %s", took)
	}
}

func TestGroupCommitErrorReachesEveryCaller(t *testing.T) {
	boom := errors.New("database down")
	f := &fakeApplier{fail: func([]Change) error { return boom }}
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: 30 * time.Millisecond})
	defer g.Close()
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for c := range errs {
		wg.Go(func() { _, _, errs[c] = g.Apply(context.Background(), changes(fmt.Sprint(c), 2)) })
	}
	wg.Wait()
	for c, err := range errs {
		if !errors.Is(err, boom) {
			t.Fatalf("caller %d got %v", c, err)
		}
	}
	if f.txCount() >= 10 {
		t.Fatalf("%d transactions: the error was not shared by a batch", f.txCount())
	}
}

func TestGroupCommitConflictIsPerRequest(t *testing.T) {
	// Any batch containing "bad-1" fails with a conflict on that change.
	f := &fakeApplier{fail: func(batch []Change) error {
		for i, c := range batch {
			if c.ID == "bad-1" {
				return &ConflictError{Positions: []int{i}, Current: []int64{9}}
			}
		}
		return nil
	}}
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: 50 * time.Millisecond})
	defer g.Close()
	var wg sync.WaitGroup
	errs := map[string]error{}
	var mu sync.Mutex
	for _, tag := range []string{"a", "bad", "b", "c"} {
		wg.Go(func() {
			_, _, err := g.Apply(context.Background(), changes(tag, 3))
			mu.Lock()
			errs[tag] = err
			mu.Unlock()
		})
	}
	wg.Wait()
	var ce *ConflictError
	if !errors.As(errs["bad"], &ce) || fmt.Sprint(ce.Positions) != "[1]" || ce.Current[0] != 9 {
		t.Fatalf("conflicting request got %v", errs["bad"])
	}
	for _, tag := range []string{"a", "b", "c"} {
		if errs[tag] != nil {
			t.Fatalf("request %s failed with %v", tag, errs[tag])
		}
	}
	if f.next != 9 {
		t.Fatalf("committed %d changes, want 9", f.next)
	}
}

func TestSplitPositions(t *testing.T) {
	reqs := []*gcRequest{{changes: make([]Change, 2)}, {changes: make([]Change, 3)}, {changes: make([]Change, 1)}}
	got := splitPositions(reqs, []int{1, 2, 4, 5}, []int64{10, 20, 40, 50})
	if len(got) != 3 || fmt.Sprint(got[0].Positions, got[1].Positions, got[2].Positions) != "[1] [0 2] [0]" ||
		fmt.Sprint(got[1].Current) != "[20 40]" {
		t.Fatalf("split %v %v %v", got[0], got[1], got[2])
	}
}

func TestGroupCommitCancellation(t *testing.T) {
	f := &fakeApplier{block: make(chan struct{})}
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: time.Millisecond})
	defer g.Close()

	// A caller that gives up while its transaction runs gets its context's
	// error; with nobody left waiting, the transaction is cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := g.Apply(ctx, changes("x", 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		ended := f.ended
		f.mu.Unlock()
		if ended == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(f.block)
	// The cancelled transaction never reached the store.
	if _, _, err := g.Apply(context.Background(), changes("y", 1)); err != nil {
		t.Fatal(err)
	}
	if f.next != 1 || len(f.batches) != 1 || f.batches[0][0].ID != "y-0" {
		t.Fatalf("store saw %v", f.batches)
	}

	// An already-cancelled context never enqueues.
	done, cancelDone := context.WithCancel(context.Background())
	cancelDone()
	if _, _, err := g.Apply(done, changes("z", 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestGroupCommitClose(t *testing.T) {
	f := &fakeApplier{}
	collected := make(chan struct{}, 1)
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: time.Hour, received: func() { collected <- struct{}{} }})
	res := make(chan error, 1)
	go func() {
		_, _, err := g.Apply(context.Background(), changes("pending", 2))
		res <- err
	}()
	// The request is collected but would wait an hour for company; Close
	// flushes it.
	<-collected
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-res; err != nil {
		t.Fatalf("pending request: %v", err)
	}
	if _, _, err := g.Apply(context.Background(), changes("late", 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("apply after close: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
}

// gather starts the requests in order, each only after the committer has
// taken the one before into its batch, and returns their results.
func gather(t *testing.T, g *GroupCommitter, collected <-chan struct{}, ctxs []context.Context, batches [][]Change) []gcResult {
	t.Helper()
	out := make([]gcResult, len(batches))
	var wg sync.WaitGroup
	for i, b := range batches {
		ctx := context.Background()
		if i < len(ctxs) && ctxs[i] != nil {
			ctx = ctxs[i]
		}
		wg.Go(func() {
			first, last, err := g.Apply(ctx, b)
			out[i] = gcResult{first, last, err}
		})
		<-collected
	}
	wg.Wait()
	return out
}

func TestGroupCommitInvalidRequestFailsAlone(t *testing.T) {
	f := &fakeApplier{}
	collected := make(chan struct{}, 8)
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: time.Hour, MaxChanges: 4, received: func() { collected <- struct{}{} }})
	defer g.Close()
	bad := changes("bad", 3)
	bad[2].Payload = []byte("{\"x\":\"\xff\"}") // invalid UTF-8
	if _, _, err := g.Apply(context.Background(), bad); err == nil {
		t.Fatal("invalid request accepted")
	} else {
		var ce *ChangeError
		if !errors.As(err, &ce) || ce.Position != 2 || !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid request got %v", err)
		}
	}
	res := gather(t, g, collected, nil, [][]Change{changes("a", 2), changes("b", 2)})
	for i, r := range res {
		if r.err != nil {
			t.Fatalf("request %d: %v", i, r.err)
		}
	}
	if f.txCount() != 1 {
		t.Fatalf("%d transactions", f.txCount())
	}
}

func TestGroupCommitMissingIndexFailsAlone(t *testing.T) {
	// Any batch naming index "gone" fails like the store does.
	f := &fakeApplier{fail: func(batch []Change) error {
		var e *IndexNotFoundError
		for i, c := range batch {
			if c.Index == "gone" {
				if e == nil {
					e = &IndexNotFoundError{Indexes: []string{"gone"}}
				}
				e.Positions = append(e.Positions, i)
			}
		}
		if e != nil {
			return e
		}
		return nil
	}}
	collected := make(chan struct{}, 8)
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: time.Hour, MaxChanges: 7, received: func() { collected <- struct{}{} }})
	defer g.Close()
	gone := changes("g", 3)
	gone[1].Index = "gone"
	res := gather(t, g, collected, nil, [][]Change{changes("a", 2), gone, changes("b", 2)})
	var nf *IndexNotFoundError
	if !errors.As(res[1].err, &nf) || fmt.Sprint(nf.Positions) != "[1]" || !errors.Is(res[1].err, ErrNotFound) {
		t.Fatalf("request naming a missing index got %v", res[1].err)
	}
	if res[0].err != nil || res[2].err != nil || res[0].first != 1 || res[2].first != 3 {
		t.Fatalf("other requests: %+v %+v", res[0], res[2])
	}
}

// TestGroupCommitMissingIndexNamesOwnIndex checks that when two requests in
// the same batch each name a different missing index, each one's error
// names only its own index, not the whole batch's.
func TestGroupCommitMissingIndexNamesOwnIndex(t *testing.T) {
	f := &fakeApplier{fail: func(batch []Change) error {
		// Mirrors sql.go's checkIndexes: Indexes is every missing name in
		// the combined batch, first-use order; Positions is every position
		// naming any of them.
		var e *IndexNotFoundError
		seen := map[string]bool{}
		for i, c := range batch {
			if c.Index == "gone1" || c.Index == "gone2" {
				if e == nil {
					e = &IndexNotFoundError{}
				}
				if !seen[c.Index] {
					seen[c.Index] = true
					e.Indexes = append(e.Indexes, c.Index)
				}
				e.Positions = append(e.Positions, i)
			}
		}
		if e != nil {
			return e
		}
		return nil
	}}
	collected := make(chan struct{}, 8)
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: time.Hour, MaxChanges: 4, received: func() { collected <- struct{}{} }})
	defer g.Close()
	req1 := changes("x", 2)
	req1[0].Index = "gone1"
	req2 := changes("y", 2)
	req2[1].Index = "gone2"
	res := gather(t, g, collected, nil, [][]Change{req1, req2})

	var nf1, nf2 *IndexNotFoundError
	if !errors.As(res[0].err, &nf1) || fmt.Sprint(nf1.Indexes) != "[gone1]" || fmt.Sprint(nf1.Positions) != "[0]" {
		t.Fatalf("request 1 (names gone1): %v", res[0].err)
	}
	if !errors.As(res[1].err, &nf2) || fmt.Sprint(nf2.Indexes) != "[gone2]" || fmt.Sprint(nf2.Positions) != "[1]" {
		t.Fatalf("request 2 (names gone2): %v", res[1].err)
	}
}

// A request held for a conflict caused by an earlier request is
// re-evaluated when that earlier request's caller gives up.
func TestGroupCommitHeldConflictReopens(t *testing.T) {
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	f := &fakeApplier{}
	f.fail = func(batch []Change) error {
		// B conflicts only when A is in the batch before it.
		for i, c := range batch {
			if c.ID == "B-0" && i > 0 && batch[0].ID == "A-0" {
				cancelA() // A's caller leaves while the transaction runs
				return &ConflictError{Positions: []int{i}, Current: []int64{1}}
			}
		}
		return nil
	}
	collected := make(chan struct{}, 8)
	g := NewGroupCommitter(f, GroupCommitOptions{MaxDelay: time.Hour, MaxChanges: 2, received: func() { collected <- struct{}{} }})
	defer g.Close()
	res := gather(t, g, collected, []context.Context{ctxA, nil}, [][]Change{changes("A", 1), changes("B", 1)})
	if !errors.Is(res[0].err, context.Canceled) {
		t.Fatalf("A got %v", res[0].err)
	}
	if res[1].err != nil || res[1].first != 1 {
		t.Fatalf("B got %+v; its conflict depended on A, which never committed", res[1])
	}
}
