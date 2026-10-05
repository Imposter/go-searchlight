//go:build chaos

package chaos

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/node"
)

// TestKillMidBulk: a node killed (SIGKILL) while _bulk requests are in flight on it
// loses no acknowledged write; the load fails over to the other nodes, and the node
// rejoins and converges.
func TestKillMidBulk(t *testing.T) {
	c := newCluster(t, 3, "")
	c.createIndex()
	m := newModel()
	l := c.startLoad(m, 4, 3)
	l.waitWrites(t, 2000)
	c.waitDurable(1)
	waitUntil(t, time.Minute, "a _bulk in flight on chaos-2", func() bool { return c.members[1].bulks.Load() > 0 })
	c.kill(1)
	l.waitWrites(t, 3000)
	c.restart(1)
	c.assertReopened(1, true)
	l.waitWrites(t, 1000)
	l.stop()
	c.settle(m)
	c.verify(m)
	if c.lb.retriedOf("failover refused")+c.lb.retriedOf("failover cut") == 0 {
		t.Error("no request failed over: the kill hit no request in flight")
	}
}

// TestKillMidMerge: a node killed while its merges run (a slow merge budget keeps
// them running) reopens from its last durable manifest, loses nothing and converges.
func TestKillMidMerge(t *testing.T) {
	c := newCluster(t, 3, "", "refresh_interval=50ms", "merge_budget=256KiB", "merge_threads=1")
	c.createIndex()
	m := newModel()
	l := c.startLoad(m, 4, 2)
	l.waitWrites(t, 2000)
	c.waitDurable(1)
	var merging float64
	waitUntil(t, 3*time.Minute, "a merge running on chaos-2", func() bool {
		merging = c.metric(1, "searchlight_shard_merge_backlog")
		return merging > 0
	})
	c.kill(1)
	t.Logf("killed chaos-2 with %.0f segments merging", merging)
	orphans := orphanSegments(t, c.members[1].DataDir())
	l.waitWrites(t, 2000)
	c.restart(1)
	c.assertReopened(1, true)
	for _, path := range orphans {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("%s, which no durable manifest lists (a merge output the kill cut short), was not collected at the reopen: %v", path, err)
		}
	}
	t.Logf("the reopen collected %d segment files no durable manifest listed", len(orphans))
	l.waitWrites(t, 1000)
	l.stop()
	c.settle(m)
	c.verify(m)
}

// TestKillMidRecovery: a node with an empty data directory, killed while it recovers
// its copies from its peers (once the first copy has been fetched, while the others
// transfer or replay the changelog), comes back, recovers them again and converges; the
// load never sees an error.
func TestKillMidRecovery(t *testing.T) {
	c := newCluster(t, 3, "")
	c.createIndex()
	m := newModel()
	l := c.startLoad(m, 4, 2)
	l.waitWrites(t, 12000)

	r := c.members[2]
	r.disrupted.Store(true)
	if err := r.Stop(time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(r.DataDir()); err != nil {
		t.Fatal(err)
	}
	l.waitWrites(t, 1000)
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Logs().Wait(t.Context(), "shard copy recovered from a peer", r.Exited()); err != nil {
		t.Fatalf("chaos-3 recovered no copy from a peer: %v", err)
	}
	c.kill(2)
	recovered := len(r.Logs().All("shard copy recovered from a peer"))
	serving := 0
	for _, l := range r.Logs().All("shard copy state changed") {
		if l["state"] == "serving" {
			serving++
		}
	}
	t.Logf("killed chaos-3 mid-recovery, once its first copy had been fetched: %d of %d copies fetched, %d serving", recovered, shards, serving)
	if serving >= shards {
		t.Fatalf("every copy of chaos-3 served before the kill: it hit no recovery (load more data before it)")
	}
	l.waitWrites(t, 1000)
	start := time.Now()
	c.restart(2)
	c.assertReopened(2, false)
	t.Logf("chaos-3 recovered its copies again and was ready %s after its restart", time.Since(start).Round(time.Millisecond))
	l.waitWrites(t, 1000)
	l.stop()
	c.settle(m)
	c.verify(m)
}

// TestDatabaseRestart: the database restarting under load costs writes a few retried
// 503s while it is down, never an acknowledged write, and the nodes reconnect and
// converge. The restart is SEARCHLIGHT_CHAOS_DB_RESTART's (in CI, docker restart of the
// Postgres service container).
func TestDatabaseRestart(t *testing.T) {
	cmdline := os.Getenv(envDBRestart)
	if cmdline == "" {
		t.Skip(envDBRestart + " is not set: no command to restart the database with")
	}
	c := newCluster(t, 3, "")
	c.write503 = true
	c.createIndex()
	m := newModel()
	l := c.startLoad(m, 4, 3)
	l.waitWrites(t, 2000)
	restartDatabase(t, cmdline, filepath.Join(c.dir, "db-restart.log"))
	l.waitWrites(t, 3000)
	l.stop()
	c.settle(m)
	c.verify(m)
	if c.lb.retriedOf("write 503") == 0 && !c.sawDatabaseErrors() {
		t.Error("no write was refused and no node logged a database error: the restart was not felt")
	}
}

// sawDatabaseErrors reports whether a node logged that it could not reach the database.
func (c *cluster) sawDatabaseErrors() bool {
	for _, m := range c.members {
		for _, msg := range []string{"heartbeat failed", "renewing the leases failed; a copy past its deadline pauses (no peer reads it) until a renewal succeeds", "tailer retrying after a failure"} {
			if m.Logs().Find(msg) != nil {
				return true
			}
		}
	}
	return false
}

// TestCorruptSegment: a segment damaged on disk while its node was down fails its
// checksum when the node opens it; the copy is wiped and rebuilt from a peer, never
// served, and converges.
func TestCorruptSegment(t *testing.T) {
	c := newCluster(t, 3, "")
	c.createIndex()
	m := newModel()
	l := c.startLoad(m, 3, 2)
	l.waitWrites(t, 6000)

	r := c.members[1]
	r.disrupted.Store(true)
	if err := r.Stop(time.Minute); err != nil {
		t.Fatal(err)
	}
	path := largestSegment(t, r.DataDir())
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(raw) / 2; i < len(raw)/2+16; i++ {
		raw[i] ^= 0xa5
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("corrupted %s (%d bytes)", path, len(raw))
	c.restart(1)
	wiped := r.Logs().Find("shard copy does not open; wiping it to rebuild")
	if wiped == nil {
		t.Fatal("chaos-2 did not detect the corrupt segment")
	}
	if !strings.Contains(fmt.Sprint(wiped["error"]), "corrupt") {
		t.Errorf("the copy was wiped for %v, not the corruption", wiped["error"])
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the corrupt segment is still on disk: %v", err)
	}
	l.waitWrites(t, 1000)
	l.stop()
	c.settle(m)
	c.verify(m)
}

func largestSegment(t *testing.T, dataDir string) string {
	t.Helper()
	var best string
	var size int64
	err := filepath.WalkDir(filepath.Join(dataDir, "indexes"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".seg" {
			return err
		}
		man, err := os.ReadFile(filepath.Join(filepath.Dir(path), "manifest"))
		if err != nil {
			return err
		}
		if !strings.Contains(string(man), `"id":"`+strings.TrimSuffix(d.Name(), ".seg")+`"`) {
			return nil // left by a refresh or merge the stop cut short: the next open removes it
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Size() > size {
			best, size = path, info.Size()
		}
		return nil
	})
	if err != nil || best == "" {
		t.Fatalf("no segment under %s: %v", dataDir, err)
	}
	return best
}

// TestRollingUpgrade: under write and read load, each node in turn is stopped
// gracefully (SIGTERM: it drains, exits 0) and started again on the new build, waiting
// for the cluster to be green before the next one; no client sees an error, nothing
// acknowledged is lost, and every node ends on the new version.
func TestRollingUpgrade(t *testing.T) {
	old, cur := binaries(t)
	c := newCluster(t, 3, old)
	c.refusedOnly = true
	c.createIndex()
	m := newModel()
	l := c.startLoad(m, 4, 3)
	l.waitWrites(t, 2000)
	for i, mb := range c.members {
		mb.disrupted.Store(true)
		start := time.Now()
		if err := mb.Stop(time.Minute); err != nil {
			t.Fatal(err)
		}
		mb.UseBinary(cur)
		c.restart(i)
		c.assertReopened(i, true)
		c.waitGreen(3 * time.Minute)
		t.Logf("upgraded %s in %s", mb.NodeID(), time.Since(start).Round(time.Millisecond))
		l.waitWrites(t, 1000)
	}
	l.stop()
	c.settle(m)
	c.verify(m)
	a, err := call(t.Context(), c.lb.client, http.MethodGet, c.members[0].URL+"/_cluster/nodes", "")
	if err != nil {
		t.Fatal(err)
	}
	var nodes struct {
		Nodes []struct {
			ID      string `json:"id"`
			Version string `json:"version"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(a.body, &nodes); err != nil || len(nodes.Nodes) != len(c.members) {
		t.Fatalf("/_cluster/nodes: %v %s", err, a.body)
	}
	for _, n := range nodes.Nodes {
		if n.Version != newVersion {
			t.Errorf("%s runs %s after the upgrade, want %s", n.ID, n.Version, newVersion)
		}
	}
}

// TestRestartTimeIsFlat: a node's restart to serving (T9: from the new process's start
// until its own copies serve every document) does not grow with the index's size: it
// reopens its segments and replays the changelog's tail, never rebuilds. Measured at
// one index size and again at twenty times it.
func TestRestartTimeIsFlat(t *testing.T) {
	c := newCluster(t, 3, "")
	c.createIndex()
	m := newModel()
	c.fill(m, 0, 2000)
	c.settle(m)
	small := c.timeRestart(0, m)
	c.fill(m, 2000, 38000)
	c.settle(m)
	large := c.timeRestart(0, m)
	t.Logf("restart to serving: %s at 2,000 documents, %s at 40,000", small.Round(time.Millisecond), large.Round(time.Millisecond))
	if large > 2*small+500*time.Millisecond {
		t.Errorf("a restart took %s at 40,000 documents against %s at 2,000: it grows with the index", large, small)
	}
	c.verify(m)
}

// fill writes count documents, from number from, in batches through the balancer.
func (c *cluster) fill(m *model, from, count int) {
	c.t.Helper()
	const batch = 1000
	for at := from; at < from+count; at += batch {
		var body strings.Builder
		ids := map[string]doc{}
		for k := at; k < min(at+batch, from+count); k++ {
			id := fmt.Sprintf("f-%06d", k)
			d := doc{v: 1, n: 1_000_000 + k, brand: fmt.Sprintf("b%d", k%7)}
			ids[id] = d
			fmt.Fprintf(&body, "{\"upsert\": {\"id\": %q}}\n{\"w\": \"fill\", \"k\": %d, \"v\": 1, \"n\": %d, \"brand\": %q, \"title\": \"filled item %d\"}\n", id, k, d.n, d.brand, k)
		}
		a, err := c.lb.do(c.t.Context(), http.MethodPost, "/indexes/"+index+"/_bulk", body.String(), 3*time.Minute, true)
		if err != nil {
			c.t.Fatal(err)
		}
		var resp struct {
			Seq    int64 `json:"seq"`
			Errors bool  `json:"errors"`
		}
		if a.status != http.StatusOK || json.Unmarshal(a.body, &resp) != nil || resp.Errors {
			c.t.Fatalf("fill: HTTP %d %s", a.status, truncate(a.body))
		}
		m.mu.Lock()
		for id, d := range ids {
			m.docs[id] = d
		}
		m.head = max(m.head, resp.Seq)
		m.acked += int64(len(ids))
		m.mu.Unlock()
	}
}

// timeRestart stops node i gracefully, starts it again and returns how long it took
// from the start until its own copies serve every document. The restarted node must
// have reopened its copies, not rebuilt them.
func (c *cluster) timeRestart(i int, m *model) time.Duration {
	c.t.Helper()
	v := m.snapshot()
	perShard := make([]int64, shards)
	for id := range v.docs {
		perShard[node.ShardFor(id, shards)]++
	}
	mb := c.members[i]
	mb.disrupted.Store(true)
	if err := mb.Stop(time.Minute); err != nil {
		c.t.Fatal(err)
	}
	start := time.Now()
	if err := mb.Start(c.t.Context()); err != nil {
		c.t.Fatal(err)
	}
	eventually(c.t, 5*time.Minute, mb.NodeID()+" serves its copies again", func() error {
		copies, err := c.ownCopies(c.t.Context(), mb)
		if err != nil {
			return err
		}
		if len(copies) != shards {
			return fmt.Errorf("%d copies", len(copies))
		}
		for _, sc := range copies {
			if sc.State != "serving" || sc.Docs != perShard[sc.Shard] {
				return fmt.Errorf("shard %d: %s with %d of %d documents", sc.Shard, sc.State, sc.Docs, perShard[sc.Shard])
			}
		}
		return nil
	})
	took := time.Since(start)
	mb.disrupted.Store(false)
	if rebuilt := mb.Logs().All("shard copy must be rebuilt"); len(rebuilt) > 0 {
		c.t.Errorf("%s rebuilt copies on a restart rather than reopening them: %v", mb.NodeID(), rebuilt)
	}
	opened := 0
	for _, l := range mb.Logs().All("shard opened") {
		if docs, _ := l["documents"].(float64); docs > 0 {
			opened++
		}
	}
	if opened < shards {
		c.t.Errorf("%s reopened %d copies with documents, want %d", mb.NodeID(), opened, shards)
	}
	return took
}

func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertReopened checks that node i, just started again after a kill or a stop, found
// every copy's files whole: none was wiped as unopenable and, with reopen (the node
// had flushed copies), every copy reopened its segments with documents rather than
// being rebuilt.
func (c *cluster) assertReopened(i int, reopen bool) {
	c.t.Helper()
	logs := c.members[i].Logs()
	if wiped := logs.All("shard copy does not open; wiping it to rebuild"); len(wiped) > 0 {
		c.t.Errorf("%s found a copy it had written unopenable after the restart: %v", c.members[i].NodeID(), wiped)
	}
	if !reopen {
		return
	}
	if rebuilt := logs.All("shard copy must be rebuilt"); len(rebuilt) > 0 {
		c.t.Errorf("%s rebuilt copies on its restart rather than reopening them: %v", c.members[i].NodeID(), rebuilt)
	}
	withDocs := 0
	for _, l := range logs.All("shard opened") {
		if docs, _ := l["documents"].(float64); docs > 0 {
			withDocs++
		}
	}
	if withDocs < shards {
		c.t.Errorf("%s reopened %d copies holding documents, want %d", c.members[i].NodeID(), withDocs, shards)
	}
}

func orphanSegments(t *testing.T, dataDir string) []string {
	t.Helper()
	manifests, err := filepath.Glob(filepath.Join(dataDir, "indexes", "*", "*", "manifest"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, path := range manifests {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		_, body, _ := strings.Cut(string(raw), "\n")
		var man struct {
			Segments []struct {
				ID string `json:"id"`
			} `json:"segments"`
		}
		if err := json.Unmarshal([]byte(body), &man); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		listed := map[string]bool{}
		for _, s := range man.Segments {
			listed[s.ID+".seg"] = true
		}
		segs, err := filepath.Glob(filepath.Join(filepath.Dir(path), "*.seg"))
		if err != nil {
			t.Fatal(err)
		}
		for _, seg := range segs {
			if !listed[filepath.Base(seg)] {
				out = append(out, seg)
			}
		}
	}
	return out
}
