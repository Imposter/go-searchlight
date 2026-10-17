package cluster

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/store"
)

// TestOlderMappingReaders: a node that reads no point type counts while it heartbeats
// within the fence or holds a live lease; a lease of a node the registry does not list
// counts too.
func TestOlderMappingReaders(t *testing.T) {
	fence := 7 * time.Second
	id := store.ShardID{Index: "i"}
	newNode := store.Node{ID: "new", Version: "1.3.0", MappingTypes: store.MappingTypes}
	silentOld := store.Node{ID: "old", Version: "1.2.0", HeartbeatAge: 8 * time.Second}
	for _, tc := range []struct {
		name   string
		nodes  []store.Node
		copies []store.Copy
		want   []string
	}{
		{"every node reads points", []store.Node{newNode}, []store.Copy{{Shard: id, NodeID: "new", LeaseLeft: time.Second}}, nil},
		{"an old node heartbeating", []store.Node{newNode, {ID: "old", Version: "1.2.0", HeartbeatAge: time.Second}}, nil, []string{"old (1.2.0)"}},
		{"an old node silent past the fence", []store.Node{newNode, silentOld}, nil, nil},
		{"an old node silent but still leasing", []store.Node{newNode, silentOld}, []store.Copy{{Shard: id, NodeID: "old", LeaseLeft: time.Second}}, []string{"old (1.2.0)"}},
		{"an old node's lease run out", []store.Node{newNode, silentOld}, []store.Copy{{Shard: id, NodeID: "old", LeaseLeft: -time.Second}}, nil},
		{"a lease of an unregistered node", []store.Node{newNode}, []store.Copy{{Shard: id, NodeID: "gone", LeaseLeft: time.Second}}, []string{"gone (unregistered)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &view{copies: map[store.ShardID][]store.Copy{}}
			for _, c := range tc.copies {
				v.copies[c.Shard] = append(v.copies[c.Shard], c)
			}
			if got := olderMappingReaders(tc.nodes, v, fence); !slices.Equal(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// While a node of a release before point fields heartbeats, creating an index with a
// point field and patching one into a mapping are refused (409, naming the node), and
// nothing is written; mappings without one are not held back. Once the old node is
// gone, both go through.
func TestPointFieldsWaitForEveryNode(t *testing.T) {
	db := sqliteDB(t)
	c := newCluster(t, db, func(int, *Options) {})
	a := c.start(0)
	createIndex(t, a.n, "plain", 1, 0)
	old := store.Node{ID: "node-old", Address: "127.0.0.1:1", Version: "1.2.0", BodyCodecs: store.BodyCodecs}
	if err := a.st.Registry().Heartbeat(tctx(t), old); err != nil {
		t.Fatal(err)
	}
	point := schema.PointType(schema.Number, schema.Number)
	refused := func(what string, err error) {
		t.Helper()
		var ae *api.Error
		if !errors.As(err, &ae) || ae.Status != http.StatusConflict || !strings.Contains(ae.Detail, "node-old (1.2.0)") || !strings.Contains(ae.Detail, "spot") {
			t.Fatalf("%s while an older node serves: %v; want a 409 naming it", what, err)
		}
	}
	_, err := a.n.CreateIndex(tctx(t), "pts", api.IndexSpec{Mapping: &schema.Mapping{Fields: map[string]schema.FieldType{"spot": point}}, Settings: api.IndexSettings{Shards: 1}})
	refused("creating an index with a point field", err)
	if _, err := a.n.GetIndex(tctx(t), "pts"); err == nil {
		t.Fatal("the refused index exists")
	}
	_, err = a.n.PatchMapping(tctx(t), "plain", map[string]schema.FieldType{"spot": point})
	refused("patching in a point field", err)
	if _, err := a.n.PatchMapping(tctx(t), "plain", map[string]schema.FieldType{"seen": schema.Date}); err != nil {
		t.Fatalf("a mapping without points while an older node serves: %v", err)
	}
	if err := a.st.Registry().RemoveNode(tctx(t), old.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := a.n.PatchMapping(tctx(t), "plain", map[string]schema.FieldType{"spot": point}); err != nil {
		t.Fatalf("patching in a point field once every node reads it: %v", err)
	}
	if _, err := a.n.CreateIndex(tctx(t), "pts", api.IndexSpec{Mapping: &schema.Mapping{Fields: map[string]schema.FieldType{"spot": point}}, Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatalf("creating a point index once every node reads it: %v", err)
	}
}
