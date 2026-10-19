package cluster

import (
	"errors"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// The leader prunes each index's tombstones older than its gc_deletes: a versioned
// delete's version holds off lower versions until then, and a lower version applies
// after.
func TestLeaderPrunesTombstones(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"n": schema.Number}},
		Settings: api.IndexSettings{Shards: 1, GCDeletes: time.Second},
	}
	if _, err := a.n.CreateIndex(tctx(t), "tomb", spec); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, a.st, "tomb", 1, 1, time.Minute)
	write := func(op api.WriteOp) api.ItemResult {
		t.Helper()
		res, err := a.n.Write(tctx(t), "tomb", []api.WriteOp{op}, api.WriteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return res.Items[0]
	}
	if it := write(api.WriteOp{Kind: api.OpDelete, ID: "d", Version: 5}); it.Err == nil {
		t.Fatal("a versioned delete of nothing is a 404")
	}
	if it := write(api.WriteOp{Kind: api.OpUpsert, ID: "d", Body: []byte(`{"n":1}`), Version: 3}); !it.Noop {
		t.Fatalf("a write under the tombstone: %+v", it)
	}
	eventually(t, time.Minute, "the tombstone is pruned", func() error {
		if it := write(api.WriteOp{Kind: api.OpUpsert, ID: "d", Body: []byte(`{"n":1}`), Version: 3}); it.Noop {
			return errors.New("still a noop")
		}
		return nil
	})
}
