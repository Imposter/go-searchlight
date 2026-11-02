package cluster

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
)

// TestTokenOfRecreatedIndexOnLaggingNode: node 1 has not synced its catalogue since
// index "rc" was dropped and created again through node 0. A token of the new
// incarnation, fresh from a write on node 0, is the catalogue's: node 1 adopts that
// incarnation and answers it, where it refused it as a token of another incarnation.
func TestTokenOfRecreatedIndexOnLaggingNode(t *testing.T) {
	forSQLiteAndPostgres(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, func(i int, o *Options) {
			if i == 1 {
				o.CatalogInterval = time.Hour
			}
		})
		n0 := c.start(0).n
		n1 := c.start(1).n
		createIndex(t, n0, "rc", 1, 1)
		tok := mustWrite(t, n0, "rc", upsertOp("a", 1))
		eventually(t, 20*time.Second, "node 1 reads the first incarnation", func() error {
			_, err := count(tctx(t), n1, "rc", tok)
			return err
		})
		if err := n0.DeleteIndex(tctx(t), "rc"); err != nil {
			t.Fatal(err)
		}
		createIndex(t, n0, "rc", 1, 1)
		var tok2 *api.Token
		eventually(t, 20*time.Second, "a write to the new incarnation", func() error {
			res, err := n0.Write(tctx(t), "rc", []api.WriteOp{upsertOp("b", 2)}, api.WriteOptions{})
			if err != nil {
				return err
			}
			if res.Items[0].Err != nil {
				return res.Items[0].Err
			}
			tok2 = res.Token
			return nil
		})
		if tok2.UID == tok.UID {
			t.Fatal("the new incarnation has the old one's uid")
		}
		var refused error
		eventually(t, 20*time.Second, "node 1 reads the new incarnation's write", func() error {
			got, err := count(tctx(t), n1, "rc", tok2)
			if err != nil && api.ProblemFor(err).Status == http.StatusBadRequest {
				refused = err
				return nil
			}
			if err != nil {
				return err
			}
			if got != 1 {
				return fmt.Errorf("count = %d", got)
			}
			return nil
		})
		if refused != nil {
			t.Fatalf("node 1 refused a token of the catalogue's incarnation: %v", refused)
		}
		if _, err := count(tctx(t), n1, "rc", tok); err == nil || api.ProblemFor(err).Status != http.StatusBadRequest {
			t.Fatalf("a token of the dropped incarnation: %v, want a 400", err)
		}
	})
}

