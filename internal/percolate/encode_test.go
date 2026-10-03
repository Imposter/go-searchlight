package percolate

import (
	"bytes"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
)

func TestEncodeQueryRoundTrips(t *testing.T) {
	src := `{"all":[{"field":"brand","op":"in","value":["a","b"]},{"not":{"field":"price","op":"between","value":[1,5]}}]}`
	n, ps := query.Parse([]byte(src))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	b, err := EncodeQuery(n)
	if err != nil {
		t.Fatal(err)
	}
	again, ps := query.Parse(b)
	if len(ps) > 0 || !bytes.Equal(query.Canonical(again), query.Canonical(n)) {
		t.Errorf("EncodeQuery = %s, which parses to another query (%v)", b, ps)
	}
}
