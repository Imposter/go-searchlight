package api

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// writePercolate writes exactly what encoding/json writes for the same response:
// ids that need escaping, given and stored documents, no match, stale or not.
func TestWritePercolateIsEncodingJSON(t *testing.T) {
	ids := [][]string{
		{"a", "b<c>&d", "q\"uote", "back\\slash", "tab\tline\u2028sep", "日本語"},
		nil,
		{"x"},
	}
	for _, stale := range []bool{false, true} {
		res := &PercolateResponse{Stale: stale}
		type plain struct {
			ID      string   `json:"id,omitempty"`
			Found   bool     `json:"found"`
			Queries []string `json:"queries"`
		}
		var want []plain
		for i, list := range ids {
			r := PercolateResult{Found: i != 1}
			if i == 2 {
				r.ID = "stored<1>"
			}
			p := plain{ID: r.ID, Found: r.Found, Queries: list}
			if list != nil {
				raw, err := json.Marshal(list)
				if err != nil {
					t.Fatal(err)
				}
				r.Queries = raw
			} else {
				p.Queries = []string{}
			}
			res.Results = append(res.Results, r)
			want = append(want, p)
		}
		out := map[string]any{"took_ms": int64(12), "took_us": int64(12345), "results": want}
		if stale {
			out["stale"] = true
		}
		wantBody, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		if err := writePercolate(rec, res, func() time.Duration { return 12345678 * time.Nanosecond }); err != nil {
			t.Fatal(err)
		}
		if got := rec.Body.Bytes(); !bytes.Equal(got, append(wantBody, '\n')) {
			t.Fatalf("stale=%v:\n got  %s\n want %s", stale, got, wantBody)
		}
		if got := rec.Header().Get("Server-Timing"); got != "total;dur=12.345" {
			t.Fatalf("Server-Timing %q", got)
		}
		if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(rec.Body.Len()) {
			t.Fatalf("Content-Length %q for %d bytes", got, rec.Body.Len())
		}
	}
}
