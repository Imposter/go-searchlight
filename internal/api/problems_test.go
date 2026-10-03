package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

func TestProblemFor(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{InvalidAt("query.all.0", "bad"), 400, CodeInvalid},
		{&FieldError{Loc: "shards", Message: "bad"}, 400, CodeInvalid},
		{&search.RequestError{Problems: []query.Problem{{Loc: "sort.0", Message: "bad"}}}, 400, CodeInvalid},
		{&schema.ValidationError{Field: "x", Message: "bad"}, 400, CodeInvalid},
		{fmt.Errorf("apply: %w", &store.ConflictError{Positions: []int{0}, Current: []int64{7}}), 409, CodeConflict},
		{fmt.Errorf("update: %w", store.ErrConflict), 409, CodeConflict},
		{store.ErrExists, 409, CodeIndexExists},
		{&store.IndexNotFoundError{Indexes: []string{"x"}, Positions: []int{0}}, 404, CodeNotFound},
		{fmt.Errorf("apply: %w", shard.ErrBackpressure), 429, CodeTooMany},
		{&shard.ChangeError{Err: shard.ErrDocTooLarge}, 413, CodeTooLarge},
		{context.DeadlineExceeded, 504, CodeTimeout},
		{context.Canceled, 499, CodeCanceled},
		{shard.ErrClosed, 503, CodeUnavailable},
		{fmt.Errorf("x: %w", shard.ErrFailed), 503, CodeUnavailable},
		{store.ErrClosed, 503, CodeUnavailable},
		{errors.New("boom"), 500, CodeInternal},
	}
	for _, tc := range cases {
		got := ProblemFor(tc.err)
		if got.Status != tc.status || got.Code != tc.code {
			t.Errorf("ProblemFor(%v) = %d %s, want %d %s", tc.err, got.Status, got.Code, tc.status, tc.code)
		}
	}
	if e := ProblemFor(&store.ConflictError{Positions: []int{0}, Current: []int64{7}}); e.Extra["current_seq"] != int64(7) {
		t.Errorf("a conflict's current_seq = %v", e.Extra)
	}
	if e := ProblemFor(errors.New("secret detail")); e.Detail != "internal error" {
		t.Errorf("a 500 leaks its cause: %q", e.Detail)
	}
}

func TestWriteProblem(t *testing.T) {
	w := httptest.NewRecorder()
	e := TooMany(1500*time.Millisecond, "slow down")
	e.Problems = []query.Problem{{Loc: "a.b", Message: "m"}}
	writeProblem(w, e, "rid")
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "2" || w.Header().Get("Content-Type") != ProblemContentType {
		t.Errorf("status %d, headers %v", w.Code, w.Header())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	want := `map[code:too_many_requests detail:slow down problems:[map[loc:a.b message:m]] request_id:rid status:429 title:Too Many Requests type:urn:searchlight:problem:too_many_requests]`
	if fmt.Sprint(body) != want {
		t.Errorf("body = %v\nwant %s", body, want)
	}
}

func TestIndexSettingsJSON(t *testing.T) {
	var s IndexSettings
	if err := json.Unmarshal([]byte(`{"shards": 4, "refresh_interval": "250ms", "replicas_per_shard": 2}`), &s); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(s)
	if string(b) != `{"shards":4,"replicas_per_shard":2,"refresh_interval":"250ms"}` {
		t.Errorf("round trip = %s", b)
	}
	if err := json.Unmarshal([]byte(`{"refresh_interval": -1}`), &s); err != nil || s.RefreshInterval != DisabledRefresh || s.Shards != DefaultShards {
		t.Errorf("-1: %+v %v", s, err)
	}
	b, _ = json.Marshal(s)
	if string(b) != `{"shards":1,"replicas_per_shard":0,"refresh_interval":-1}` {
		t.Errorf("disabled = %s", b)
	}
	for _, bad := range []string{`{"shards": 0}`, `{"shards": 5000}`, `{"replicas_per_shard": -1}`, `{"refresh_interval": "0s"}`, `{"refresh_interval": 5}`, `{"other": 1}`} {
		if err := json.Unmarshal([]byte(bad), &s); err == nil {
			t.Errorf("%s: want an error", bad)
		}
	}
}
