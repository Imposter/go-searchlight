package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/config"
)

// deadlineWriter is a ResponseWriter that takes read deadlines, as an HTTP/1 one does.
type deadlineWriter struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (w *deadlineWriter) SetReadDeadline(t time.Time) error {
	w.deadline = t
	return nil
}

func TestDrainBounds(t *testing.T) {
	cfg := config.Default()
	cfg.MaxBodyBytes = 64 << 10
	s := &Server{cfg: cfg}
	body := func() *trackedBody {
		return &trackedBody{r: io.NopCloser(io.LimitReader(strings.NewReader(strings.Repeat("x", 8<<20)), 8<<20))}
	}
	req := func(length int64, proto int) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/indexes/x/_search", http.NoBody)
		r.ContentLength, r.ProtoMajor = length, proto
		return r
	}
	cases := []struct {
		name   string
		w      http.ResponseWriter
		r      *http.Request
		status int
		want   int64
	}{
		{"a 413 drains up to max_body_bytes, at least 1 MiB", &deadlineWriter{ResponseRecorder: httptest.NewRecorder()}, req(-1, 1), http.StatusRequestEntityTooLarge, minDrainBytes},
		{"a 401 drains 256 KiB at most", &deadlineWriter{ResponseRecorder: httptest.NewRecorder()}, req(-1, 1), http.StatusUnauthorized, authDrainBytes},
		{"a 403 drains 256 KiB at most", &deadlineWriter{ResponseRecorder: httptest.NewRecorder()}, req(-1, 1), http.StatusForbidden, authDrainBytes},
		{"a body declared past the bound is not drained", &deadlineWriter{ResponseRecorder: httptest.NewRecorder()}, req(10<<20, 1), http.StatusRequestEntityTooLarge, 0},
		{"no read deadline, no drain", httptest.NewRecorder(), req(-1, 1), http.StatusRequestEntityTooLarge, 0},
		{"HTTP/2 needs no drain", &deadlineWriter{ResponseRecorder: httptest.NewRecorder()}, req(-1, 2), http.StatusRequestEntityTooLarge, 0},
	}
	for _, tc := range cases {
		start := time.Now()
		if got := s.drain(tc.w, tc.r, body(), tc.status); got != tc.want {
			t.Errorf("%s: drained %d bytes, want %d", tc.name, got, tc.want)
		}
		if dw, ok := tc.w.(*deadlineWriter); ok && tc.want > 0 {
			if d := dw.deadline.Sub(start); d <= 0 || d > drainTimeout+time.Second {
				t.Errorf("%s: read deadline %v from the start, want about %v", tc.name, d, drainTimeout)
			}
		}
	}
}
