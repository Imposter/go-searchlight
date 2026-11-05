package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBearerToken(t *testing.T) {
	for header, want := range map[string]struct {
		token string
		ok    bool
	}{
		"Bearer abc": {"abc", true}, "bearer  abc ": {"abc", true}, "Bearer ": {"", true},
		"": {"", false}, "Bearer": {"", false}, "Basic abc": {"", false},
	} {
		r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		if token, ok := BearerToken(r); token != want.token || ok != want.ok {
			t.Errorf("BearerToken(%q) = %q, %v; want %q, %v", header, token, ok, want.token, want.ok)
		}
	}
	a, b := SumToken("one-token"), SumToken("one-token")
	if c := SumToken("another"); a.Match(&b) != 1 || a.Match(&c) != 0 {
		t.Error("TokenSum.Match")
	}
}
