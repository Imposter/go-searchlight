package reqcache

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newTestRedis(t *testing.T, o RedisOptions) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	if o.URL == "" {
		o.URL = "redis://" + mr.Addr() + "/0"
	}
	s, err := NewRedis(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, mr
}

func TestRedisStoreGetSet(t *testing.T) {
	t.Parallel()
	s, mr := newTestRedis(t, RedisOptions{Prefix: "p:", TTL: time.Hour})
	ctx := context.Background()
	if v, err := s.Get(ctx, "k"); v != nil || err != nil {
		t.Fatalf("Get of a missing key = %q, %v", v, err)
	}
	if err := s.Set(ctx, "k", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get(ctx, "k"); string(v) != "value" || err != nil {
		t.Fatalf("Get = %q, %v", v, err)
	}
	if !mr.Exists("p:k") {
		t.Fatal("the key was not written under its prefix")
	}
	if ttl := mr.TTL("p:k"); ttl != time.Hour {
		t.Fatalf("TTL %v, want 1h", ttl)
	}
	mr.FastForward(2 * time.Hour)
	if v, _ := s.Get(ctx, "k"); v != nil {
		t.Fatal("the entry outlived its TTL")
	}
}

func TestRedisStoreRefusesLargeValues(t *testing.T) {
	t.Parallel()
	s, mr := newTestRedis(t, RedisOptions{MaxEntryBytes: 4})
	if err := s.Set(context.Background(), "k", []byte("12345")); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Set = %v, want ErrTooLarge", err)
	}
	if len(mr.Keys()) != 0 {
		t.Fatal("a value past MaxEntryBytes was written")
	}
	if s.MaxEntryBytes() != 4 || s.Name() != "redis" {
		t.Fatal("options not kept")
	}
}

func TestRedisStoreDatabaseAndPassword(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	mr.RequireAuth("sesame")
	s, err := NewRedis(RedisOptions{URL: "redis://" + mr.Addr() + "/3", Password: "sesame"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Set(context.Background(), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	mr.Select(3)
	if !mr.Exists(DefaultRedisPrefix + "k") {
		t.Fatal("the key is not in database 3 under the default prefix")
	}
	wrong, err := NewRedis(RedisOptions{URL: "redis://:nope@" + mr.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.Close()
	if _, err := wrong.Get(context.Background(), "k"); err == nil {
		t.Fatal("a wrong password read")
	}
}

// TestRedisStoreDown: an unreachable Redis fails each call within the timeout.
func TestRedisStoreDown(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	addr := mr.Addr()
	mr.Close()
	s, err := NewRedis(RedisOptions{URL: "redis://" + addr, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now()
	for range 3 {
		if _, err := s.Get(context.Background(), "k"); err == nil {
			t.Fatal("a Get of a closed Redis succeeded")
		}
		if err := s.Set(context.Background(), "k", []byte("v")); err == nil {
			t.Fatal("a Set of a closed Redis succeeded")
		}
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("six failing calls took %v", d)
	}
}

func TestRedisURLErrorsHideThePassword(t *testing.T) {
	t.Parallel()
	_, err := NewRedis(RedisOptions{URL: "http://user:hunter2@host"})
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("NewRedis = %v: want an error without the password", err)
	}
}

// TestRedisStoreReadsNoMoreThanMaxEntry: a value past MaxEntryBytes in Redis (written
// by another client) is ErrTooLarge, read only one byte past the bound.
func TestRedisStoreReadsNoMoreThanMaxEntry(t *testing.T) {
	t.Parallel()
	s, mr := newTestRedis(t, RedisOptions{MaxEntryBytes: 8})
	if err := mr.Set(DefaultRedisPrefix+"big", strings.Repeat("x", 1000)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(context.Background(), "big"); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Get = %v, want ErrTooLarge", err)
	}
	if err := mr.Set(DefaultRedisPrefix+"ok", "12345678"); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get(context.Background(), "ok"); string(v) != "12345678" || err != nil {
		t.Fatalf("Get = %q, %v", v, err)
	}
}
