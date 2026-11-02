package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestWlMappingShards: the shard count reaches the index's settings, and the mapping stays
// valid JSON.
func TestWlMappingShards(t *testing.T) {
	var spec struct {
		Settings struct {
			Shards int `json:"shards"`
		} `json:"settings"`
	}
	if err := json.Unmarshal(fmt.Appendf(nil, wlMapping, 64), &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Settings.Shards != 64 {
		t.Fatalf("shards = %d", spec.Settings.Shards)
	}
}

// TestScrapeSums adds a metric up over its label sets and leaves other metrics out.
func TestScrapeSums(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("# HELP x y\nmetric_a{shard=\"0\"} 2\nmetric_a{shard=\"1\"} 3.5\nmetric_ab 9\nmetric_b 7\n"))
	}))
	defer srv.Close()
	got := scrapeSums(context.Background(), srv.URL, "metric_a", "metric_b")
	if got["metric_a"] != 5.5 || got["metric_b"] != 7 {
		t.Fatalf("got %v", got)
	}
	if scrapeSums(context.Background(), "") != nil {
		t.Fatal("no admin URL should read nothing")
	}
}

func TestPGDelta(t *testing.T) {
	got := pgDelta(map[string]float64{"wal_bytes": 10}, map[string]float64{"wal_bytes": 25, "wal_sync": 3})
	if got["wal_bytes"] != 15 || got["wal_sync"] != 3 {
		t.Fatalf("got %v", got)
	}
}
