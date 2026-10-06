package shard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// The shard reports refresh latency, merge time and bytes, segment count, buffered
// documents, changes and filter-cache hits and misses.
func TestShardMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	opts := testOptions()
	opts.Meter = meter
	opts.FilterCache = NewFilterCache(1<<20, meter)
	opts.Index, opts.Shard = "products", 3
	h := newHarness(t, opts)
	h.upsert("a", "b")
	h.refresh()
	h.upsert("c")
	h.del("a")
	h.refresh()
	h.forceMerge(1)
	h.del("b")
	h.refresh() // a sidecar on the merged segment
	h.waitNoOrphans()
	h.s.opts.hooks = &testHooks{at: func(point string) error {
		if point == pointRefreshBuilt {
			return errors.New("disk full")
		}
		return nil
	}}
	h.upsert("e")
	if err := h.s.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh did not fail")
	}
	h.s.opts.hooks = nil
	h.s.jan.drain()
	if p := h.s.jan.pendingFiles(); len(p) != 0 {
		t.Fatalf("the failed refresh's files are still pending removal: %v", p)
	}
	h.refresh()
	g := h.s.Acquire()
	for range 2 {
		g.FilterCache().GetOrCompute(g.Segments[0].ID, "k", func() *roaring.Bitmap { return roaring.BitmapOf(1) })
	}
	g.Release()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			got[m.Name] = true
		}
	}
	for _, name := range []string{
		telemetry.MetricRefreshDuration, telemetry.MetricMergeDuration, telemetry.MetricMergeBytes,
		telemetry.MetricShardSegments, telemetry.MetricShardBufferDocs, telemetry.MetricShardDocuments,
		telemetry.MetricShardDiskSize, telemetry.MetricIndexChanges, telemetry.MetricSearchFilterCacheLooks,
		telemetry.MetricMergeBacklog, telemetry.MetricShardTerms, telemetry.MetricRefreshFailures,
	} {
		if !got[name] {
			t.Errorf("metric %s not reported (have %v)", name, got)
		}
	}
	if runtime.GOOS == "linux" || runtime.GOOS == "windows" {
		if !got[telemetry.MetricShardMmapResident] {
			t.Errorf("metric %s not reported on %s", telemetry.MetricShardMmapResident, runtime.GOOS)
		}
	}
	// Disk size counts the segments, their sidecars and the manifest.
	var want int64
	for _, name := range dirFiles(t, h.dir) {
		info, err := os.Stat(filepath.Join(h.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		want += info.Size()
	}
	if disk := gaugeValue(rm, telemetry.MetricShardDiskSize); int64(disk) != want {
		t.Errorf("disk size %v, want the %d bytes of every file the shard holds", disk, want)
	}
	st := h.s.opts.FilterCache.Stats()
	if st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("filter cache stats %+v, want one hit and one miss", st)
	}
}

// gaugeValue returns the last value of the float gauge name in rm.
func gaugeValue(rm metricdata.ResourceMetrics, name string) float64 {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok && m.Name == name && len(g.DataPoints) > 0 {
				return g.DataPoints[len(g.DataPoints)-1].Value
			}
		}
	}
	return -1
}

// M5: a persistent background failure is logged once a minute, not every tick.
func TestRateLimitedWarn(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	w := rateLimitedWarn{clock: clk, every: time.Hour}
	logged := 0
	for range 5 {
		if _, ok := w.allow(); ok {
			logged++
		}
	}
	if logged != 1 || w.suppressed != 4 {
		t.Fatalf("logged %d, suppressed %d; want 1 and 4", logged, w.suppressed)
	}
	clk.Advance(time.Hour - time.Nanosecond)
	if _, ok := w.allow(); ok {
		t.Fatal("logged again within the interval")
	}
	clk.Advance(time.Nanosecond)
	if n, ok := w.allow(); !ok || n != 5 {
		t.Fatalf("after the interval: allow() = %d, %v; want 5, true", n, ok)
	}
}
