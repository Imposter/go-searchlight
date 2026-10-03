package shard

import (
	"context"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// The shard reports refresh latency, merge time and bytes, segment count, buffered
// documents, changes and filter-cache hits and misses.
func TestShardMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	opts := testOptions()
	opts.Meter = meter
	opts.Index, opts.Shard = "products", 3
	h := newHarness(t, opts)
	h.upsert("a", "b")
	h.refresh()
	h.upsert("c")
	h.del("a")
	h.refresh()
	h.forceMerge(1)
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
		telemetry.MetricMergeBacklog,
	} {
		if !got[name] {
			t.Errorf("metric %s not reported (have %v)", name, got)
		}
	}
	st := h.s.opts.FilterCache.Stats()
	if st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("filter cache stats %+v, want one hit and one miss", st)
	}
}
