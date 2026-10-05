package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/otlptranslator"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// newMeterProvider builds the meter provider with a Prometheus reader on a
// private registry (which also carries the Go runtime and process
// collectors) and, when OTLP metrics are selected, a periodic OTLP reader
// (interval from OTEL_METRIC_EXPORT_INTERVAL). It returns the provider and
// the /metrics handler.
func newMeterProvider(ctx context.Context, res *resource.Resource, getenv func(string) string) (*sdkmetric.MeterProvider, http.Handler, error) {
	reg := prometheus.NewRegistry()
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, nil, fmt.Errorf("go collector: %w", err)
	}
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, nil, fmt.Errorf("process collector: %w", err)
	}
	prom, err := promexp.New(
		promexp.WithRegisterer(reg),
		promexp.WithTranslationStrategy(otlptranslator.UnderscoreEscapingWithSuffixes),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	opts := []sdkmetric.Option{sdkmetric.WithResource(res), sdkmetric.WithReader(prom)}

	enabled, protocol, err := otlpSelection(getenv, "metrics")
	if err != nil {
		return nil, nil, err
	}
	if enabled {
		var exp sdkmetric.Exporter
		if protocol == protoGRPC {
			exp, err = otlpmetricgrpc.New(ctx)
		} else {
			exp, err = otlpmetrichttp.New(ctx)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("otlp metric exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	}
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
	return sdkmetric.NewMeterProvider(opts...), handler, nil
}

// Kind is the instrument kind of a catalogued metric, which fixes the
// Instruments method that creates it.
type Kind uint8

// Instrument kinds.
const (
	KindCounter       Kind = iota + 1 // metric.Int64Counter, via Instruments.Counter
	KindUpDownCounter                 // metric.Int64UpDownCounter, via Instruments.UpDownCounter
	KindHistogram                     // metric.Float64Histogram, via Instruments.Histogram
	KindGauge                         // metric.Float64Gauge or an observable one, via Instruments.Gauge / ObservableGauge
)

func (k Kind) String() string {
	switch k {
	case KindCounter:
		return "counter"
	case KindUpDownCounter:
		return "up-down counter"
	case KindHistogram:
		return "histogram"
	case KindGauge:
		return "gauge"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// MetricSpec describes one metric in the catalogue.
type MetricSpec struct {
	Name        string
	Kind        Kind
	Unit        string // UCUM: "s", "By", "1", or a {annotation}
	Description string // names the expected attributes
	Buckets     []float64
}

// Metric names (spec §11). Every Searchlight metric is listed here and in
// Catalog; create instruments through Instruments so names, units and
// buckets stay consistent. On /metrics, dots become underscores and the unit
// adds a suffix (s: _seconds, By: _bytes, 1: _ratio; counters add _total).
const (
	// Requests.
	MetricHTTPRequestDuration = "searchlight.http.request.duration"
	MetricHTTPRequestErrors   = "searchlight.http.request.errors"
	MetricHTTPInflight        = "searchlight.http.requests.inflight"

	// Search.
	MetricSearchPhaseDuration    = "searchlight.search.phase.duration"
	MetricSearchSegmentsTouched  = "searchlight.search.segments.touched"
	MetricSearchFilterCacheLooks = "searchlight.search.filter_cache.lookups"
	MetricSearchDocsScanned      = "searchlight.search.documents.scanned"
	MetricSearchDocsMatched      = "searchlight.search.documents.matched"

	// Percolation.
	MetricPercolateDuration      = "searchlight.percolate.duration"
	MetricPercolateCandidates    = "searchlight.percolate.candidates"
	MetricPercolateVerifications = "searchlight.percolate.verifications"
	MetricPercolateAlwaysCheck   = "searchlight.percolate.always_check"

	// Indexing.
	MetricIndexChanges         = "searchlight.index.changes"
	MetricGroupCommitBatchSize = "searchlight.store.group_commit.batch_size"

	// Refresh and merge.
	MetricRefreshDuration = "searchlight.shard.refresh.duration"
	MetricRefreshFailures = "searchlight.shard.refresh.failures"
	MetricFlushDuration   = "searchlight.shard.flush.duration"
	MetricFlushFailures   = "searchlight.shard.flush.failures"
	MetricMergeFailures   = "searchlight.shard.merge.failures"
	MetricMergeDuration   = "searchlight.shard.merge.duration"
	MetricMergeBytes      = "searchlight.shard.merge.bytes"
	MetricMergeBacklog    = "searchlight.shard.merge.backlog"
	MetricShardSegments   = "searchlight.shard.segments"
	MetricShardBufferDocs = "searchlight.shard.buffer.documents"

	// Size.
	MetricShardDocuments    = "searchlight.shard.documents"
	MetricShardTerms        = "searchlight.shard.terms"
	MetricShardDiskSize     = "searchlight.shard.disk.size"
	MetricShardMmapResident = "searchlight.shard.mmap.resident"

	// Replication.
	MetricReplicaLagSeq           = "searchlight.replica.lag.seq"
	MetricReplicaLagTime          = "searchlight.replica.lag.time"
	MetricReplicaRecoveryProgress = "searchlight.replica.recovery.progress"
	MetricReplicaRecoveryBytes    = "searchlight.replica.recovery.bytes"
	MetricReplicaRecoveries       = "searchlight.replica.recoveries"
	MetricReplicaRecoveryDuration = "searchlight.replica.recovery.duration"
	MetricReplicaApplyBatchSize   = "searchlight.replica.apply.batch_size"
	MetricReplicaApplyDuration    = "searchlight.replica.apply.duration"
	MetricReplicaBackpressure     = "searchlight.replica.backpressure"
	MetricReplicaHalts            = "searchlight.replica.halts"
	MetricReplicaHalted           = "searchlight.replica.halted"
	MetricReplicaPollFailing      = "searchlight.replica.poll.failing"
	MetricReplicaWatchReconnects  = "searchlight.replica.watch.reconnects"
	MetricClusterLeaseChanges     = "searchlight.cluster.lease.changes"
	MetricClusterAllocations      = "searchlight.cluster.allocation.changes"
	MetricClusterNodes            = "searchlight.cluster.nodes"
	MetricClusterPeerDuration     = "searchlight.cluster.peer.request.duration"
	MetricClusterReadRetries      = "searchlight.cluster.read.retries"

	// Database.
	MetricStoreOperationDuration = "searchlight.store.operation.duration"
	MetricStoreErrors            = "searchlight.store.errors"
	MetricStoreWALSize           = "searchlight.store.wal.size"
)

// Histogram bucket boundaries.
var (
	// FastBuckets suit request-path latencies, in seconds (100µs to 30s).
	FastBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	// SlowBuckets suit background work, in seconds (1ms to 1h).
	SlowBuckets = []float64{0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 900, 3600}
	// CountBuckets suit sizes and counts (1 to 1M).
	CountBuckets = []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 100000, 1000000}
)

// Catalog lists every Searchlight metric.
var Catalog = []MetricSpec{
	{MetricHTTPRequestDuration, KindHistogram, "s", "HTTP request latency by route, method and status.", FastBuckets},
	{MetricHTTPRequestErrors, KindCounter, "{request}", "HTTP requests answered with a 4xx or 5xx, by route, method and status.", nil},
	{MetricHTTPInflight, KindUpDownCounter, "{request}", "HTTP requests in progress, by route.", nil},

	{MetricSearchPhaseDuration, KindHistogram, "s", "Search time by phase (plan, execute, reduce, fetch) and index.", FastBuckets},
	{MetricSearchSegmentsTouched, KindHistogram, "{segment}", "Segments a shard search read, by index.", CountBuckets},
	{MetricSearchFilterCacheLooks, KindCounter, "{lookup}", "Filter cache lookups by index and result (hit or miss).", nil},
	{MetricSearchDocsScanned, KindCounter, "{document}", "Documents a shard search checked one at a time (residual verification), by index.", nil},
	{MetricSearchDocsMatched, KindCounter, "{document}", "Documents a shard search matched, by index.", nil},

	{MetricPercolateDuration, KindHistogram, "s", "Percolation time per document by phase (probe, verify) and index.", FastBuckets},
	{MetricPercolateCandidates, KindHistogram, "{query}", "Candidate queries per percolated document, by index.", CountBuckets},
	{MetricPercolateVerifications, KindCounter, "{query}", "Candidate queries verified with the exact matcher, by index and result (match or miss).", nil},
	{MetricPercolateAlwaysCheck, KindGauge, "{query}", "Saved queries on the always-check list, by index and shard.", nil},

	{MetricIndexChanges, KindCounter, "{change}", "Changes applied to shard copies, by index and kind (upsert, delete, query_upsert, query_delete).", nil},
	{MetricGroupCommitBatchSize, KindHistogram, "{change}", "Changes per group-commit transaction.", CountBuckets},

	{MetricRefreshDuration, KindHistogram, "s", "Shard refresh time, by index and shard.", SlowBuckets},
	{MetricRefreshFailures, KindCounter, "{refresh}", "Shard refreshes that failed (the buffer is kept and retried), by index and shard.", nil},
	{MetricFlushDuration, KindHistogram, "s", "Shard flush time (fsyncs, sidecars, manifest), by index and shard.", SlowBuckets},
	{MetricFlushFailures, KindCounter, "{flush}", "Shard flushes that failed (retried at the next), by index and shard.", nil},
	{MetricMergeFailures, KindCounter, "{merge}", "Segment merges that failed or were abandoned, by index and shard.", nil},
	{MetricMergeDuration, KindHistogram, "s", "Segment merge time, by index and shard.", SlowBuckets},
	{MetricMergeBytes, KindCounter, "By", "Bytes written by segment merges, by index and shard.", nil},
	{MetricMergeBacklog, KindGauge, "{segment}", "Segments waiting to be merged, by index and shard.", nil},
	{MetricShardSegments, KindGauge, "{segment}", "Segments in the current generation, by index and shard.", nil},
	{MetricShardBufferDocs, KindGauge, "{change}", "Documents and saved queries in the write buffer, not yet refreshed, by index and shard.", nil},

	{MetricShardDocuments, KindGauge, "{document}", "Live documents, by index and shard.", nil},
	{MetricShardTerms, KindGauge, "{term}", "Distinct terms across a shard's segments, by index and shard.", nil},
	{MetricShardDiskSize, KindGauge, "By", "Segment bytes on disk, by index and shard.", nil},
	{MetricShardMmapResident, KindGauge, "By", "Memory-mapped segment bytes resident in memory, by index and shard.", nil},

	{MetricReplicaLagSeq, KindGauge, "{change}", "Changes a shard copy trails the changelog by, by index and shard.", nil},
	{MetricReplicaLagTime, KindGauge, "s", "Age of the oldest change a shard copy has not applied, by index and shard.", nil},
	{MetricReplicaRecoveryProgress, KindGauge, "1", "Fraction of a recovering shard copy that has been fetched, by index, shard and source (peer, blob or sql).", nil},
	{MetricReplicaRecoveryBytes, KindCounter, "By", "Bytes fetched by recoveries, by index, shard and source.", nil},
	{MetricReplicaRecoveries, KindCounter, "{recovery}", "Shard copy recoveries, by index, shard, source (sql, peer or reopen), reason and result (ok or error).", nil},
	{MetricReplicaRecoveryDuration, KindHistogram, "s", "Shard copy recovery time, by index, shard and source.", SlowBuckets},
	{MetricReplicaApplyBatchSize, KindHistogram, "{change}", "Changes per batch a tailer applies to its shard copy, by index and shard.", CountBuckets},
	{MetricReplicaApplyDuration, KindHistogram, "s", "Time a tailer takes to analyze and apply one batch to its shard copy, by index and shard.", FastBuckets},
	{MetricReplicaBackpressure, KindCounter, "{wait}", "Times a tailer waited out a full write buffer before applying again, by index and shard.", nil},
	{MetricReplicaHalts, KindCounter, "{halt}", "Shard copies halted by a change they could not apply, by index, shard and reason.", nil},
	{MetricReplicaHalted, KindGauge, "1", "1 while a shard copy is halted at a change it cannot apply, else 0, by index and shard.", nil},
	{MetricReplicaPollFailing, KindGauge, "1", "1 while a shard copy's changelog polls fail (its lag in changes is then the last known one), else 0, by index and shard.", nil},
	{MetricReplicaWatchReconnects, KindCounter, "{reconnect}", "Changelog notification subscriptions restarted after they failed.", nil},
	{MetricClusterLeaseChanges, KindCounter, "{change}", "Shard lease events by kind (claim, renew, renew_failed, reclaim, lapse, resume, lost, release).", nil},
	{MetricClusterAllocations, KindCounter, "{change}", "Shard copy state transitions by index and state (recovering, serving, retiring).", nil},
	{MetricClusterNodes, KindGauge, "{node}", "Live nodes in the cluster, as this node reads the registry.", nil},
	{MetricClusterPeerDuration, KindHistogram, "s", "Internal peer API request latency by route, peer and status (0: unreachable).", FastBuckets},
	{MetricClusterReadRetries, KindCounter, "{retry}", "Shard reads retried on another copy after one failed or timed out, by operation.", nil},

	{MetricStoreOperationDuration, KindHistogram, "s", "SQL store latency by operation and dialect.", FastBuckets},
	{MetricStoreErrors, KindCounter, "{error}", "SQL store errors by operation and dialect.", nil},
	{MetricStoreWALSize, KindGauge, "By", "SQLite's write-ahead log file size, and the part of it no checkpoint has copied yet (pending=true).", nil},
}

var catalogIndex = func() map[string]*MetricSpec {
	m := make(map[string]*MetricSpec, len(Catalog))
	for i := range Catalog {
		m[Catalog[i].Name] = &Catalog[i]
	}
	return m
}()

// Instruments creates catalogued instruments by name. A failure (an unknown
// name, the wrong kind, or an SDK error) is recorded for Err and a no-op
// instrument is returned, so callers can create a batch and check once:
//
//	in := telemetry.NewInstruments(t.Meter)
//	dur := in.Histogram(telemetry.MetricSearchPhaseDuration)
//	hits := in.Counter(telemetry.MetricSearchFilterCacheLooks)
//	if err := in.Err(); err != nil { ... }
type Instruments struct {
	meter metric.Meter
	noop  metric.Meter
	errs  []error
}

// NewInstruments returns an Instruments creating from m.
func NewInstruments(m metric.Meter) *Instruments {
	return &Instruments{meter: m, noop: noop.NewMeterProvider().Meter(ScopeName)}
}

// Err returns every failure so far, or nil.
func (in *Instruments) Err() error { return errors.Join(in.errs...) }

func (in *Instruments) spec(name string, kind Kind) (*MetricSpec, bool) {
	s, ok := catalogIndex[name]
	switch {
	case !ok:
		in.errs = append(in.errs, fmt.Errorf("metric %q is not in the telemetry catalogue", name))
	case s.Kind != kind:
		in.errs = append(in.errs, fmt.Errorf("metric %q is a %s, not a %s", name, s.Kind, kind))
	default:
		return s, true
	}
	return nil, false
}

func (in *Instruments) fail(name string, err error) {
	in.errs = append(in.errs, fmt.Errorf("metric %q: %w", name, err))
}

// Counter creates a catalogued counter.
func (in *Instruments) Counter(name string) metric.Int64Counter {
	if s, ok := in.spec(name, KindCounter); ok {
		c, err := in.meter.Int64Counter(s.Name, metric.WithDescription(s.Description), metric.WithUnit(s.Unit))
		if err == nil {
			return c
		}
		in.fail(name, err)
	}
	c, _ := in.noop.Int64Counter(name)
	return c
}

// UpDownCounter creates a catalogued up-down counter.
func (in *Instruments) UpDownCounter(name string) metric.Int64UpDownCounter {
	if s, ok := in.spec(name, KindUpDownCounter); ok {
		c, err := in.meter.Int64UpDownCounter(s.Name, metric.WithDescription(s.Description), metric.WithUnit(s.Unit))
		if err == nil {
			return c
		}
		in.fail(name, err)
	}
	c, _ := in.noop.Int64UpDownCounter(name)
	return c
}

// Histogram creates a catalogued histogram with its bucket boundaries.
func (in *Instruments) Histogram(name string) metric.Float64Histogram {
	if s, ok := in.spec(name, KindHistogram); ok {
		opts := []metric.Float64HistogramOption{metric.WithDescription(s.Description), metric.WithUnit(s.Unit)}
		if len(s.Buckets) > 0 {
			opts = append(opts, metric.WithExplicitBucketBoundaries(s.Buckets...))
		}
		h, err := in.meter.Float64Histogram(s.Name, opts...)
		if err == nil {
			return h
		}
		in.fail(name, err)
	}
	h, _ := in.noop.Float64Histogram(name)
	return h
}

// Gauge creates a catalogued synchronous gauge.
func (in *Instruments) Gauge(name string) metric.Float64Gauge {
	if s, ok := in.spec(name, KindGauge); ok {
		g, err := in.meter.Float64Gauge(s.Name, metric.WithDescription(s.Description), metric.WithUnit(s.Unit))
		if err == nil {
			return g
		}
		in.fail(name, err)
	}
	g, _ := in.noop.Float64Gauge(name)
	return g
}

// ObservableGauge creates a catalogued gauge observed by callbacks at each
// collection; use it for values that are cheaper to read than to track. More
// callbacks can be added with the meter's RegisterCallback.
func (in *Instruments) ObservableGauge(name string, callbacks ...metric.Float64Callback) metric.Float64ObservableGauge {
	if s, ok := in.spec(name, KindGauge); ok {
		opts := []metric.Float64ObservableGaugeOption{metric.WithDescription(s.Description), metric.WithUnit(s.Unit)}
		for _, cb := range callbacks {
			opts = append(opts, metric.WithFloat64Callback(cb))
		}
		g, err := in.meter.Float64ObservableGauge(s.Name, opts...)
		if err == nil {
			return g
		}
		in.fail(name, err)
	}
	g, _ := in.noop.Float64ObservableGauge(name)
	return g
}
