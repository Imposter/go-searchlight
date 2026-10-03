package shard

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// instruments are a shard's metrics (spec section 11), labelled with its index and
// shard.
type instruments struct {
	refresh metric.Float64Histogram
	merge   metric.Float64Histogram

	refreshFailures metric.Int64Counter
	mergeFailures   metric.Int64Counter
	mergeBytes      metric.Int64Counter
	backlog         metric.Float64Gauge
	segments        metric.Float64Gauge
	bufferDocs      metric.Float64Gauge
	documents       metric.Float64Gauge
	diskBytes       metric.Float64Gauge
	terms           metric.Float64Gauge
	resident        metric.Float64ObservableGauge
	residentCB      metric.Registration
	changes         metric.Int64Counter

	attrs     metric.MeasurementOption
	kindAttrs [QueryDelete + 1]metric.AddOption
}

func newInstruments(meter metric.Meter, index string, shard int, log *slog.Logger) *instruments {
	if meter == nil {
		meter = metricnoop.NewMeterProvider().Meter(telemetry.ScopeName)
	}
	in := telemetry.NewInstruments(meter)
	base := []attribute.KeyValue{attribute.String(telemetry.KeyIndex, index), attribute.Int(telemetry.KeyShard, shard)}
	i := &instruments{
		refresh: in.Histogram(telemetry.MetricRefreshDuration),
		merge:   in.Histogram(telemetry.MetricMergeDuration),

		refreshFailures: in.Counter(telemetry.MetricRefreshFailures),
		mergeFailures:   in.Counter(telemetry.MetricMergeFailures),
		mergeBytes:      in.Counter(telemetry.MetricMergeBytes),
		backlog:         in.Gauge(telemetry.MetricMergeBacklog),
		segments:        in.Gauge(telemetry.MetricShardSegments),
		bufferDocs:      in.Gauge(telemetry.MetricShardBufferDocs),
		documents:       in.Gauge(telemetry.MetricShardDocuments),
		diskBytes:       in.Gauge(telemetry.MetricShardDiskSize),
		terms:           in.Gauge(telemetry.MetricShardTerms),
		resident:        in.ObservableGauge(telemetry.MetricShardMmapResident),
		changes:         in.Counter(telemetry.MetricIndexChanges),
		attrs:           metric.WithAttributeSet(attribute.NewSet(base...)),
	}
	for k := Upsert; k <= QueryDelete; k++ {
		i.kindAttrs[k] = metric.WithAttributeSet(attribute.NewSet(append(base[:2:2], attribute.String("kind", k.String()))...))
	}
	if err := in.Err(); err != nil {
		log.Error("shard metrics unavailable", slog.Any("error", err))
	}
	return i
}

func (i *instruments) countChanges(ctx context.Context, changes []Change) {
	var counts [QueryDelete + 1]int64
	for j := range changes {
		counts[changes[j].Kind]++
	}
	for k := Upsert; k <= QueryDelete; k++ {
		if counts[k] > 0 {
			i.changes.Add(ctx, counts[k], i.kindAttrs[k])
		}
	}
}

func (i *instruments) recordRefresh(ctx context.Context, d time.Duration) {
	i.refresh.Record(ctx, d.Seconds(), i.attrs)
}

func (i *instruments) recordMerge(ctx context.Context, d time.Duration, bytes int64) {
	i.merge.Record(ctx, d.Seconds(), i.attrs)
	if bytes > 0 {
		i.mergeBytes.Add(ctx, bytes, i.attrs)
	}
}

func (i *instruments) recordBuffer(ctx context.Context, docs int) {
	i.bufferDocs.Record(ctx, float64(docs), i.attrs)
}

func (i *instruments) recordBacklog(ctx context.Context, segments int) {
	i.backlog.Record(ctx, float64(segments), i.attrs)
}

// observeResident reports, at each collection, how much of the current generation's
// segment mappings is resident in memory (where the platform can say).
func (i *instruments) observeResident(meter metric.Meter, s *Shard) {
	if meter == nil {
		return
	}
	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		g := s.Acquire()
		if g == nil {
			return nil
		}
		defer g.Release()
		var total int64
		for _, sv := range g.Segments {
			n, err := sv.Reader.ResidentBytes()
			if err != nil {
				return nil // unsupported here, or closed: nothing to report
			}
			total += n
		}
		o.ObserveFloat64(i.resident, float64(total), i.attrs)
		return nil
	}, i.resident)
	if err != nil {
		s.log.Error("shard resident-memory metric unavailable", slog.Any("error", err))
		return
	}
	i.residentCB = reg
}

// stop unregisters the shard's metric callbacks.
func (i *instruments) stop() {
	if i.residentCB != nil {
		_ = i.residentCB.Unregister()
	}
}

// recordGeneration records a newly published generation's size: its segments, their
// deletes sidecars and the manifest on disk, and its terms.
func (s *Shard) recordGeneration(ctx context.Context, g *Generation) {
	disk := s.manifestBytes.Load()
	var terms uint64
	for i := range g.docs {
		disk += g.docs[i].ref.bytes + g.docs[i].delBytes
		terms += g.docs[i].ref.reader.NumTerms()
	}
	for i := range g.queries {
		disk += g.queries[i].ref.bytes + g.queries[i].delBytes
	}
	s.inst.segments.Record(ctx, float64(len(g.docs)+len(g.queries)), s.inst.attrs)
	s.inst.documents.Record(ctx, float64(g.numDocs), s.inst.attrs)
	s.inst.diskBytes.Record(ctx, float64(disk), s.inst.attrs)
	s.inst.terms.Record(ctx, float64(terms), s.inst.attrs)
}
