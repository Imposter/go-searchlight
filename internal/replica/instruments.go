package replica

import (
	"context"
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// instruments are a tailer's metrics (spec section 11), labelled with its index and
// shard. The lag and halted gauges are observed at each collection from the tailer's
// state, so they stay live while the copy is stuck.
type instruments struct {
	meter         metric.Meter
	log           *slog.Logger
	lagSeq        metric.Float64ObservableGauge
	lagTime       metric.Float64ObservableGauge
	halted        metric.Float64ObservableGauge
	pollFailing   metric.Float64ObservableGauge
	progress      metric.Float64Gauge
	batchSize     metric.Float64Histogram
	applyDur      metric.Float64Histogram
	recoveryDur   metric.Float64Histogram
	recoveries    metric.Int64Counter
	recoveryBytes metric.Int64Counter
	backpressure  metric.Int64Counter
	halts         metric.Int64Counter

	base  []attribute.KeyValue
	attrs metric.MeasurementOption
}

func newInstruments(meter metric.Meter, id ShardID, log *slog.Logger) *instruments {
	if meter == nil {
		meter = metricnoop.NewMeterProvider().Meter(telemetry.ScopeName)
	}
	in := telemetry.NewInstruments(meter)
	base := []attribute.KeyValue{attribute.String(telemetry.KeyIndex, id.Index), attribute.Int(telemetry.KeyShard, id.Shard)}
	i := &instruments{
		meter:         meter,
		log:           log,
		lagSeq:        in.ObservableGauge(telemetry.MetricReplicaLagSeq),
		lagTime:       in.ObservableGauge(telemetry.MetricReplicaLagTime),
		halted:        in.ObservableGauge(telemetry.MetricReplicaHalted),
		pollFailing:   in.ObservableGauge(telemetry.MetricReplicaPollFailing),
		progress:      in.Gauge(telemetry.MetricReplicaRecoveryProgress),
		batchSize:     in.Histogram(telemetry.MetricReplicaApplyBatchSize),
		applyDur:      in.Histogram(telemetry.MetricReplicaApplyDuration),
		recoveryDur:   in.Histogram(telemetry.MetricReplicaRecoveryDuration),
		recoveries:    in.Counter(telemetry.MetricReplicaRecoveries),
		recoveryBytes: in.Counter(telemetry.MetricReplicaRecoveryBytes),
		backpressure:  in.Counter(telemetry.MetricReplicaBackpressure),
		halts:         in.Counter(telemetry.MetricReplicaHalts),
		base:          base,
		attrs:         metric.WithAttributeSet(attribute.NewSet(base...)),
	}
	if err := in.Err(); err != nil {
		log.Error("replica metrics unavailable", slog.Any("error", err))
	}
	return i
}

// observe reports t's lag and halt at every collection until the returned function
// is called (Run registers it for its life).
func (i *instruments) observe(t *Tailer) func() {
	reg, err := i.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		seq, age := t.Lag()
		o.ObserveFloat64(i.lagSeq, float64(seq), i.attrs)
		o.ObserveFloat64(i.lagTime, age.Seconds(), i.attrs)
		o.ObserveFloat64(i.halted, flag(t.Halt() != nil), i.attrs)
		o.ObserveFloat64(i.pollFailing, flag(t.PollFailing()), i.attrs)
		return nil
	}, i.lagSeq, i.lagTime, i.halted, i.pollFailing)
	if err != nil {
		i.log.Error("replica lag metrics unavailable", slog.Any("error", err))
		return func() {}
	}
	return func() { _ = reg.Unregister() }
}

// with returns the base attributes plus extra.
func (i *instruments) with(extra ...attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(append(slices.Clip(i.base), extra...)...))
}

func flag(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
