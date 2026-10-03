package replica

import (
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// instruments are a tailer's metrics (spec section 11), labelled with its index and
// shard.
type instruments struct {
	lagSeq        metric.Float64Gauge
	lagTime       metric.Float64Gauge
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
		lagSeq:        in.Gauge(telemetry.MetricReplicaLagSeq),
		lagTime:       in.Gauge(telemetry.MetricReplicaLagTime),
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

// with returns the base attributes plus extra.
func (i *instruments) with(extra ...attribute.KeyValue) metric.MeasurementOption {
	return metric.WithAttributeSet(attribute.NewSet(append(slices.Clip(i.base), extra...)...))
}
