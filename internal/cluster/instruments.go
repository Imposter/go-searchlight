package cluster

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// instruments are the cluster's metrics (spec section 11): lease and allocation
// changes, live nodes, peer requests and retries, and the bytes recoveries fetch from
// peers and bundles.
type instruments struct {
	leases      metric.Int64Counter
	allocations metric.Int64Counter
	peerDur     metric.Float64Histogram
	retries     metric.Int64Counter
	recovered   metric.Int64Counter
	nodes       atomic.Int64
}

func newInstruments(meter metric.Meter, log *slog.Logger) *instruments {
	if meter == nil {
		meter = metricnoop.NewMeterProvider().Meter(telemetry.ScopeName)
	}
	in := telemetry.NewInstruments(meter)
	i := &instruments{
		leases:      in.Counter(telemetry.MetricClusterLeaseChanges),
		allocations: in.Counter(telemetry.MetricClusterAllocations),
		peerDur:     in.Histogram(telemetry.MetricClusterPeerDuration),
		retries:     in.Counter(telemetry.MetricClusterReadRetries),
		recovered:   in.Counter(telemetry.MetricReplicaRecoveryBytes),
	}
	in.ObservableGauge(telemetry.MetricClusterNodes, func(_ context.Context, o metric.Float64Observer) error {
		o.Observe(float64(i.nodes.Load()))
		return nil
	})
	if err := in.Err(); err != nil {
		log.Error("cluster metrics unavailable", slog.Any("error", err))
	}
	return i
}

func (i *instruments) lease(ctx context.Context, kind string) {
	i.leases.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", kind)))
}

func (i *instruments) allocation(ctx context.Context, id store.ShardID, state string) {
	i.allocations.Add(ctx, 1, metric.WithAttributes(attribute.String(telemetry.KeyIndex, id.Index), attribute.String("state", state)))
}

func (i *instruments) peer(ctx context.Context, route, node string, status int, d time.Duration) {
	i.peerDur.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.String("route", route), attribute.String("peer", node),
		attribute.String("status", strconv.Itoa(status))))
}

func (i *instruments) retry(ctx context.Context, op string) {
	i.retries.Add(ctx, 1, metric.WithAttributes(attribute.String("op", op)))
}

func (i *instruments) recoveryBytes(ctx context.Context, source string, n int64) {
	i.recovered.Add(ctx, n, metric.WithAttributes(attribute.String("source", source)))
}
