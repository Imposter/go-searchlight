package node

import (
	"log/slog"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/replica"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// replicaTailer is the replica tailer as the node uses it: Run, Applied, Wake, Shard,
// Lag and PollFailing as they are; StateName and HaltErr from State and Halt.
type replicaTailer struct {
	*replica.Tailer
}

// StateName implements [StateReporter].
func (t replicaTailer) StateName() string { return t.State().String() }

// HaltErr implements [HaltReporter].
func (t replicaTailer) HaltErr() error {
	if h := t.Halt(); h != nil {
		return h
	}
	return nil
}

// HaltReporter is implemented by a tailer whose copy can halt while its Run goes on
// (the replica tailer retries a halted copy with backoff): HaltErr is why, nil while
// it is not halted.
type HaltReporter interface {
	HaltErr() error
}

// PollReporter is implemented by a tailer that knows whether its polls of the
// changelog are failing now (the database unreachable).
type PollReporter interface {
	PollFailing() bool
}

// ReplicaTailers is the production [NewTailerFunc]: a replica tailer per copy, with
// the node's settings (changelog_poll_interval, max_lag, remap_debounce and the
// retry bounds) and its shared Hub (nil: the tailers poll and are woken by writes).
func ReplicaTailers(cfg config.Config, hub *replica.Hub, log *slog.Logger, tr trace.Tracer, meter metric.Meter) NewTailerFunc {
	remap := cfg.RemapDebounce
	if remap == 0 {
		remap = -1 // the config's 0 is "rebuild at once"; the replica's is "the default"
	}
	return func(st store.Store, sh *shard.Shard, id store.ShardID, env TailerEnv) Tailer {
		return replicaTailer{replica.NewTailer(st, sh, id, replica.Options{
			Copy:            env.Copy,
			Fetcher:         env.Fetcher,
			PollInterval:    cfg.ChangelogPollInterval,
			MaxLag:          cfg.MaxLag,
			RemapDebounce:   remap,
			HaltRetryBase:   cfg.HaltRetryBase,
			HaltRetryCap:    cfg.HaltRetryCap,
			RebuildRetryCap: cfg.RebuildRetryCap,
			Hub:             hub,
			Logger:          log,
			Tracer:          tr,
			Meter:           meter,
		})}
	}
}
