// Package config loads Searchlight's settings (spec §12) from command-line
// flags and SEARCHLIGHT_* environment variables, applies the production
// defaults and validates the result.
//
// Every setting has one snake_case name, used everywhere: the flag is
// --<name> and the environment variable is SEARCHLIGHT_<NAME>. A flag wins
// over the environment, which wins over the default. Secrets (store_url and
// cluster_token) can also come from a file named by SEARCHLIGHT_<NAME>_FILE,
// so container secrets never have to sit in the environment. Only store_url
// is required.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// EnvPrefix prefixes every setting's environment variable.
const EnvPrefix = "SEARCHLIGHT_"

// StoreSchemes are the store_url schemes Searchlight accepts, one per SQL
// dialect.
var StoreSchemes = []string{"postgres", "postgresql", "mysql", "sqlite"}

// Config is Searchlight's validated configuration.
type Config struct {
	// StoreURL locates the SQL database that is the write-ahead log and the
	// system of record, e.g. postgres://user:pass@host/db or
	// sqlite:///var/lib/searchlight/searchlight.db. Required.
	StoreURL string
	// Listen is the public API address.
	Listen string
	// AdminListen serves /healthz, /readyz, /metrics and, when Pprof is set,
	// /debug/pprof/*. It is loopback by default; a container sets :8781 so probes and
	// Prometheus reach it.
	AdminListen string
	// AdvertiseAddress is the host:port peers use to reach this node. When
	// unset it is derived from Listen, with this host's name when Listen has
	// no host.
	AdvertiseAddress string
	// NodeID names this node in the registry, logs and telemetry.
	NodeID string
	// DataDir holds the local segments.
	DataDir string
	// TokensFile lists the API bearer tokens. Empty disables API auth, which
	// Load refuses unless InsecureNoAuth is set.
	TokensFile string
	// InsecureNoAuth allows an empty TokensFile: the public API then serves
	// every request unauthenticated. Meant for a laptop or a test, never for a
	// reachable node; the API logs a warning at start when it is on.
	InsecureNoAuth bool
	// ClusterToken authenticates the internal peer API.
	ClusterToken string
	// PeerCAFile, when set, is a PEM bundle of the certificate authorities that sign
	// peers' TLS certificates (the peer API runs over TLS when tls_cert is set); unset,
	// the system roots.
	PeerCAFile string
	// LeaseTTL is how long a shard copy's lease lasts unrenewed (renewed every 2 s).
	// 0 picks the store's default: 30 s on SQLite, whose single writer can delay a
	// renewal behind a long commit, 10 s elsewhere.
	LeaseTTL time.Duration
	// PruneStallTimeout is how long a copy behind its shard's others may hold the
	// changelog's prune floor without progress (halted, stuck recovering, or its node
	// silent) before pruning goes on without it; it rebuilds when it moves again.
	PruneStallTimeout time.Duration
	// RetiringRetention is how long the changelog is kept for a copy whose node shut
	// down cleanly (its row left retiring, its lease run out), so a node restarted
	// within it replays the tail rather than rebuilding.
	RetiringRetention time.Duration
	// ChangelogRetention caps how old a change may grow before it is pruned whatever
	// copy still needs it (that copy rebuilds).
	ChangelogRetention time.Duration
	// BundleInterval is how often a bundle of each shard's durable state is uploaded to
	// the store's blobs (sl_blobs), by one serving copy of the shard, for a copy to
	// recover from when no peer can serve it; 0 uploads none. Each retained bundle
	// holds the changelog's prune floor at its seq, so a copy restored from it can
	// replay the rest.
	BundleInterval time.Duration
	// BundleRetention is how many bundles of each shard are kept, the newest.
	BundleRetention int
	// RefreshInterval is how often each shard's write buffer becomes a
	// searchable segment. A refresh is visibility only: it fsyncs nothing.
	RefreshInterval time.Duration
	// FlushInterval is how often each shard copy makes what refreshes published
	// durable: it fsyncs the new segments and their deletes, writes its manifest and
	// only then reports the manifest's seq as applied (the changelog is pruned by it).
	// The SQL changelog is the write-ahead log, so nothing acknowledged is ever at
	// risk: a crash replays at most this much more of the changelog. A merge, a peer
	// snapshot and a shutdown flush at once as well.
	FlushInterval time.Duration
	// MaxLag is how far a copy may trail the changelog and still serve reads
	// and report ready.
	MaxLag time.Duration
	// ChangelogPollInterval is how often a shard copy polls the changelog for
	// changes when nothing wakes it sooner. Postgres pushes notifications, and
	// the node that commits a write wakes its own copies; the poll is what
	// brings other nodes' writes to a copy on MySQL and SQLite, and the
	// safety net everywhere.
	ChangelogPollInterval time.Duration
	// RemapDebounce is how long a copy a mapping change must rebuild waits for
	// more mapping changes, so a burst costs one rebuild; 0 rebuilds at once.
	RemapDebounce time.Duration
	// HaltRetryBase and HaltRetryCap bound the backoff of a halted copy's retries;
	// RebuildRetryCap bounds it for a rebuild that keeps failing.
	HaltRetryBase   time.Duration
	HaltRetryCap    time.Duration
	RebuildRetryCap time.Duration
	// MergeBudget caps the bytes per second background merges write on this
	// node; 0 means unlimited. It is the I/O half of the merge budget.
	MergeBudget int64
	// MergeThreads caps the concurrent background merges on this node. It is
	// the CPU half of the merge budget.
	MergeThreads int
	// SearchThreads sizes the search worker pool.
	SearchThreads int
	// GCHeapFloor is the heap size below which the garbage collector does not start
	// a cycle (at most a quarter of GOMEMLIMIT); 0 turns the floor off, and a GOGC
	// environment variable overrides it.
	GCHeapFloor int64
	// LogLevel is the minimum level logged.
	LogLevel slog.Level
	// Pprof serves /debug/pprof/* on the admin listener.
	Pprof bool
	// ShutdownTimeout is a graceful shutdown's whole budget after ShutdownGrace: drain,
	// listener and node stop (final manifests), then the store and telemetry closing.
	ShutdownTimeout time.Duration

	// MaxBodyBytes caps a public API request body. It stays below MaxBodyLimit,
	// the largest document a segment stores, so a body never holds a document
	// no refresh could write.
	MaxBodyBytes int64
	// MaxDocBytes caps one document's JSON (a PUT body, a bulk line). It is at
	// most MaxBodyBytes.
	MaxDocBytes int64
	// MaxBulkOps caps the operations of one _bulk request.
	MaxBulkOps int
	// RequestTimeout is every public API request's deadline: a search past it
	// answers with what it has (timed_out), a wait past it gives up.
	RequestTimeout time.Duration
	// ReadTimeout bounds reading a whole request, headers and body, so a slow
	// client cannot hold a connection open (slowloris).
	ReadTimeout time.Duration
	// SearchQueue caps the reads (searches, counts, percolations, field
	// catalogues) in progress at once; past it a read is refused with 429.
	SearchQueue int
	// MaxInflightWriteBytes and MaxInflightReadBytes cap the heap the writes, and
	// the reads, in progress may take at once (Elasticsearch's indexing pressure):
	// each request is charged its body bytes times InflightAmplification, and past
	// either budget a request is refused with 429. Each is at least
	// MaxBodyBytes times InflightAmplification, so any single request fits.
	MaxInflightWriteBytes int64
	MaxInflightReadBytes  int64
	// InflightAmplification is the heap a request takes per byte of its body, at
	// its peak: measured at about 4.5 for a bulk and 8 for a bulk with percolate
	// or a percolation (BenchmarkBulkPeakHeap); the default, 10, leaves headroom.
	// A node's heap is then about 1.5 times the two budgets, plus each shard
	// copy's write buffers (the shard's refresh bytes times its max buffer factor:
	// 64 MiB times 4 by default).
	InflightAmplification int
	// DropTimeout bounds dropping an index from the store, which deletes its
	// documents, queries and changes, apart from the request's own deadline.
	DropTimeout time.Duration
	// ShutdownGrace is how long the API keeps serving after readiness turns
	// false at shutdown, before it closes its listener, so a load balancer stops
	// sending it traffic first.
	ShutdownGrace time.Duration
	// MaxIndexFields caps the fields of one index's mapping (Elasticsearch's
	// index.mapping.total_fields.limit): a document or mapping change that would
	// pass it is refused.
	MaxIndexFields int
	// TLSCert and TLSKey, both set, serve the public API over TLS. Unset, serve
	// it behind a TLS-terminating proxy.
	TLSCert string
	TLSKey  string
}

// MaxBodyLimit is the largest max_body_bytes: 32 MiB less 1 KiB, so a body
// and the longest document id (512 bytes) stay below the largest document a
// segment stores (segment.MaxStoredBytes, 32 MiB with its id), and no accepted
// body can carry a document a refresh would refuse.
const MaxBodyLimit = 32<<20 - 1<<10

// Default returns the production defaults. StoreURL is empty because it has
// no default.
func Default() Config {
	return Config{
		Listen:                ":8780",
		AdminListen:           "127.0.0.1:8781",
		NodeID:                hostname(),
		DataDir:               "data",
		RefreshInterval:       time.Second,
		FlushInterval:         10 * time.Second,
		MaxLag:                2 * time.Second,
		ChangelogPollInterval: 500 * time.Millisecond,
		RemapDebounce:         2 * time.Second,
		HaltRetryBase:         30 * time.Second,
		HaltRetryCap:          10 * time.Minute,
		RebuildRetryCap:       2 * time.Minute,
		MergeBudget:           64 << 20,
		MergeThreads:          max(1, runtime.GOMAXPROCS(0)/4),
		SearchThreads:         runtime.GOMAXPROCS(0),
		GCHeapFloor:           64 << 20,
		LogLevel:              slog.LevelInfo,
		ShutdownTimeout:       time.Minute,
		MaxBodyBytes:          16 << 20,
		MaxDocBytes:           4 << 20,
		MaxBulkOps:            10_000,
		RequestTimeout:        30 * time.Second,
		ReadTimeout:           time.Minute,
		SearchQueue:           1000,

		MaxInflightWriteBytes: 512 << 20,
		MaxInflightReadBytes:  256 << 20,
		InflightAmplification: 10,
		DropTimeout:           10 * time.Minute,
		PruneStallTimeout:     15 * time.Minute,
		RetiringRetention:     15 * time.Minute,
		ChangelogRetention:    24 * time.Hour,
		BundleRetention:       2,
		ShutdownGrace:         2 * time.Second,
		MaxIndexFields:        1000,
	}
}

// removedSettings are settings Searchlight no longer has, each with what replaced it:
// Load refuses one given as a flag or an environment variable by name, rather than
// letting the flag parser fail generically or the variable be ignored.
var removedSettings = []struct{ name, instead string }{
	{"seq_persist_interval", "flush_interval (default 10s) now persists the seq"},
}

// setting describes one configuration setting.
type setting struct {
	name    string // snake_case; flag --name, env SEARCHLIGHT_NAME
	usage   string
	isBool  bool                            // the flag may be given bare (--name)
	secret  bool                            // also read from SEARCHLIGHT_NAME_FILE
	parse   func(c *Config, v string) error // v is trimmed and non-empty
	format  func(c *Config) string
	display func(c *Config) string // how logs show the value; nil uses format
}

// envName returns the setting's environment variable.
func (s *setting) envName() string { return EnvPrefix + strings.ToUpper(s.name) }

var settings = []setting{
	{
		name: "store_url", secret: true,
		usage:  "SQL database URL (postgres://, mysql:// or sqlite://); required",
		parse:  func(c *Config, v string) error { c.StoreURL = v; return nil },
		format: func(c *Config) string { return c.StoreURL },
		display: func(c *Config) string {
			if u, err := url.Parse(c.StoreURL); err == nil {
				return u.Redacted()
			}
			return redacted(c.StoreURL)
		},
	},
	{
		name: "listen", usage: "public API address (host:port)",
		parse:  func(c *Config, v string) error { c.Listen = v; return nil },
		format: func(c *Config) string { return c.Listen },
	},
	{
		name: "admin_listen", usage: "admin address for /healthz, /readyz, /metrics and pprof (host:port; loopback by default)",
		parse:  func(c *Config, v string) error { c.AdminListen = v; return nil },
		format: func(c *Config) string { return c.AdminListen },
	},
	{
		name: "advertise_address", usage: "host:port peers use to reach this node (default: derived from listen)",
		parse:  func(c *Config, v string) error { c.AdvertiseAddress = v; return nil },
		format: func(c *Config) string { return c.AdvertiseAddress },
	},
	{
		name: "node_id", usage: "this node's name in the registry, logs and telemetry (default: the host name)",
		parse:  func(c *Config, v string) error { c.NodeID = v; return nil },
		format: func(c *Config) string { return c.NodeID },
	},
	{
		name: "data_dir", usage: "directory for local segments",
		parse:  func(c *Config, v string) error { c.DataDir = v; return nil },
		format: func(c *Config) string { return c.DataDir },
	},
	{
		name: "tokens_file", usage: "file of API bearer tokens (empty disables API auth)",
		parse:  func(c *Config, v string) error { c.TokensFile = v; return nil },
		format: func(c *Config) string { return c.TokensFile },
	},
	{
		name: "insecure_no_auth", isBool: true, usage: "serve the API without auth when tokens_file is empty (never on a reachable node)",
		parse: func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("want true or false, got %q", v)
			}
			c.InsecureNoAuth = b
			return nil
		},
		format: func(c *Config) string { return strconv.FormatBool(c.InsecureNoAuth) },
	},
	{
		name: "cluster_token", secret: true, usage: "token for the internal peer API",
		parse:   func(c *Config, v string) error { c.ClusterToken = v; return nil },
		format:  func(c *Config) string { return c.ClusterToken },
		display: func(c *Config) string { return redacted(c.ClusterToken) },
	},
	{
		name: "peer_ca_file", usage: "PEM bundle of the CAs that sign peers' TLS certificates (default: the system roots)",
		parse:  func(c *Config, v string) error { c.PeerCAFile = v; return nil },
		format: func(c *Config) string { return c.PeerCAFile },
	},
	{
		name: "lease_ttl", usage: "how long a shard copy's lease lasts unrenewed (0 = 30s on SQLite, 10s elsewhere)",
		parse: func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 || (d > 0 && d < 3*time.Second) {
				return fmt.Errorf("want 0 or a duration of at least 3s, got %q", v)
			}
			c.LeaseTTL = d
			return nil
		},
		format: func(c *Config) string { return c.LeaseTTL.String() },
	},
	{
		name: "prune_stall_timeout", usage: "how long a copy making no progress may hold the changelog's prune floor",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.PruneStallTimeout, v) },
		format: func(c *Config) string { return c.PruneStallTimeout.String() },
	},
	{
		name: "retiring_retention", usage: "how long the changelog is kept for a cleanly stopped node's copies to replay on restart",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.RetiringRetention, v) },
		format: func(c *Config) string { return c.RetiringRetention.String() },
	},
	{
		name: "changelog_retention", usage: "the oldest a change may grow before it is pruned whatever copy still needs it",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.ChangelogRetention, v) },
		format: func(c *Config) string { return c.ChangelogRetention.String() },
	},
	{
		name: "bundle_interval", usage: "how often each shard's durable state is uploaded to sl_blobs for recovery without a peer (0 = never)",
		parse: func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 || (d > 0 && d < time.Second) {
				return fmt.Errorf("want 0 or a duration of at least 1s, got %q", v)
			}
			c.BundleInterval = d
			return nil
		},
		format: func(c *Config) string { return c.BundleInterval.String() },
	},
	{
		name: "bundle_retention", usage: "how many bundles of each shard sl_blobs keeps, the newest",
		parse:  func(c *Config, v string) error { return positiveInt(&c.BundleRetention, v) },
		format: func(c *Config) string { return strconv.Itoa(c.BundleRetention) },
	},
	{
		name: "refresh_interval", usage: "how often written documents become searchable",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.RefreshInterval, v) },
		format: func(c *Config) string { return c.RefreshInterval.String() },
	},
	{
		name: "flush_interval", usage: "how often a shard copy fsyncs what refreshes published and advances its durable seq",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.FlushInterval, v) },
		format: func(c *Config) string { return c.FlushInterval.String() },
	},
	{
		name: "max_lag", usage: "how far a copy may trail the changelog and still serve and report ready",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.MaxLag, v) },
		format: func(c *Config) string { return c.MaxLag.String() },
	},
	{
		name: "changelog_poll_interval", usage: "how often a shard copy polls the changelog when nothing wakes it sooner",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.ChangelogPollInterval, v) },
		format: func(c *Config) string { return c.ChangelogPollInterval.String() },
	},
	{
		name: "remap_debounce", usage: "how long a copy a mapping change must rebuild waits for more mapping changes (0 = none)",
		parse: func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 {
				return fmt.Errorf("want a duration such as 2s or 0s, got %q", v)
			}
			c.RemapDebounce = d
			return nil
		},
		format: func(c *Config) string { return c.RemapDebounce.String() },
	},
	{
		name: "halt_retry_base", usage: "first backoff before a halted shard copy is retried",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.HaltRetryBase, v) },
		format: func(c *Config) string { return c.HaltRetryBase.String() },
	},
	{
		name: "halt_retry_cap", usage: "longest backoff between a halted shard copy's retries",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.HaltRetryCap, v) },
		format: func(c *Config) string { return c.HaltRetryCap.String() },
	},
	{
		name: "rebuild_retry_cap", usage: "longest backoff between retries of a shard copy rebuild that keeps failing",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.RebuildRetryCap, v) },
		format: func(c *Config) string { return c.RebuildRetryCap.String() },
	},
	{
		name: "merge_budget", usage: "bytes per second merges may write, e.g. 64MiB (0 = unlimited)",
		parse: func(c *Config, v string) error {
			n, err := ParseBytes(v)
			c.MergeBudget = n
			return err
		},
		format: func(c *Config) string { return FormatBytes(c.MergeBudget) },
	},
	{
		name: "merge_threads", usage: "concurrent background merges (default: GOMAXPROCS/4, at least 1)",
		parse:  func(c *Config, v string) error { return positiveInt(&c.MergeThreads, v) },
		format: func(c *Config) string { return strconv.Itoa(c.MergeThreads) },
	},
	{
		name: "gc_heap_floor", usage: "heap size below which the garbage collector does not start a cycle, e.g. 64MiB (0 = off; GOGC overrides; at most GOMEMLIMIT/4)",
		parse: func(c *Config, v string) error {
			n, err := ParseBytes(v)
			c.GCHeapFloor = n
			return err
		},
		format: func(c *Config) string { return FormatBytes(c.GCHeapFloor) },
	},
	{
		name: "search_threads", usage: "search worker pool size (default: GOMAXPROCS)",
		parse:  func(c *Config, v string) error { return positiveInt(&c.SearchThreads, v) },
		format: func(c *Config) string { return strconv.Itoa(c.SearchThreads) },
	},
	{
		name: "log_level", usage: "minimum log level: debug, info, warn or error",
		parse: func(c *Config, v string) error {
			if err := c.LogLevel.UnmarshalText([]byte(v)); err != nil {
				return fmt.Errorf("want debug, info, warn or error, got %q", v)
			}
			return nil
		},
		format: func(c *Config) string { return strings.ToLower(c.LogLevel.String()) },
	},
	{
		name: "pprof", isBool: true, usage: "serve /debug/pprof/* on the admin listener",
		parse: func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("want true or false, got %q", v)
			}
			c.Pprof = b
			return nil
		},
		format: func(c *Config) string { return strconv.FormatBool(c.Pprof) },
	},
	{
		name: "shutdown_timeout", usage: "how long a graceful shutdown may take in all, after shutdown_grace",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.ShutdownTimeout, v) },
		format: func(c *Config) string { return c.ShutdownTimeout.String() },
	},
	{
		name: "max_body_bytes", usage: "largest API request body, e.g. 16MiB (at most 32MiB less 1KiB)",
		parse: func(c *Config, v string) error {
			n, err := ParseBytes(v)
			c.MaxBodyBytes = n
			return err
		},
		format: func(c *Config) string { return FormatBytes(c.MaxBodyBytes) },
	},
	{
		name: "max_doc_bytes", usage: "largest document JSON, e.g. 4MiB (at most max_body_bytes)",
		parse: func(c *Config, v string) error {
			n, err := ParseBytes(v)
			c.MaxDocBytes = n
			return err
		},
		format: func(c *Config) string { return FormatBytes(c.MaxDocBytes) },
	},
	{
		name: "max_bulk_ops", usage: "most operations in one _bulk request",
		parse:  func(c *Config, v string) error { return positiveInt(&c.MaxBulkOps, v) },
		format: func(c *Config) string { return strconv.Itoa(c.MaxBulkOps) },
	},
	{
		name: "request_timeout", usage: "deadline of every API request",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.RequestTimeout, v) },
		format: func(c *Config) string { return c.RequestTimeout.String() },
	},
	{
		name: "read_timeout", usage: "how long reading a request's headers and body may take",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.ReadTimeout, v) },
		format: func(c *Config) string { return c.ReadTimeout.String() },
	},
	{
		name: "search_queue", usage: "reads in progress at once before more are refused with 429",
		parse:  func(c *Config, v string) error { return positiveInt(&c.SearchQueue, v) },
		format: func(c *Config) string { return strconv.Itoa(c.SearchQueue) },
	},
	{
		name: "max_inflight_write_bytes", usage: "heap the writes in progress may take before more get 429 (at least max_body_bytes times inflight_amplification)",
		parse: func(c *Config, v string) error {
			n, err := ParseBytes(v)
			c.MaxInflightWriteBytes = n
			return err
		},
		format: func(c *Config) string { return FormatBytes(c.MaxInflightWriteBytes) },
	},
	{
		name: "max_inflight_read_bytes", usage: "heap the reads in progress may take before more get 429 (at least max_body_bytes times inflight_amplification)",
		parse: func(c *Config, v string) error {
			n, err := ParseBytes(v)
			c.MaxInflightReadBytes = n
			return err
		},
		format: func(c *Config) string { return FormatBytes(c.MaxInflightReadBytes) },
	},
	{
		name: "inflight_amplification", usage: "heap a request takes per byte of its body, charged to the in-flight budgets",
		parse:  func(c *Config, v string) error { return positiveInt(&c.InflightAmplification, v) },
		format: func(c *Config) string { return strconv.Itoa(c.InflightAmplification) },
	},
	{
		name: "drop_timeout", usage: "how long dropping an index from the store may take",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.DropTimeout, v) },
		format: func(c *Config) string { return c.DropTimeout.String() },
	},
	{
		name: "shutdown_grace", usage: "how long the API serves after readiness turns false at shutdown (0 = none)",
		parse: func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 {
				return fmt.Errorf("want a duration such as 2s or 0s, got %q", v)
			}
			c.ShutdownGrace = d
			return nil
		},
		format: func(c *Config) string { return c.ShutdownGrace.String() },
	},
	{
		name: "max_index_fields", usage: "most fields one index's mapping holds",
		parse:  func(c *Config, v string) error { return positiveInt(&c.MaxIndexFields, v) },
		format: func(c *Config) string { return strconv.Itoa(c.MaxIndexFields) },
	},
	{
		name: "tls_cert", usage: "PEM certificate file to serve the API over TLS (with tls_key; else use a TLS proxy)",
		parse:  func(c *Config, v string) error { c.TLSCert = v; return nil },
		format: func(c *Config) string { return c.TLSCert },
	},
	{
		name: "tls_key", usage: "PEM private key file for tls_cert",
		parse:  func(c *Config, v string) error { c.TLSKey = v; return nil },
		format: func(c *Config) string { return c.TLSKey },
	},
}

// Load builds the configuration from the command-line arguments (without the
// program name) and the environment, read through env (os.Getenv in
// production). It returns flag.ErrHelp when args ask for help; Usage prints
// it. Every error names the setting it is about.
func Load(args []string, env func(string) string) (Config, error) {
	fromFlags, err := parseFlags(args)
	if err != nil {
		return Config{}, err
	}
	c := Default()
	var errs []error
	for _, r := range removedSettings {
		source := EnvPrefix + strings.ToUpper(r.name)
		if _, ok := fromFlags[r.name]; ok {
			source = "--" + r.name
		} else if env(source) == "" {
			continue
		}
		errs = append(errs, fmt.Errorf("%s was removed; %s (set by %s)", r.name, r.instead, source))
	}
	for i := range settings {
		s := &settings[i]
		v, source, err := lookup(s, fromFlags, env)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if v == "" {
			continue
		}
		if err := s.parse(&c, v); err != nil {
			errs = append(errs, fmt.Errorf("%s (from %s): %w", s.name, source, err))
		}
	}
	errs = append(errs, c.validate()...)
	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return c, nil
}

// parseFlags returns the value given for each setting on the command line.
func parseFlags(args []string) (map[string]string, error) {
	fs := flag.NewFlagSet("searchlight", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	given := map[string]string{}
	for i := range settings {
		s := &settings[i]
		record := func(v string) error { given[s.name] = v; return nil }
		if s.isBool {
			fs.BoolFunc(s.name, s.usage, record)
		} else {
			fs.Func(s.name, s.usage, record)
		}
	}
	for _, r := range removedSettings {
		fs.Func(r.name, "removed", func(v string) error { given[r.name] = v; return nil })
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, flag.ErrHelp
		}
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q; settings are given as --name=value", fs.Arg(0))
	}
	return given, nil
}

// lookup returns a setting's trimmed value and where it came from, or "" when
// it is not set anywhere.
func lookup(s *setting, fromFlags map[string]string, env func(string) string) (value, source string, err error) {
	if v, ok := fromFlags[s.name]; ok {
		v = strings.TrimSpace(v)
		if v == "" {
			return "", "", fmt.Errorf("%s (from --%s): empty value", s.name, s.name)
		}
		return v, "--" + s.name, nil
	}
	name := s.envName()
	v := strings.TrimSpace(env(name))
	if !s.secret {
		return v, name, nil
	}
	path := strings.TrimSpace(env(name + "_FILE"))
	switch {
	case v != "" && path != "":
		return "", "", fmt.Errorf("%s: %s and %s_FILE are both set; set one", s.name, name, name)
	case path != "":
		b, err := os.ReadFile(path)
		if err != nil {
			return "", "", fmt.Errorf("%s (from %s_FILE): %w", s.name, name, err)
		}
		return strings.TrimSpace(string(b)), name + "_FILE", nil
	}
	return v, name, nil
}

// validate checks required settings and the ones whose meaning depends on
// another, and derives AdvertiseAddress when it is unset.
func (c *Config) validate() []error {
	var errs []error
	if err := validStoreURL(c.StoreURL); err != nil {
		errs = append(errs, fmt.Errorf("store_url: %w", err))
	}
	for _, l := range []struct{ name, addr string }{{"listen", c.Listen}, {"admin_listen", c.AdminListen}} {
		if _, _, err := net.SplitHostPort(l.addr); err != nil {
			errs = append(errs, fmt.Errorf("%s: want host:port, got %q", l.name, l.addr))
		}
	}
	if _, port, _ := net.SplitHostPort(c.Listen); c.Listen == c.AdminListen && port != "0" {
		errs = append(errs, fmt.Errorf("admin_listen: must differ from listen (%q)", c.Listen))
	}
	if c.AdvertiseAddress == "" {
		c.AdvertiseAddress = deriveAdvertise(c.Listen)
	} else if host, port, err := net.SplitHostPort(c.AdvertiseAddress); err != nil || host == "" || port == "" || port == "0" {
		errs = append(errs, fmt.Errorf("advertise_address: want a reachable host:port, got %q", c.AdvertiseAddress))
	}
	if c.NodeID == "" {
		errs = append(errs, errors.New("node_id: must not be empty"))
	}
	switch {
	case c.TokensFile != "":
		if _, err := os.Stat(c.TokensFile); err != nil {
			errs = append(errs, fmt.Errorf("tokens_file: %w", err))
		}
	case !c.InsecureNoAuth:
		errs = append(errs, errors.New("tokens_file: is required; set insecure_no_auth=true to serve the API without auth"))
	}
	if c.MaxBodyBytes < 1<<10 || c.MaxBodyBytes > MaxBodyLimit {
		errs = append(errs, fmt.Errorf("max_body_bytes: want 1KiB to %d bytes, got %d", MaxBodyLimit, c.MaxBodyBytes))
	}
	if c.MaxDocBytes < 1 || c.MaxDocBytes > c.MaxBodyBytes {
		errs = append(errs, fmt.Errorf("max_doc_bytes: want 1 byte to max_body_bytes (%d), got %d", c.MaxBodyBytes, c.MaxDocBytes))
	}
	for _, b := range []struct {
		name string
		n    int64
	}{{"max_inflight_write_bytes", c.MaxInflightWriteBytes}, {"max_inflight_read_bytes", c.MaxInflightReadBytes}} {
		if floor := c.MaxBodyBytes * int64(max(1, c.InflightAmplification)); b.n < floor {
			errs = append(errs, fmt.Errorf("%s: want at least max_body_bytes times inflight_amplification (%d), got %d", b.name, floor, b.n))
		}
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		errs = append(errs, errors.New("tls_cert: tls_cert and tls_key are set together"))
	}
	for _, f := range []struct{ name, path string }{{"tls_cert", c.TLSCert}, {"tls_key", c.TLSKey}} {
		if f.path != "" {
			if _, err := os.Stat(f.path); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", f.name, err))
			}
		}
	}
	return errs
}

func validStoreURL(raw string) error {
	if raw == "" {
		return fmt.Errorf("is required (--store_url, %sSTORE_URL or %sSTORE_URL_FILE)", EnvPrefix, EnvPrefix)
	}
	u, err := url.Parse(raw)
	if err != nil {
		// url.Error repeats the URL, which may hold a password.
		return errors.New("is not a valid URL")
	}
	if !slices.Contains(StoreSchemes, strings.ToLower(u.Scheme)) {
		return fmt.Errorf("scheme must be one of %s, got %q", strings.Join(StoreSchemes, ", "), u.Scheme)
	}
	return nil
}

// deriveAdvertise turns the listen address into one peers can dial.
func deriveAdvertise(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return ""
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = hostname()
	}
	return net.JoinHostPort(host, port)
}

// LogValue renders the configuration for logs with its secrets redacted.
func (c Config) LogValue() slog.Value {
	attrs := make([]slog.Attr, 0, len(settings))
	for i := range settings {
		s := &settings[i]
		show := s.format
		if s.display != nil {
			show = s.display
		}
		attrs = append(attrs, slog.String(s.name, show(&c)))
	}
	return slog.GroupValue(attrs...)
}

// Usage writes the settings, their environment variables and defaults to w.
func Usage(w io.Writer) {
	def := Default()
	fmt.Fprintf(w, "Usage: searchlight [--setting=value ...]\n\n")
	fmt.Fprintf(w, "Each setting is a flag or a %s* environment variable; flags win.\n", EnvPrefix)
	fmt.Fprintf(w, "store_url and cluster_token may also be read from <VAR>_FILE.\n\n")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SETTING\tENVIRONMENT\tDEFAULT\tMEANING")
	for i := range settings {
		s := &settings[i]
		d := s.format(&def)
		if s.secret || d == "" {
			d = "-"
		}
		fmt.Fprintf(tw, "--%s\t%s\t%s\t%s\n", s.name, s.envName(), d, s.usage)
	}
	_ = tw.Flush()
}

func positiveDuration(dst *time.Duration, v string) error {
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fmt.Errorf("want a positive duration such as 1s or 500ms, got %q", v)
	}
	*dst = d
	return nil
}

func positiveInt(dst *int, v string) error {
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fmt.Errorf("want a positive integer, got %q", v)
	}
	*dst = n
	return nil
}

var byteUnits = []struct {
	suffix string
	size   int64
}{
	// Longest suffixes first, so "MiB" is not read as "B".
	{"KiB", 1 << 10},
	{"MiB", 1 << 20},
	{"GiB", 1 << 30},
	{"TiB", 1 << 40},
	{"KB", 1e3},
	{"MB", 1e6},
	{"GB", 1e9},
	{"TB", 1e12},
	{"B", 1},
}

// ParseBytes parses a non-negative byte size such as 0, 1048576, 512KiB,
// 64MiB or 1GB. Units are case-sensitive: KiB/MiB/GiB/TiB are binary and
// KB/MB/GB/TB decimal.
func ParseBytes(v string) (int64, error) {
	num, mult := v, int64(1)
	for _, u := range byteUnits {
		if strings.HasSuffix(v, u.suffix) {
			num, mult = strings.TrimSpace(strings.TrimSuffix(v, u.suffix)), u.size
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n < 0 || n > (1<<63-1)/mult {
		return 0, fmt.Errorf("want a byte size such as 64MiB or 0, got %q", v)
	}
	return n * mult, nil
}

// FormatBytes renders n in the largest binary unit that divides it exactly.
func FormatBytes(n int64) string {
	for _, u := range []struct {
		suffix string
		size   int64
	}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}} {
		if n != 0 && n%u.size == 0 {
			return strconv.FormatInt(n/u.size, 10) + u.suffix
		}
	}
	return strconv.FormatInt(n, 10)
}

func redacted(s string) string {
	if s == "" {
		return ""
	}
	return "xxxxx"
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "localhost"
	}
	return h
}
