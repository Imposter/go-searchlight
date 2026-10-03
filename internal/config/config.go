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
	// AdminListen serves /healthz, /metrics and, when Pprof is set,
	// /debug/pprof/*.
	AdminListen string
	// AdvertiseAddress is the host:port peers use to reach this node. When
	// unset it is derived from Listen, with this host's name when Listen has
	// no host.
	AdvertiseAddress string
	// NodeID names this node in the registry, logs and telemetry.
	NodeID string
	// DataDir holds the local segments.
	DataDir string
	// TokensFile lists the API bearer tokens. Empty disables API auth.
	TokensFile string
	// ClusterToken authenticates the internal peer API.
	ClusterToken string
	// RefreshInterval is how often each shard's write buffer becomes a
	// searchable segment.
	RefreshInterval time.Duration
	// SeqPersistInterval bounds how long a shard's durable seq (its manifest's) may
	// trail its refreshed seq when refreshes bring no new segment, only a changelog
	// position other shards' changes moved. Such a refresh is visible at once but
	// written to the manifest at most this often (and at the next segment change, and
	// at shutdown), sparing an fsync per refresh; a restart replays at most this much
	// more of the changelog.
	SeqPersistInterval time.Duration
	// MaxLag is how far a copy may trail the changelog and still serve reads
	// and report ready.
	MaxLag time.Duration
	// MergeBudget caps the bytes per second background merges write on this
	// node; 0 means unlimited. It is the I/O half of the merge budget.
	MergeBudget int64
	// MergeThreads caps the concurrent background merges on this node. It is
	// the CPU half of the merge budget.
	MergeThreads int
	// SearchThreads sizes the search worker pool.
	SearchThreads int
	// LogLevel is the minimum level logged.
	LogLevel slog.Level
	// Pprof serves /debug/pprof/* on the admin listener.
	Pprof bool
	// ShutdownTimeout bounds the graceful shutdown.
	ShutdownTimeout time.Duration
}

// Default returns the production defaults. StoreURL is empty because it has
// no default.
func Default() Config {
	return Config{
		Listen:             ":8780",
		AdminListen:        ":8781",
		NodeID:             hostname(),
		DataDir:            "data",
		RefreshInterval:    time.Second,
		SeqPersistInterval: 30 * time.Second,
		MaxLag:             2 * time.Second,
		MergeBudget:        64 << 20,
		MergeThreads:       max(1, runtime.GOMAXPROCS(0)/4),
		SearchThreads:      runtime.GOMAXPROCS(0),
		LogLevel:           slog.LevelInfo,
		ShutdownTimeout:    30 * time.Second,
	}
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
		name: "admin_listen", usage: "admin address for /healthz, /metrics and pprof (host:port)",
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
		name: "cluster_token", secret: true, usage: "token for the internal peer API",
		parse:   func(c *Config, v string) error { c.ClusterToken = v; return nil },
		format:  func(c *Config) string { return c.ClusterToken },
		display: func(c *Config) string { return redacted(c.ClusterToken) },
	},
	{
		name: "refresh_interval", usage: "how often written documents become searchable",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.RefreshInterval, v) },
		format: func(c *Config) string { return c.RefreshInterval.String() },
	},
	{
		name: "seq_persist_interval", usage: "how often a shard writes a seq that moved without new segments to its manifest",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.SeqPersistInterval, v) },
		format: func(c *Config) string { return c.SeqPersistInterval.String() },
	},
	{
		name: "max_lag", usage: "how far a copy may trail the changelog and still serve and report ready",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.MaxLag, v) },
		format: func(c *Config) string { return c.MaxLag.String() },
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
		name: "shutdown_timeout", usage: "how long a graceful shutdown may take",
		parse:  func(c *Config, v string) error { return positiveDuration(&c.ShutdownTimeout, v) },
		format: func(c *Config) string { return c.ShutdownTimeout.String() },
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
	if c.TokensFile != "" {
		if _, err := os.Stat(c.TokensFile); err != nil {
			errs = append(errs, fmt.Errorf("tokens_file: %w", err))
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
