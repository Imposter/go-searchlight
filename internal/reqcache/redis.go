package reqcache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	"github.com/Imposter/go-searchlight/internal/config"
)

// Redis defaults: config's, the one source of them.
const (
	DefaultRedisPrefix   = config.DefaultRequestCacheRedisPrefix
	DefaultRedisTTL      = config.DefaultRequestCacheRedisTTL
	DefaultRedisTimeout  = config.DefaultRequestCacheRedisTimeout
	DefaultRedisMaxEntry = config.DefaultRequestCacheRedisMaxEntryBytes
)

// RedisOptions configure [NewRedis].
type RedisOptions struct {
	// URL is redis://[user:password@]host:port[/db] or rediss:// for TLS, as
	// go-redis's ParseURL reads it.
	URL string
	// Password, when set, replaces the URL's.
	Password string
	// Prefix starts every key (DefaultRedisPrefix when empty), so one Redis can hold
	// several clusters' caches, or other data.
	Prefix string
	// TTL is how long an entry lives (DefaultRedisTTL when 0). Entries are keyed by
	// what the shard holds, so none goes stale; the TTL only reclaims the ones no
	// search will ask for again.
	TTL time.Duration
	// Timeout bounds each call as a whole, its dial, read and write included
	// (DefaultRedisTimeout when 0): a slow Redis delays a search by at most this
	// much per lookup.
	Timeout time.Duration
	// MaxEntryBytes is the largest value stored, or read (DefaultRedisMaxEntry when
	// 0).
	MaxEntryBytes int64
}

// RedisStore is a [Store] on Redis.
type RedisStore struct {
	c        *redis.Client
	prefix   string
	ttl      time.Duration
	timeout  time.Duration
	maxEntry int64
}

var _ Store = (*RedisStore)(nil)

// NewRedis returns a store on the Redis at o.URL. It does not connect: the first
// call does, and a Redis that is down fails each call within the timeout. Wrap it in
// a [Breaker] so that one stops being asked.
func NewRedis(o RedisOptions) (*RedisStore, error) {
	opt, err := redis.ParseURL(o.URL)
	if err != nil {
		// err may quote the URL, password and all.
		return nil, errors.New("reqcache: the Redis URL is invalid: want redis://[user:password@]host:port[/db] or rediss://")
	}
	if o.Password != "" {
		opt.Password = o.Password
	}
	if o.Prefix == "" {
		o.Prefix = DefaultRedisPrefix
	}
	if o.TTL <= 0 {
		o.TTL = DefaultRedisTTL
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultRedisTimeout
	}
	if o.MaxEntryBytes <= 0 {
		o.MaxEntryBytes = DefaultRedisMaxEntry
	}
	opt.DialTimeout, opt.ReadTimeout, opt.WriteTimeout, opt.PoolTimeout = o.Timeout, o.Timeout, o.Timeout, o.Timeout
	opt.ContextTimeoutEnabled = true
	opt.MaxRetries = -1
	opt.DialerRetries = 1 // one attempt: a dial that fails is a miss
	opt.DisableIdentity = true
	opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	return &RedisStore{c: redis.NewClient(opt), prefix: o.Prefix, ttl: o.TTL, timeout: o.Timeout, maxEntry: o.MaxEntryBytes}, nil
}

// Name implements [Store].
func (s *RedisStore) Name() string { return "redis" }

// MaxEntryBytes implements [Store].
func (s *RedisStore) MaxEntryBytes() int64 { return s.maxEntry }

// Get implements [Store]. It reads at most one byte past MaxEntryBytes (GETRANGE), so
// a value past it costs no more than that, and is ErrTooLarge. An empty value, which
// Set never writes, reads as none.
func (s *RedisStore) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	b, err := s.c.GetRange(ctx, s.prefix+key, 0, s.maxEntry).Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		return nil, nil
	case err != nil:
		return nil, err
	case len(b) == 0:
		return nil, nil
	case int64(len(b)) > s.maxEntry:
		return nil, ErrTooLarge
	}
	return b, nil
}

// Set implements [Store].
func (s *RedisStore) Set(ctx context.Context, key string, value []byte) error {
	if len(value) == 0 || int64(len(value)) > s.maxEntry {
		return ErrTooLarge
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.c.Set(ctx, s.prefix+key, value, s.ttl).Err()
}

// Close implements [Store].
func (s *RedisStore) Close() error { return s.c.Close() }
