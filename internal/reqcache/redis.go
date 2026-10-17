package reqcache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Redis defaults.
const (
	DefaultRedisPrefix   = "searchlight:rc:"
	DefaultRedisTTL      = 24 * time.Hour
	DefaultRedisTimeout  = 50 * time.Millisecond
	DefaultRedisMaxEntry = 8 << 20
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
	// Timeout bounds each dial, read and write, and each call as a whole
	// (DefaultRedisTimeout when 0): a slow Redis delays a search by at most this
	// much per lookup.
	Timeout time.Duration
	// MaxEntryBytes is the largest value stored (DefaultRedisMaxEntry when 0).
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
// call does, and a Redis that is down fails each call within the timeout.
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
	opt.MaxRetries = -1
	opt.DisableIdentity = true
	opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	return &RedisStore{c: redis.NewClient(opt), prefix: o.Prefix, ttl: o.TTL, timeout: o.Timeout, maxEntry: o.MaxEntryBytes}, nil
}

// Name implements [Store].
func (s *RedisStore) Name() string { return "redis" }

// MaxEntryBytes implements [Store].
func (s *RedisStore) MaxEntryBytes() int64 { return s.maxEntry }

// Get implements [Store].
func (s *RedisStore) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	b, err := s.c.Get(ctx, s.prefix+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return b, err
}

// Set implements [Store].
func (s *RedisStore) Set(ctx context.Context, key string, value []byte) error {
	if int64(len(value)) > s.maxEntry {
		return ErrTooLarge
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.c.Set(ctx, s.prefix+key, value, s.ttl).Err()
}

// Close implements [Store].
func (s *RedisStore) Close() error { return s.c.Close() }
