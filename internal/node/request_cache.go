package node

import (
	"fmt"

	"github.com/Imposter/go-searchlight/internal/reqcache"
	"github.com/Imposter/go-searchlight/internal/search"
)

// newRequestCache is the node's shard request cache, as o.Config sets it up: in
// memory up to request_cache_size, in front of the Redis at request_cache_redis_url
// when that is set; nil when neither is. Every copy the node hosts shares it, for its
// own searches and its peers'.
func newRequestCache(o Options) (*search.RequestCache, error) {
	cfg := o.Config
	var st reqcache.Store
	if cfg.RequestCacheRedisURL != "" {
		rs, err := reqcache.NewRedis(reqcache.RedisOptions{
			URL:           cfg.RequestCacheRedisURL,
			Password:      cfg.RequestCacheRedisPassword,
			Prefix:        cfg.RequestCacheRedisPrefix,
			TTL:           cfg.RequestCacheRedisTTL,
			Timeout:       cfg.RequestCacheRedisTimeout,
			MaxEntryBytes: cfg.RequestCacheRedisMaxEntryBytes,
		})
		if err != nil {
			return nil, fmt.Errorf("node: request_cache_redis_url: %w", err)
		}
		st = rs
	}
	return search.NewRequestCache(search.RequestCacheOptions{
		MaxBytes: cfg.RequestCacheSize,
		Store:    st,
		Version:  o.Version,
		Meter:    o.Meter,
		Logger:   o.Logger,
	}), nil
}

// RequestCacheStats returns the node's request cache counters (zero when it has none).
func (n *Single) RequestCacheStats() search.RequestCacheStats { return n.rcache.Stats() }
