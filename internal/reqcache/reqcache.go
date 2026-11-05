// Package reqcache holds the shared stores behind search's shard request cache: a
// Store keeps encoded shard results by key, for every node that reaches it. Redis is
// the one there is ([NewRedis]). The in-process cache in front of it (the L1) is
// search's own.
//
// A store is a cache, never a source of truth: a miss, an error and a timeout all mean
// the search computes its result itself, so a store that is down or slow costs
// searches nothing but the hits.
package reqcache

import (
	"context"
	"errors"
)

// Store keeps encoded shard results by key.
type Store interface {
	// Name labels the store's metrics and logs ("redis").
	Name() string
	// Get returns key's value, or nil when there is none. It returns within the
	// store's timeout.
	Get(ctx context.Context, key string) ([]byte, error)
	// Set stores value under key, replacing any. It returns within the store's
	// timeout. A value past the store's MaxEntryBytes is not stored, and is
	// ErrTooLarge.
	Set(ctx context.Context, key string, value []byte) error
	// MaxEntryBytes is the largest value Set stores.
	MaxEntryBytes() int64
	// Close releases the store's connections.
	Close() error
}

// ErrTooLarge is a value Set does not store: it is past the store's MaxEntryBytes.
var ErrTooLarge = errors.New("reqcache: value too large")
