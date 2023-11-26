package cache

import (
	"context"
	"time"
)

type Item struct {
	Key   string
	Value any

	// TTL is the cache expiration time.
	// Default TTL configured using [WithDefaultTTL] and it's 1 hour by default.
	TTL time.Duration

	// Do returns value to be cached.
	Do func(ctx context.Context) (any, error)

	// SkipLocalCache skips local cache as if it is not set.
	SkipLocalCache bool
}

func (self *Item) value(ctx context.Context) (any, error) {
	if self.Do != nil {
		return self.Do(ctx)
	}
	return self.Value, nil
}

func (self *Item) ttl(def time.Duration) time.Duration {
	if self.TTL > 0 {
		return self.TTL
	}
	return def
}
