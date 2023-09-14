package cache

import (
	"context"
	"time"
)

type Item struct {
	Ctx context.Context

	Key   string
	Value any

	// TTL is the cache expiration time.
	// Default TTL configured using [WithDefaultTTL] and it's 1 hour by default.
	TTL time.Duration

	// Do returns value to be cached.
	Do func(*Item) (any, error)

	// SkipLocalCache skips local cache as if it is not set.
	SkipLocalCache bool
}

func (self *Item) Context() context.Context {
	if self.Ctx == nil {
		return context.Background()
	}
	return self.Ctx
}

func (self *Item) value() (any, error) {
	if self.Do != nil {
		return self.Do(self)
	}
	return self.Value, nil
}
