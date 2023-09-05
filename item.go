package cache

import (
	"context"
	"fmt"
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

	// SetXX only sets the key if it already exists.
	SetXX bool

	// SetNX only sets the key if it does not already exist.
	SetNX bool

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

func (self *Item) ttl() time.Duration {
	switch {
	case self.TTL < 0:
		return 0
	case self.TTL == 0:
		return defaultTTL
	case self.TTL < time.Second:
		return time.Second
	}
	return self.TTL
}

func (self *Item) redisSet(redisCache RedisClient, b []byte,
	ttl time.Duration,
) error {
	setFn := redisCache.Set
	switch {
	case self.SetXX:
		setFn = redisCache.SetXX
	case self.SetNX:
		setFn = redisCache.SetNX
	}

	if err := setFn(self.Context(), self.Key, b, ttl); err != nil {
		return fmt.Errorf("item: set %q: %w", self.Key, err)
	}
	return nil
}
