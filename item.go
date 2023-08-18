package cache

import (
	"context"
	"time"
)

const defaultTTL = time.Hour

type Item struct {
	Ctx context.Context

	Key   string
	Value any

	// TTL is the cache expiration time.
	// Default TTL is 1 hour.
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
