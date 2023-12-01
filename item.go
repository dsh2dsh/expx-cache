package cache

import (
	"context"
	"fmt"
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

func (self *Item) marshal(
	ctx context.Context, marshalFunc MarshalFunc,
) ([]byte, error) {
	v, err := self.value(ctx)
	if err != nil || v == nil {
		return nil, err
	}
	b, err := marshalFunc(v)
	if err != nil {
		return nil, fmt.Errorf("marshal item %q: %w", self.Key, err)
	}
	return b, nil
}

func (self *Item) ttl(def time.Duration) time.Duration {
	if self.TTL > 0 {
		return self.TTL
	}
	return def
}

func (self *Item) value(ctx context.Context) (any, error) {
	if self.Do != nil {
		v, err := self.Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("value item %q: %w", self.Key, err)
		}
		return v, nil
	}
	return self.Value, nil
}
