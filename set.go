package cache

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/dsh2dsh/expx-cache/redis"
)

// Set caches the item.
func (self *Cache) Set(ctx context.Context, items ...Item) error {
	if len(items) == 1 {
		_, err := self.set(ctx, &items[0])
		return err
	}
	_, err := self.setItems(ctx, items)
	return err
}

func (self *Cache) set(ctx context.Context, item *Item) ([]byte, error) {
	b, err := item.marshal(ctx, self.Marshal)
	if err != nil || b == nil {
		return nil, err
	}

	switch {
	case self.useLocalCache(item) && self.useRedis():
		done := make(chan error)
		go func() {
			done <- self.redisSet(ctx, item, b, item.ttl(self.DefaultTTL()))
		}()
		self.localCache.Set(self.ResolveKey(item.Key), b)
		err = <-done
	case self.useLocalCache(item):
		self.localCache.Set(self.ResolveKey(item.Key), b)
	case self.useRedis():
		err = self.redisSet(ctx, item, b, item.ttl(self.DefaultTTL()))
	}
	return b, err
}

func (self *Cache) redisSet(ctx context.Context, item *Item, b []byte,
	ttl time.Duration,
) error {
	err := self.redis.Set(ctx, 1, slices.Values([]redis.Item{
		redis.NewItem(self.ResolveKey(item.Key), b, ttl),
	}))
	if err != nil {
		return self.redisCacheError(fmt.Errorf("redis set: %w", err))
	}
	return nil
}

func (self *Cache) setItems(ctx context.Context, items []Item,
) ([][]byte, error) {
	bytes, err := self.marshalItems(ctx, items)
	if err != nil {
		return nil, err
	}

	useLocal := self.localCache != nil
	switch {
	case useLocal && self.useRedis():
		done := make(chan error)
		go func() {
			done <- self.redisSetItems(ctx, items, bytes)
		}()
		self.localSetItems(items, bytes)
		err = <-done
	case useLocal:
		self.localSetItems(items, bytes)
	case self.useRedis():
		err = self.redisSetItems(ctx, items, bytes)
	}
	return bytes, err
}

func (self *Cache) localSetItems(items []Item, bytes [][]byte) {
	for i := range items {
		item := &items[i]
		b := bytes[i]
		if !item.SkipLocalCache && len(b) > 0 {
			self.localCache.Set(self.ResolveKey(item.Key), b)
		}
	}
}

func (self *Cache) redisSetItems(ctx context.Context, items []Item,
	bytes [][]byte,
) error {
	err := self.redis.Set(ctx, len(items), func(yield func(redis.Item) bool) {
		for i := range items {
			item := &items[i]
			ok := yield(redis.NewItem(self.ResolveKey(item.Key), bytes[i],
				item.ttl(self.DefaultTTL())))
			if !ok {
				return
			}
		}
	})
	if err != nil {
		return self.redisCacheError(fmt.Errorf("failed set redis items: %w", err))
	}
	return nil
}
