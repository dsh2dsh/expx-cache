package cache

import (
	"context"
	"fmt"
	"time"
)

// Set caches the item.
func (self *Cache) Set(ctx context.Context, items ...Item) error {
	if len(items) == 1 {
		_, err := self.set(ctx, &items[0])
		return err
	}
	return self.setItems(ctx, items)
}

func (self *Cache) set(ctx context.Context, item *Item) ([]byte, error) {
	b, err := item.marshal(ctx, self.Marshal)
	if err != nil || b == nil {
		return nil, err
	}

	useLocal := self.localCache != nil && !item.SkipLocalCache
	useRedis := self.redis != nil

	switch {
	case useLocal && useRedis:
		done := make(chan error)
		go func() {
			done <- self.redisSet(ctx, item, b, item.ttl(self.DefaultTTL()))
		}()
		self.localCache.Set(self.ResolveKey(item.Key), b)
		err = <-done
	case useLocal:
		self.localCache.Set(self.ResolveKey(item.Key), b)
	case useRedis:
		err = self.redisSet(ctx, item, b, item.ttl(self.DefaultTTL()))
	}

	if err != nil {
		return nil, err
	}

	return b, nil
}

func (self *Cache) redisSet(ctx context.Context, item *Item, b []byte,
	ttl time.Duration,
) error {
	err := self.redis.Set(ctx, 1, func(int) (string, []byte, time.Duration) {
		return self.ResolveKey(item.Key), b, ttl
	})
	if err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

func (self *Cache) setItems(ctx context.Context, items []Item) error {
	bytes, err := self.marshalItems(ctx, items)
	if err != nil {
		return err
	}

	useLocal := self.localCache != nil
	useRedis := self.redis != nil

	switch {
	case useLocal && useRedis:
		done := make(chan error)
		go func() {
			done <- self.redisSetItems(ctx, items, bytes)
		}()
		self.localSetItems(items, bytes)
		err = <-done
	case useLocal:
		self.localSetItems(items, bytes)
	case useRedis:
		err = self.redisSetItems(ctx, items, bytes)
	}

	return err
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
	err := self.redis.Set(ctx, len(items),
		func(i int) (string, []byte, time.Duration) {
			item := &items[i]
			return self.ResolveKey(item.Key), bytes[i], item.ttl(self.DefaultTTL())
		})
	if err != nil {
		return fmt.Errorf("failed set redis items: %w", err)
	}
	return nil
}
