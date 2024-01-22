package cache

import (
	"context"
	"fmt"
)

func (self *Cache) Delete(ctx context.Context, keys ...string) error {
	if !self.useRedis() {
		self.DeleteFromLocalCache(keys...)
		return nil
	}

	if self.localCache == nil {
		return self.DeleteFromRedis(ctx, keys...)
	}

	done := make(chan error)
	go func() {
		done <- self.DeleteFromRedis(ctx, keys...)
	}()
	self.DeleteFromLocalCache(keys...)

	return <-done
}

func (self *Cache) DeleteFromLocalCache(keys ...string) {
	if self.localCache != nil {
		for _, key := range keys {
			self.localCache.Del(self.ResolveKey(key))
		}
	}
}

func (self *Cache) DeleteFromRedis(ctx context.Context, keys ...string) error {
	if self.useRedis() {
		wrappedKeys := make([]string, len(keys))
		for i, k := range keys {
			wrappedKeys[i] = self.ResolveKey(k)
		}
		if err := self.redis.Del(ctx, wrappedKeys); err != nil {
			return self.redisCacheError(fmt.Errorf("redis delete: %w", err))
		}
	}
	return nil
}
