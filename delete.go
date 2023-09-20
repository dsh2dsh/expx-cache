package cache

import (
	"context"
	"fmt"
)

func (self *Cache) Delete(ctx context.Context, keys ...string) error {
	if self.redis == nil {
		self.DeleteFromLocalCache(keys...)
		return nil
	} else if self.localCache == nil {
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
			self.localCache.Del(key)
		}
	}
}

func (self *Cache) DeleteFromRedis(ctx context.Context, keys ...string) error {
	if self.redis != nil {
		if err := self.redis.Del(ctx, keys...); err != nil {
			return fmt.Errorf("redis delete: %w", err)
		}
	}
	return nil
}
