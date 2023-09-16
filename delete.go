package cache

import (
	"context"
	"fmt"
)

func (self *Cache) Delete(ctx context.Context, keys ...string) error {
	done := make(chan error)
	if self.redis != nil {
		go func() {
			done <- self.DeleteFromRedis(ctx, keys...)
		}()
	}
	self.DeleteFromLocalCache(keys...)

	if self.redis != nil {
		return <-done
	}

	return nil
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
		for low := 0; low < len(keys); low += self.batchSize {
			high := min(len(keys), low+self.batchSize)
			if err := self.redis.Del(ctx, keys[low:high]...); err != nil {
				return fmt.Errorf("redis delete: %w", err)
			}
		}
	}
	return nil
}
