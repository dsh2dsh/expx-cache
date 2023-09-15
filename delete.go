package cache

import (
	"context"
	"fmt"
	"sync"
)

func (self *Cache) Delete(ctx context.Context, keys ...string) error {
	gogogo := len(keys) > 0 && self.redis != nil
	var wg sync.WaitGroup

	if gogogo {
		wg.Add(1)
		go func() {
			defer wg.Done()
			self.DeleteFromLocalCache(keys...)
		}()
	} else {
		self.DeleteFromLocalCache(keys...)
	}

	err := self.DeleteFromRedis(ctx, keys...)

	if gogogo {
		wg.Wait()
	}

	return err
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
