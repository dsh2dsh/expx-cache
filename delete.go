package cache

import (
	"context"
	"fmt"
)

func (self *Cache) Delete(ctx context.Context, key string) error {
	if self.localCache != nil {
		self.localCache.Del(key)
	}

	if self.redis == nil {
		return nil
	} else if err := self.redis.Del(ctx, key); err != nil {
		return fmt.Errorf("delete %q from redis: %w", key, err)
	}

	return nil
}

func (self *Cache) DeleteFromLocalCache(key string) {
	if self.localCache != nil {
		self.localCache.Del(key)
	}
}
