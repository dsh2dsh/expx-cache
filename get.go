package cache

import (
	"context"
	"fmt"
)

// Exists reports whether value for the given key exists.
func (self *Cache) Exists(ctx context.Context, key string) (bool, error) {
	b, err := self.getBytes(ctx, key, false)
	if err != nil {
		return false, err
	}
	return b != nil, nil
}

// Get gets the value for the given key.
func (self *Cache) Get(ctx context.Context, key string, value any) (bool, error) {
	return self.get(ctx, key, value, false)
}

// Get gets the value for the given key skipping local cache.
func (self *Cache) GetSkippingLocalCache(
	ctx context.Context, key string, value any,
) (bool, error) {
	return self.get(ctx, key, value, true)
}

func (self *Cache) get(
	ctx context.Context, key string, value any, skipLocalCache bool,
) (bool, error) {
	b, err := self.getBytes(ctx, key, skipLocalCache)
	if err != nil {
		return false, err
	}
	return b != nil, self.unmarshal(b, value)
}

func (self *Cache) getBytes(
	ctx context.Context, key string, skipLocalCache bool,
) ([]byte, error) {
	key = self.WrapKey(key)

	if !skipLocalCache && self.localCache != nil {
		b := self.localCache.Get(key)
		if b != nil {
			self.addLocalHit()
			return b, nil
		}
		self.addLocalMiss()
	}

	if self.redis == nil {
		return nil, nil
	}

	b, err := self.redis.Get(ctx, key)
	if err != nil {
		self.addMiss()
		return nil, fmt.Errorf("get %q from redis: %w", key, err)
	} else if b == nil {
		self.addMiss()
		return nil, nil
	}
	self.addHit()

	if !skipLocalCache && self.localCache != nil {
		self.localCache.Set(key, b)
	}
	return b, nil
}
