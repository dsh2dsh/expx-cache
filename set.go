package cache

import (
	"fmt"
	"time"
)

// Set caches the item.
func (self *Cache) Set(item *Item) error {
	_, err := self.set(item)
	return err
}

func (self *Cache) set(item *Item) ([]byte, error) {
	b, err := self.marshalItem(item)
	if err != nil || b == nil {
		return nil, err
	}

	useLocal := self.localCache != nil && !item.SkipLocalCache
	ttl := self.ItemTTL(item)
	useRedis := self.redis != nil && ttl != 0

	key := self.resolveKey(item.Key)
	switch {
	case useLocal && useRedis:
		done := make(chan error)
		go func() {
			done <- self.redisSet(item, b, ttl)
		}()
		self.localCache.Set(key, b)
		err = <-done
	case useLocal:
		self.localCache.Set(key, b)
	case useRedis:
		err = self.redisSet(item, b, ttl)
	}

	if err != nil {
		return nil, err
	}

	return b, nil
}

func (self *Cache) marshalItem(item *Item) ([]byte, error) {
	value, err := item.value()
	if err != nil || value == nil {
		return nil, err
	}

	b, err := self.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("cache: marshal item %q: %w", item.Key, err)
	}
	return b, nil
}

func (self *Cache) redisSet(item *Item, b []byte, ttl time.Duration) error {
	if err := self.redis.Set(item.Context(), self.resolveKey(item.Key), b, ttl); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}
