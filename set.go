package cache

import (
	"fmt"
)

// Set caches the item.
func (self *Cache) Set(item *Item) error {
	_, err := self.set(item)
	return err
}

func (self *Cache) set(item *Item) ([]byte, error) {
	value, err := item.value()
	if err != nil {
		return nil, err
	} else if value == nil {
		return nil, nil
	}

	b, err := self.Marshal(value)
	if err != nil {
		return nil, err
	}

	if self.localCache != nil && !item.SkipLocalCache {
		self.localCache.Set(item.Key, b)
	}

	if self.redis == nil {
		if self.localCache == nil {
			return b, errRedisLocalCacheNil
		}
		return b, nil
	}

	ttl := item.ttl()
	if ttl == 0 {
		return b, nil
	}

	setFn := self.redis.Set
	switch {
	case item.SetXX:
		setFn = self.redis.SetXX
	case item.SetNX:
		setFn = self.redis.SetNX
	}

	if err := setFn(item.Context(), item.Key, b, ttl); err != nil {
		return nil, fmt.Errorf("cache set %q with ttl %v: %w", item.Key, ttl, err)
	}

	return b, nil
}
