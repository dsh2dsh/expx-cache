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

	if self.redis == nil || self.ItemTTL(item) == 0 {
		return b, nil
	} else if err := item.redisSet(self.redis, b, self.ItemTTL(item)); err != nil {
		return nil, fmt.Errorf("cache: set: %w", err)
	}

	return b, nil
}
