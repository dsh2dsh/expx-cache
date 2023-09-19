package cache

import (
	"fmt"
)

// Once gets the item.Value for the given item.Key from the cache or executes,
// caches, and returns the results of the given item.Do, making sure that only
// one execution is in-flight for a given item.Key at a time. If a duplicate
// comes in, the duplicate caller waits for the original to complete and
// receives the same results.
func (self *Cache) Once(item *Item) error {
	b, fromCache, err := self.getSetItemBytesOnce(item)
	if err != nil {
		return err
	} else if len(b) == 0 {
		return nil
	}

	if err := self.Unmarshal(b, item.Value); err != nil {
		if fromCache {
			if err := self.Delete(item.Context(), item.Key); err != nil {
				return err
			}
			return self.Once(item)
		}
		return err
	}

	return nil
}

func (self *Cache) getSetItemBytesOnce(item *Item) ([]byte, bool, error) {
	if self.localCache != nil {
		b := self.localCache.Get(item.Key)
		if b != nil {
			self.addLocalHit()
			return b, true, nil
		}
		// We don't need double addLocalMiss() here, because getBytes does it.
	}

	fromCache, localHit := false, true
	v, err, _ := self.group.Do(item.Key, func() (any, error) {
		localHit = false
		b, err := self.getBytes(item.Context(), item.Key, item.SkipLocalCache)
		if err == nil && b != nil {
			fromCache = true
			return b, nil
		}

		b, err = self.set(item)
		if err != nil {
			return nil, err
		}
		return b, nil
	})

	if err != nil {
		return nil, false, fmt.Errorf("do: %w", err)
	} else if localHit {
		self.addLocalHit()
	}

	return v.([]byte), fromCache, nil
}
