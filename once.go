package cache

import (
	"context"
	"fmt"
)

// Once gets the item.Value for the given item.Key from the cache or executes,
// caches, and returns the results of the given item.Do, making sure that only
// one execution is in-flight for a given item.Key at a time. If a duplicate
// comes in, the duplicate caller waits for the original to complete and
// receives the same results.
func (self *Cache) Once(ctx context.Context, item *Item) error {
	return self.once(ctx, item, true)
}

func (self *Cache) once(ctx context.Context, item *Item, autoFix bool) error {
	b, fromCache, err := self.getSetItemBytesOnce(ctx, item)
	if err != nil {
		return err
	} else if len(b) == 0 {
		return nil
	}

	if err := self.unmarshal(b, item.Value); err != nil {
		if fromCache && autoFix {
			if err := self.Delete(ctx, item.Key); err != nil {
				return err
			}
			return self.once(ctx, item, false)
		}
		return err
	}

	return nil
}

func (self *Cache) getSetItemBytesOnce(
	ctx context.Context, item *Item,
) ([]byte, bool, error) {
	key := self.ResolveKey(item.Key)

	if self.localCache != nil {
		b := self.localCache.Get(key)
		if len(b) > 0 {
			self.addLocalHit()
			return b, true, nil
		}
		// We don't need double addLocalMiss() here, because getBytes does it.
	}

	fromCache, localHit := false, true
	v, err, _ := self.group.Do(key, func() (any, error) {
		localHit = false
		b, err := self.getBytes(ctx, item.Key, item.SkipLocalCache)
		if err == nil && len(b) > 0 {
			fromCache = true
			return b, nil
		}

		b, err = self.set(ctx, item)
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
