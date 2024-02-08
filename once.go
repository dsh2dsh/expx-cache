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
func (self *Cache) Once(ctx context.Context, item Item) error {
	return self.once(ctx, &item)
}

func (self *Cache) once(ctx context.Context, item *Item) error {
	b, err := self.getSetItemBytesOnce(ctx, item, self.redisGetSet)
	if err != nil || len(b) == 0 {
		return err
	}
	return self.unmarshal(b, item.Value)
}

func (self *Cache) getSetItemBytesOnce(ctx context.Context, item *Item,
	redisGetSet func(ctx context.Context, item *Item) ([]byte, error),
) ([]byte, error) {
	key := self.ResolveKey(item.Key)
	if self.useLocalCache(item) {
		if b := self.localCache.Get(key); len(b) != 0 {
			self.addLocalHit()
			return b, nil
		}
	}

	localMiss := false
	v, err, _ := self.group.Do(key, func() (any, error) {
		if self.useLocalCache(item) {
			self.addLocalMiss()
		}
		localMiss = true
		return redisGetSet(ctx, item)
	})

	if err != nil {
		return nil, fmt.Errorf("group Do: %w", err)
	} else if !localMiss {
		self.addLocalHit()
	}
	return v.([]byte), nil
}

func (self *Cache) redisGetSet(ctx context.Context, item *Item) ([]byte, error) {
	b, err := self.redisGet(ctx, self.ResolveKey(item.Key), item.SkipLocalCache)
	if err != nil || len(b) != 0 {
		return b, err
	}
	return self.set(ctx, item)
}
