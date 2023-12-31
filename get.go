package cache

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// Exists reports whether value for the given key exists.
func (self *Cache) Exists(ctx context.Context, key string) (bool, error) {
	b, err := self.getBytes(ctx, key, false)
	if err != nil {
		return false, err
	}
	return len(b) > 0, nil
}

func (self *Cache) getBytes(
	ctx context.Context, key string, skipLocalCache bool,
) ([]byte, error) {
	key = self.ResolveKey(key)

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

	bytesIter, err := self.redis.Get(ctx, 1, func(int) string { return key })
	if err != nil {
		self.addMiss()
		return nil, fmt.Errorf("get %q from redis: %w", key, err)
	}

	b, _ := bytesIter()
	if len(b) == 0 {
		self.addMiss()
		return nil, nil
	}
	self.addHit()

	if !skipLocalCache && self.localCache != nil {
		self.localCache.Set(key, b)
	}
	return b, nil
}

func (self *Cache) Get(parentCtx context.Context, items ...*Item) ([]*Item, error) {
	if len(items) == 1 {
		return self.getOneItems(parentCtx, items)
	}

	g, ctx := errgroup.WithContext(parentCtx)
	missed, err := self.localGetItems(ctx, g, items)
	if err != nil {
		return nil, err
	}

	if len(missed) > 0 {
		missed, err = self.redisGetItems(ctx, g, missed)
		if err != nil {
			return nil, err
		}
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed unmarshal items: %w", err)
	} else if parentCtx.Err() != nil {
		return nil, fmt.Errorf("failed unmarshal items: %w", context.Cause(ctx))
	}

	return missed, nil
}

func (self *Cache) getOneItems(ctx context.Context, items []*Item) ([]*Item, error) {
	item := items[0]
	b, err := self.getBytes(ctx, item.Key, item.SkipLocalCache)
	if err != nil {
		return nil, err
	} else if len(b) == 0 {
		return items, nil
	}
	return nil, self.unmarshal(b, item.Value)
}

func (self *Cache) localGetItems(
	ctx context.Context, g *errgroup.Group, items []*Item,
) ([]*Item, error) {
	if self.localCache == nil {
		return items, nil
	}
	missed := items[:0]

	for _, item := range items {
		if item.SkipLocalCache {
			missed = append(missed, item)
			self.addLocalMiss()
		} else if b := self.localCache.Get(self.ResolveKey(item.Key)); len(b) == 0 {
			missed = append(missed, item)
			self.addLocalMiss()
		} else {
			self.addLocalHit()
			if err := self.acquireUnmarshal(ctx, g, b, item.Value); err != nil {
				return nil, err
			}
		}
	}

	return missed, nil
}

func (self *Cache) redisGetItems(
	ctx context.Context, g *errgroup.Group, items []*Item,
) ([]*Item, error) {
	if self.redis == nil {
		return items, nil
	}
	missed := items[:0]

	bytesIter, err := self.redis.Get(ctx, len(items), func(i int) string {
		return self.ResolveKey(items[i].Key)
	})
	if err != nil {
		return nil, fmt.Errorf("failed get redis items: %w", err)
	}

	var nextItem int
	for b, ok := bytesIter(); ok; b, ok = bytesIter() {
		item := items[nextItem]
		if len(b) == 0 {
			missed = append(missed, item)
			self.addMiss()
		} else {
			self.addHit()
			if self.localCache != nil && !item.SkipLocalCache {
				self.localCache.Set(self.ResolveKey(item.Key), b)
			}
			if err := self.acquireUnmarshal(ctx, g, b, item.Value); err != nil {
				return nil, err
			}
		}
		nextItem++
	}

	return missed, nil
}

func (self *Cache) GetSet(ctx context.Context, items ...*Item) error {
	missed, err := self.Get(ctx, items...)
	if err != nil {
		return err
	} else if len(missed) == 0 {
		return nil
	}

	return self.Set(ctx, items...)
}
