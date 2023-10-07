package cache

import (
	"context"
	"fmt"
	"time"
)

func (self *MultiCache) Set(ctx context.Context, items []*Item) error {
	blobs, err := self.marshalItems(ctx, items)
	if err != nil {
		return err
	}

	useLocal := self.cache.localCache != nil && !self.skipLocalCache
	useRedis := self.cache.redis != nil && !self.skipRedis

	switch {
	case useLocal && useRedis:
		done := make(chan error)
		go func() {
			done <- self.redisSet(ctx, blobs)
		}()
		self.localSet(blobs)
		err = <-done
	case useLocal:
		self.localSet(blobs)
	case useRedis:
		err = self.redisSet(ctx, blobs)
	}

	return err
}

func (self *MultiCache) marshalItems(
	ctx context.Context, items []*Item,
) ([]blobItem, error) {
	g := newMarshalGroup(ctx, self.cache).SetLimit(self.groupLimit)

	blobs := make([]blobItem, len(items))
	for i, item := range items {
		if g.Context().Err() != nil {
			break
		}
		i, item := i, item
		g.GoMarshal(item, func(b []byte) {
			blobs[i] = blobItem{
				Key:   self.cache.ResolveKey(item.Key),
				Value: b,
				Item:  item,
			}
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	} else if ctx.Err() != nil {
		return nil, fmt.Errorf("cache: context cancelled: %w", context.Cause(ctx))
	}

	return blobs, nil
}

func (self *MultiCache) localSet(items []blobItem) {
	for i := range items {
		item := &items[i]
		if len(item.Value) > 0 && !item.Item.SkipLocalCache {
			self.cache.localCache.Set(item.Key, item.Value)
		}
	}
}

func (self *MultiCache) redisSet(ctx context.Context, items []blobItem) error {
	i := 0
	err := self.cache.redis.MSet(ctx,
		func() (key string, b []byte, ttl time.Duration, ok bool) {
			for i < len(items) {
				item := &items[i]
				ttl = self.cache.ItemTTL(item.Item)
				i++
				if ttl > 0 && len(item.Value) > 0 {
					return item.Key, item.Value, ttl, true
				}
			}
			return
		})
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}

	return nil
}
