package cache

import (
	"context"
	"fmt"
)

func (self *MultiCache) Get(ctx context.Context, items []*Item) ([]*Item, error) {
	localHit, localMiss := self.localGet(items)
	hit, miss, err := self.redisGet(ctx, localMiss, localHit)
	if err != nil {
		return nil, err
	}

	useLocalCache := self.cache.localCache != nil && !self.skipLocalCache
	if useLocalCache && len(hit) > len(localHit) {
		self.localSet(hit[len(localHit):])
	}

	if len(hit) > 0 {
		if err := self.unmarshalItems(ctx, hit); err != nil {
			return nil, err
		}
	}

	if len(miss) == 0 {
		return nil, nil
	}

	missedItems := make([]*Item, len(miss))
	for i := range miss {
		missedItems[i] = miss[i].Item
	}

	return missedItems, nil
}

func (self *MultiCache) localGet(items []*Item) (hit []blobItem, miss []blobItem) {
	hit = make([]blobItem, 0, len(items))
	miss = make([]blobItem, 0, len(items))

	skipLocalCache := self.cache.localCache == nil || self.skipLocalCache

	for _, item := range items {
		blob := blobItem{Key: self.cache.resolveKey(item.Key), Item: item}
		if skipLocalCache || item.SkipLocalCache {
			miss = append(miss, blob)
		} else if b := self.cache.localCache.Get(blob.Key); len(b) == 0 {
			miss = append(miss, blob)
			self.cache.addLocalMiss()
		} else {
			blob.Value = b
			hit = append(hit, blob)
			self.cache.addLocalHit()
		}
	}

	return
}

func (self *MultiCache) redisGet(
	ctx context.Context, items []blobItem, hit []blobItem,
) ([]blobItem, []blobItem, error) {
	if self.cache.redis == nil || self.skipRedis || len(items) == 0 {
		return hit, items, nil
	}

	keys := make([]string, len(items))
	for i := range items {
		keys[i] = items[i].Key
	}
	blobs, err := self.cache.redis.MGet(ctx, keys)
	if err != nil {
		return nil, nil, fmt.Errorf("cache: mget: %w", err)
	}

	miss := items[:0]
	for i := range items {
		if len(blobs[i]) == 0 {
			miss = append(miss, items[i])
			self.cache.addMiss()
		} else {
			items[i].Value = blobs[i]
			hit = append(hit, items[i])
			self.cache.addHit()
		}
	}

	return hit, miss, nil
}

func (self *MultiCache) unmarshalItems(ctx context.Context, items []blobItem) error {
	g := newMarshalGroup(ctx, self.cache)
	for i := range items {
		if g.Context().Err() != nil {
			break
		}
		item := &items[i]
		g.GoUnmarshal(item.Value, item.Item)
	}

	if err := g.Wait(); err != nil {
		return err
	} else if ctx.Err() != nil {
		return fmt.Errorf("cache: context cancelled: %w", context.Cause(ctx))
	}

	return nil
}
