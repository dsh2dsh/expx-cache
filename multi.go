package cache

import "context"

func NewMultiCache(cache *Cache) *MultiCache {
	return &MultiCache{cache: cache}
}

type MultiCache struct {
	cache *Cache

	// groupLimit sets num of goroutines we'll use for calling Do of items.
	groupLimit int

	// skipLocalCache skips local cache as if it is not set.
	skipLocalCache bool

	// skipRedis skips redis cache as if itsn't set
	skipRedis bool
}

func (self *MultiCache) WithSkipLocalCache(v bool) *MultiCache {
	self.skipLocalCache = v
	return self
}

func (self *MultiCache) WithGroupDo(n int) *MultiCache {
	self.groupLimit = n
	return self
}

func (self *MultiCache) WithSkipRedis(v bool) *MultiCache {
	self.skipRedis = v
	return self
}

// --------------------------------------------------

func (self *MultiCache) GetSet(ctx context.Context, items []*Item) error {
	missedItems, err := self.Get(ctx, items)
	if err != nil {
		return err
	} else if len(missedItems) == 0 {
		return nil
	}

	return self.Set(ctx, missedItems)
}
