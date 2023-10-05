package cache

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

func newMarshalGroup(ctx context.Context, cache *Cache) *marshalGroup {
	g, ctx := errgroup.WithContext(ctx)
	return &marshalGroup{
		cache: cache,
		g:     g,
		ctx:   ctx,
	}
}

type marshalGroup struct {
	cache *Cache

	ctx     context.Context
	g       *errgroup.Group
	groupDo bool
}

func (self *marshalGroup) Context() context.Context {
	return self.ctx
}

func (self *marshalGroup) SetLimit(n int) *marshalGroup {
	if n > 0 {
		self.g.SetLimit(n)
		self.groupDo = true
	}
	return self
}

func (self *marshalGroup) GoMarshal(item *Item, cb func(b []byte)) {
	if item.Do == nil {
		self.marshalValue(item, cb)
	} else {
		self.marshalDo(item, cb)
	}
}

func (self *marshalGroup) marshalValue(item *Item, cb func(b []byte)) {
	if err := self.cache.workers.Acquire(self.ctx, 1); err != nil {
		return
	}

	self.g.Go(func() error {
		defer self.cache.workers.Release(1)
		b, err := self.cache.Marshal(item.Value)
		if err != nil {
			return fmt.Errorf("marshal item %q: %w", item.Key, err)
		}
		cb(b)
		return nil
	})
}

func (self *marshalGroup) marshalDo(item *Item, cb func(b []byte)) {
	acquired := false
	if !self.groupDo {
		if err := self.cache.workers.Acquire(self.ctx, 1); err != nil {
			return
		}
		acquired = true
	}

	self.g.Go(func() error {
		if acquired {
			defer self.cache.workers.Release(1)
		}

		item.Ctx = self.ctx
		v, err := item.value()
		if err != nil {
			return fmt.Errorf("item value %q: %w", item.Key, err)
		}

		if self.ctx.Err() != nil {
			return nil
		} else if !acquired {
			if err := self.cache.workers.Acquire(self.ctx, 1); err != nil {
				return nil
			}
			defer self.cache.workers.Release(1)
		}

		b, err := self.cache.Marshal(v)
		if err != nil {
			return fmt.Errorf("marshal item %q: %w", item.Key, err)
		}
		cb(b)
		return nil
	})
}

func (self *marshalGroup) GoUnmarshal(b []byte, item *Item) {
	if err := self.cache.workers.Acquire(self.ctx, 1); err != nil {
		return
	}

	self.g.Go(func() error {
		defer self.cache.workers.Release(1)
		if err := self.cache.Unmarshal(b, item.Value); err != nil {
			return fmt.Errorf("unmarshal item %q: %w", item.Key, err)
		}
		return nil
	})
}

func (self *marshalGroup) Wait() error {
	if err := self.g.Wait(); err != nil {
		return fmt.Errorf("errgroup: %w", err)
	}
	return nil
}
