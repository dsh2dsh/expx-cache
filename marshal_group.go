package cache

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"golang.org/x/sync/errgroup"
)

func newMarshalGroup(ctx context.Context, cache *Cache) *marshalGroup {
	mg, ctx := errgroup.WithContext(ctx)
	mg.SetLimit(runtime.GOMAXPROCS(0))
	vg, ctx := errgroup.WithContext(ctx)

	return &marshalGroup{
		cache: cache,
		mg:    mg,
		vg:    vg,
		ctx:   ctx,
	}
}

type marshalGroup struct {
	cache *Cache

	mg *errgroup.Group
	vg *errgroup.Group

	ctx context.Context
}

func (self *marshalGroup) Context() context.Context {
	return self.ctx
}

func (self *marshalGroup) SetLimit(n int) *marshalGroup {
	if n > 0 {
		self.vg.SetLimit(n)
	} else {
		self.vg.SetLimit(runtime.GOMAXPROCS(0))
	}
	return self
}

func (self *marshalGroup) Go(item *Item, cb func(b []byte)) {
	if item.Do == nil {
		self.marshalValue(item, cb)
	} else {
		self.marshalDo(item, cb)
	}
}

func (self *marshalGroup) marshalValue(item *Item, cb func(b []byte)) {
	self.mg.Go(func() error {
		b, err := self.cache.Marshal(item.Value)
		if err != nil {
			return fmt.Errorf("marshal item %q: %w", item.Key, err)
		}
		cb(b)
		return nil
	})
}

func (self *marshalGroup) marshalDo(item *Item, cb func(b []byte)) {
	self.vg.Go(func() error {
		item.Ctx = self.ctx
		v, err := item.value()
		if err != nil {
			return fmt.Errorf("item value %q: %w", item.Key, err)
		}

		if self.Context().Err() != nil {
			return nil
		}

		marshalChan := make(chan error)
		self.mg.Go(func() error {
			b, err := self.cache.Marshal(v)
			if err != nil {
				err = fmt.Errorf("marshal item %q: %w", item.Key, err)
			} else {
				cb(b)
			}
			marshalChan <- err
			return nil
		})

		return <-marshalChan
	})
}

func (self *marshalGroup) Wait() error {
	err1 := self.vg.Wait()
	err2 := self.mg.Wait()
	if err := errors.Join(err1, err2); err != nil {
		return fmt.Errorf("errgroup: %w", err)
	}
	return nil
}
