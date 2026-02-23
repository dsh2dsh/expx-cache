package cache

import (
	"context"
	"fmt"
	"runtime"

	"github.com/klauspost/compress/s2"
	"golang.org/x/sync/errgroup"
)

const compressionThreshold = 64

const (
	noCompression = iota
	s2Compression
)

func (self *Cache) Marshal(value any) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	}

	b, err := self.marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	return compress(b), nil
}

func compress(data []byte) []byte {
	if len(data) < compressionThreshold {
		b := make([]byte, len(data)+1)
		copy(b, data)
		b[len(data)] = noCompression
		return b
	}

	b := make([]byte, s2.MaxEncodedLen(len(data))+1)
	b = s2.Encode(b, data)
	return append(b, s2Compression) //nolint:makezero // b is a sub-slice
}

func (self *Cache) Unmarshal(b []byte, value any) error {
	if len(b) == 0 {
		return nil
	}

	switch value := value.(type) {
	case nil:
		return nil
	case *[]byte:
		clone := make([]byte, len(b))
		copy(clone, b)
		*value = clone
		return nil
	case *string:
		*value = string(b)
		return nil
	}

	b, err := uncompress(b)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	if err := self.unmarshal(b, value); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	return nil
}

func uncompress(b []byte) ([]byte, error) {
	c := b[len(b)-1]
	switch c {
	case noCompression:
		return b[:len(b)-1], nil
	case s2Compression:
		b = b[:len(b)-1]
		decoded, err := s2.Decode(nil, b)
		if err != nil {
			return nil, fmt.Errorf("decompress error: %w", err)
		}
		return decoded, nil
	}
	return nil, fmt.Errorf("unknown compression method: %x", c)
}

func (self *Cache) marshalItems(ctx context.Context, items []Item,
) ([][]byte, error) {
	g := self.marshalGroup(ctx)
	bytes := make([][]byte, len(items))
	var err error
	for i := range items {
		if g.Canceled() {
			err = g.Cause()
			break
		}
		g.GoMarshal(&items[i], func(b []byte) { bytes[i] = b })
	}

	const errMsg = "failed marshal items: %w"
	if err2 := g.Wait(); err2 != nil {
		return nil, fmt.Errorf(errMsg, err2)
	} else if err != nil {
		return nil, fmt.Errorf(errMsg, err)
	}
	return bytes, nil
}

func (self *Cache) unmarshalItems(ctx context.Context, bytes [][]byte,
	items []Item,
) (err error) {
	g := self.unmarshalGroup(ctx)
	for i := range items {
		if g.Canceled() {
			err = g.Cause()
			break
		}
		if err = g.GoUnmarshal(bytes[i], items[i].Value); err != nil {
			break
		}
	}

	const errMsg = "unmarshal items(%v): %w"
	if err2 := g.Wait(); err2 != nil {
		err = fmt.Errorf(errMsg, len(items), err2)
	} else if err != nil {
		err = fmt.Errorf(errMsg, len(items), err)
	}
	return err
}

// --------------------------------------------------

func (self *Cache) marshalGroup(ctx context.Context) marshalGroup {
	g, ctx := errgroup.WithContext(ctx)
	if self.valueProcs < 1 {
		g.SetLimit(runtime.GOMAXPROCS(0))
	} else {
		g.SetLimit(self.valueProcs)
	}
	return marshalGroup{ctx: ctx, g: g, cache: self}
}

func (self *Cache) unmarshalGroup(ctx context.Context) marshalGroup {
	g, ctx := errgroup.WithContext(ctx)
	return marshalGroup{ctx: ctx, g: g, cache: self}
}

type marshalGroup struct {
	ctx   context.Context
	g     *errgroup.Group
	cache *Cache
}

func (self *marshalGroup) GoMarshal(item *Item, fn func([]byte)) {
	self.g.Go(func() error {
		b, err := item.marshal(self.ctx, self.marshal)
		if err != nil {
			return err
		}
		fn(b)
		return nil
	})
}

func (self *marshalGroup) marshal(v any) ([]byte, error) {
	if err := self.cache.marshalers.Acquire(self.ctx, 1); err != nil {
		return nil, fmt.Errorf("failed acquire marshalers: %w", err)
	}
	defer self.cache.marshalers.Release(1)
	b, err := self.cache.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (self *marshalGroup) GoUnmarshal(b []byte, v any) error {
	if err := self.cache.marshalers.Acquire(self.ctx, 1); err != nil {
		return fmt.Errorf("failed acquire marshalers: %w", err)
	}
	self.g.Go(func() error {
		defer self.cache.marshalers.Release(1)
		if err := self.cache.Unmarshal(b, v); err != nil {
			return err
		}
		return nil
	})
	return nil
}

func (self *marshalGroup) Ctx() context.Context {
	return self.ctx
}

func (self *marshalGroup) Canceled() bool {
	return self.ctx.Err() != nil
}

func (self *marshalGroup) Cause() error {
	return context.Cause(self.ctx) //nolint:wrapcheck // we need the error as is
}

func (self *marshalGroup) Wait() error {
	return self.g.Wait() //nolint:wrapcheck // caller will wrap the error
}
