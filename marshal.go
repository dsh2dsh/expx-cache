package cache

import (
	"context"
	"fmt"
	"runtime"

	"github.com/klauspost/compress/s2"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
)

const (
	compressionThreshold = 64
	noCompression        = 0x0
	s2Compression        = 0x1
)

func (self *Cache) Marshal(value any) ([]byte, error) {
	return self.marshal(value)
}

func (self *Cache) Unmarshal(b []byte, value any) error {
	return self.unmarshal(b, value)
}

func (self *Cache) marshalItems(parentCtx context.Context, items []Item,
) ([][]byte, error) {
	g, ctx := self.valueGroup(parentCtx)
	bytes := make([][]byte, len(items))

	for i := range items {
		if ctx.Err() != nil {
			break
		}
		i, item := i, &items[i]
		g.Go(func() error {
			b, err := item.marshal(ctx, func(v any) ([]byte, error) {
				return self.acquireMarshal(ctx, v)
			})
			if err != nil {
				return err
			}
			bytes[i] = b
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed marshal items: %w", err)
	} else if parentCtx.Err() != nil {
		return nil, fmt.Errorf("failed marshal items: %w", context.Cause(ctx))
	}

	return bytes, nil
}

func (self *Cache) valueGroup(ctx context.Context) (*errgroup.Group, context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	if self.valueProcs < 1 {
		g.SetLimit(runtime.GOMAXPROCS(0))
	} else {
		g.SetLimit(self.valueProcs)
	}
	return g, ctx
}

func (self *Cache) acquireMarshal(ctx context.Context, v any) ([]byte, error) {
	if err := self.marshalers.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}
	defer self.marshalers.Release(1)
	b, err := self.marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (self *Cache) acquireUnmarshal(ctx context.Context, g *errgroup.Group,
	b []byte, v any,
) error {
	if err := self.marshalers.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	g.Go(func() error {
		defer self.marshalers.Release(1)
		if err := self.unmarshal(b, v); err != nil {
			return err
		}
		return nil
	})
	return nil
}

// --------------------------------------------------

func marshal(value any) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal: msgpack error: %w", err)
	}

	return compress(b), nil
}

func unmarshal(b []byte, value any) error {
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

	switch c := b[len(b)-1]; c {
	case noCompression:
		b = b[:len(b)-1]
	case s2Compression:
		b = b[:len(b)-1]
		if decoded, err := s2.Decode(nil, b); err != nil {
			return fmt.Errorf("unmarshal: decompress error: %w", err)
		} else {
			b = decoded
		}
	default:
		return fmt.Errorf("unmarshal: unknown compression method: %x", c)
	}

	if err := msgpack.Unmarshal(b, value); err != nil {
		return fmt.Errorf("unmarshal msgpack error: %w", err)
	}

	return nil
}

func compress(data []byte) []byte {
	if len(data) < compressionThreshold {
		b := make([]byte, len(data)+1)
		copy(b, data)
		b = b[:len(data)]               // make a sub-slice for safe append
		return append(b, noCompression) //nolint:makezero // b is a sub-slice
	}

	b := make([]byte, s2.MaxEncodedLen(len(data))+1)
	b = s2.Encode(b, data)
	return append(b, s2Compression) //nolint:makezero // b is a sub-slice
}
