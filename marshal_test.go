package cache

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCache_WithMarshal(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	var called bool
	cache.WithMarshal(func(v any) ([]byte, error) {
		called = true
		return marshal(v)
	})

	ctx := t.Context()
	value := "abc"
	require.NoError(t, cache.Set(ctx, Item{Key: testKey, Value: &value}))

	assert.True(t, called, "custom marshall func wasn't called")
}

func TestCache_WithUnmarshal(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	ctx := t.Context()
	value := "abc"
	item := Item{Key: testKey, Value: &value}
	require.NoError(t, cache.Set(ctx, item))

	var called bool
	cache.WithUnmarshal(func(b []byte, v any) error {
		called = true
		return unmarshal(b, v)
	})
	item.Value = &value
	assert.Empty(t, valueNoError[[]Item](t)(cache.Get(ctx, item)))
	assert.True(t, called, "custom unmarshall func wasn't called")
}

func TestCache_Marshal_nil(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)
	assert.Nil(t, valueNoError[[]byte](t)(cache.Marshal(nil)))
}

func TestCache_Marshal_compression(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	s := strings.Repeat("foobar", 100)
	b := valueNoError[[]byte](t)(cache.Marshal(&s))
	assert.NotNil(t, b)
	assert.Equal(t, s2Compression, int(b[len(b)-1]))
}

func TestCache_Marshal_noCompression(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	s := "foobar"
	b := valueNoError[[]byte](t)(cache.Marshal(&s))
	assert.NotNil(t, b)
	assert.Equal(t, noCompression, int(b[len(b)-1]))
}

type msgpackErrItem struct {
	Foo string
	err error
}

func (self *msgpackErrItem) EncodeMsgpack(enc *msgpack.Encoder) error {
	return self.err
}

func TestCache_Marshal_msgpackErr(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	wantErr := errors.New("expected error")
	b, err := cache.Marshal(&msgpackErrItem{Foo: "bar", err: wantErr})
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, b)
}

func TestCache_Unmarshal_nil(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	require.NoError(t, cache.Unmarshal([]byte{}, nil))
	require.NoError(t, cache.Unmarshal([]byte("foobar"), nil))
}

func TestCache_Unmarshal_compression(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	type fooItem struct {
		Foo string
	}
	item := fooItem{
		Foo: strings.Repeat("foobar", 100),
	}

	b := valueNoError[[]byte](t)(cache.Marshal(&item))
	assert.Equal(t, s2Compression, int(b[len(b)-1]))

	gotItem := fooItem{}
	require.NoError(t, cache.Unmarshal(b, &gotItem))
	assert.Equal(t, item, gotItem)

	require.ErrorContains(t, cache.Unmarshal([]byte{0x0, 0xff}, &gotItem),
		"unknown compression method")

	require.ErrorContains(t, cache.Unmarshal([]byte{0x1, s2Compression}, &gotItem),
		"unmarshal: decompress error")
}

func TestCache_marshalersAcquireCanceled(t *testing.T) {
	t.Parallel()

	const foobar = "foobar"
	assertMarshal := func(ctx context.Context, t *testing.T, c *Cache, item Item) {
		var callCount uint32
		c.WithMarshal(func(v any) ([]byte, error) {
			atomic.AddUint32(&callCount, 1)
			return nil, nil
		})
		b, err := c.marshalItems(ctx, []Item{item})
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, b)
		assert.Equal(t, uint32(0), callCount)
	}

	tests := []struct {
		name   string
		item   Item
		assert func(ctx context.Context, t *testing.T, c *Cache, item Item)
	}{
		{
			name:   "marshalItems with Value",
			item:   Item{Key: testKey, Value: foobar},
			assert: assertMarshal,
		},
		{
			name: "marshalItems with Do",
			item: Item{Key: testKey, Do: func(ctx context.Context) (any, error) {
				return foobar, nil
			}},
			assert: assertMarshal,
		},
		{
			name: "GoUnmarshal",
			item: Item{Key: testKey},
			assert: func(ctx context.Context, t *testing.T, c *Cache, item Item) {
				var callCount uint32
				c.WithUnmarshal(func(b []byte, v any) error {
					atomic.AddUint32(&callCount, 1)
					return nil
				})
				g := c.unmarshalGroup(ctx)
				require.ErrorIs(t, g.GoUnmarshal(nil, nil), context.Canceled)
				assert.Equal(t, uint32(0), callCount)
			},
		},
		{
			name: "Get",
			item: Item{Key: testKey, Value: foobar},
			assert: func(ctx context.Context, t *testing.T, c *Cache, item Item) {
				require.NoError(t, c.Set(ctx, item))
				b, err := c.Get(ctx, item, item)
				require.ErrorIs(t, err, context.Canceled)
				assert.Nil(t, b)
			},
		},
		{
			name: "unmarshalItems",
			item: Item{Key: testKey},
			assert: func(ctx context.Context, t *testing.T, c *Cache, item Item) {
				var callCount uint32
				c.WithUnmarshal(func(b []byte, v any) error {
					atomic.AddUint32(&callCount, 1)
					return nil
				})
				err := c.unmarshalItems(ctx, [][]byte{nil, nil}, []Item{item, item})
				require.ErrorIs(t, err, context.Canceled)
				assert.Equal(t, uint32(0), callCount)
			},
		},
	}

	ctx := t.Context()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := New().WithTinyLFU(1000, time.Minute)
			require.NotNil(t, cache)

			ctx, cancel := context.WithCancel(ctx)
			for cache.marshalers.TryAcquire(1) {
			}
			sig := make(chan struct{})
			go func() {
				<-sig
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()

			sig <- struct{}{}
			tt.assert(ctx, t, cache, tt.item)
		})
	}
}

func TestCache_marshalErrGroup(t *testing.T) {
	const foobar = "foobar"
	wantErr := errors.New("expected error")

	marshalAssert := func(ctx context.Context, t *testing.T, c *Cache, item Item) {
		c.WithMarshal(func(v any) ([]byte, error) {
			return nil, wantErr
		})
		b, err := c.marshalItems(ctx, []Item{item})
		require.ErrorIs(t, err, wantErr)
		assert.Nil(t, b)
	}

	tests := []struct {
		name   string
		item   Item
		assert func(ctx context.Context, t *testing.T, c *Cache, item Item)
	}{
		{
			name:   "marshalItems with Value",
			item:   Item{Key: testKey, Value: foobar},
			assert: marshalAssert,
		},
		{
			name: "marshalItems with Do",
			item: Item{Key: testKey, Do: func(ctx context.Context) (any, error) {
				return foobar, nil
			}},
			assert: marshalAssert,
		},
		{
			name: "GoUnmarshal",
			item: Item{Key: testKey},
			assert: func(ctx context.Context, t *testing.T, c *Cache, item Item) {
				c.WithUnmarshal(func(b []byte, v any) error {
					return wantErr
				})
				b := valueNoError[[]byte](t)(marshal(foobar))
				g := c.unmarshalGroup(ctx)
				require.NoError(t, g.GoUnmarshal(b, nil))
				require.ErrorIs(t, g.Wait(), wantErr)
			},
		},
		{
			name: "unmarshalItems",
			item: Item{Key: testKey},
			assert: func(ctx context.Context, t *testing.T, c *Cache, item Item) {
				c.WithUnmarshal(func(b []byte, v any) error {
					return wantErr
				})
				b := valueNoError[[]byte](t)(marshal(foobar))
				err := c.unmarshalItems(ctx, [][]byte{b, b}, []Item{item, item})
				require.ErrorIs(t, err, wantErr)
			},
		},
	}

	ctx := t.Context()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := New()
			require.NotNil(t, cache)
			tt.assert(ctx, t, cache, tt.item)
		})
	}
}

func TestCache_marshalItems_canceled(t *testing.T) {
	tests := []struct {
		name  string
		items int
	}{
		{
			name:  "1 item",
			items: 1,
		},
		{
			name:  "2 items",
			items: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			const foobar = "foobar"
			var callCount uint64

			items := make([]Item, tt.items)
			for i := range items {
				items[i] = Item{
					Key: testKey,
					Do: func(ctx context.Context) (any, error) {
						n := atomic.AddUint64(&callCount, 1)
						if n == 1 {
							cancel()
						} else {
							<-ctx.Done()
						}
						return foobar, nil
					},
				}
			}

			cache := New()
			require.NotNil(t, cache)
			for cache.marshalers.TryAcquire(1) {
			}
			b, err := cache.marshalItems(ctx, items)
			require.ErrorIs(t, err, context.Canceled)
			assert.Nil(t, b)
		})
	}
}

func TestCache_marshalGroup_limit(t *testing.T) {
	tests := []struct {
		name      string
		makeCache func(t *testing.T) *Cache
		callCount uint64
	}{
		{
			name:      "default",
			makeCache: func(t *testing.T) *Cache { return New() },
			callCount: uint64(runtime.GOMAXPROCS(0)),
		},
		{
			name:      "limit 0",
			makeCache: func(t *testing.T) *Cache { return New(WithItemMaxProcs(0)) },
			callCount: uint64(runtime.GOMAXPROCS(0)),
		},
		{
			name: "limit 1",
			makeCache: func(t *testing.T) *Cache {
				cache1 := New()
				cache2 := cache1.WithItemMaxProcs(1)
				require.NotSame(t, cache1, cache2)
				return cache2
			},
			callCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.makeCache(t)
			ctx, cancel := context.WithCancel(t.Context())
			g := cache.marshalGroup(ctx)

			var callCount uint64
			for g.g.TryGo(func() error {
				atomic.AddUint64(&callCount, 1)
				<-ctx.Done()
				return nil
			}) {
			}

			cancel()
			require.NoError(t, g.Wait())
			assert.Equal(t, tt.callCount, callCount)
		})
	}
}

func TestCache_unmarshalItems_gCanceled(t *testing.T) {
	const maxItems = 3
	var bytes [maxItems][]byte
	var items [maxItems]Item
	for i := range items {
		items[i] = Item{Key: testKey}
	}

	testErr := errors.New("test error")
	var callCount uint32
	cache := New(WithMarshalMaxProcs(1)).WithUnmarshal(
		func(b []byte, v any) error {
			atomic.AddUint32(&callCount, 1)
			return testErr
		})
	require.NotNil(t, cache)

	ctx := t.Context()
	err := cache.unmarshalItems(ctx, bytes[:], items[:])
	require.ErrorIs(t, err, testErr)
}
