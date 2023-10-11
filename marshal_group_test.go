package cache

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalGroup_SetLimit(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	ctx := context.Background()
	g := newMarshalGroup(ctx, cache)
	require.NotNil(t, g)
	assert.False(t, g.groupDo)

	assert.Same(t, g, g.SetLimit(1))
	assert.True(t, g.groupDo)
}

func TestMarshalGroup_errorAcquire(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()

	const foobar = "foobar"
	foobarDo := func(item *Item) (any, error) {
		return foobar, nil
	}

	marshalAssert := func(t *testing.T, g *marshalGroup, item *Item) {
		var callCount uint64
		g.GoMarshal(item, func(b []byte) {
			atomic.AddUint64(&callCount, 1)
		})
		assert.Equal(t, uint64(0), callCount)
	}

	tests := []struct {
		name      string
		item      Item
		configure func(t *testing.T, g *marshalGroup) *marshalGroup
		assert    func(t *testing.T, g *marshalGroup, item *Item)
	}{
		{
			name:   "GoMarshal with Value",
			item:   Item{Key: testKey, Value: foobar},
			assert: marshalAssert,
		},
		{
			name:   "GoMarshal with Do",
			item:   Item{Key: testKey, Do: foobarDo},
			assert: marshalAssert,
		},
		{
			name: "GoMarshal with Do and groupDo",
			item: Item{Key: testKey, Do: foobarDo},
			configure: func(t *testing.T, g *marshalGroup) *marshalGroup {
				return g.SetLimit(1)
			},
			assert: marshalAssert,
		},
		{
			name: "GoUnmarshal",
			item: Item{Key: testKey},
			assert: func(t *testing.T, g *marshalGroup, item *Item) {
				b := valueNoError[[]byte](t)(marshal(foobar))
				var got string
				item.Value = &got
				g.GoUnmarshal(b, item)
				assert.Empty(t, got)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			cache := New()
			require.NotNil(t, cache)

			ctx, cancel := context.WithCancel(context.Background())
			g := newMarshalGroup(ctx, cache)
			require.NotNil(t, g)
			if tt.configure != nil {
				g = tt.configure(t, g)
				require.NotNil(t, g)
			}
			for cache.workers.TryAcquire(1) {
			}

			sig := make(chan struct{})
			go func() {
				<-sig
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()

			item := tt.item
			sig <- struct{}{}
			tt.assert(t, g, &item)
			assert.NoError(t, g.Wait())
		})
	}
}

func TestMarshalGroup_errorMarshal(t *testing.T) {
	const foobar = "foobar"
	foobarDo := func(item *Item) (any, error) {
		return foobar, nil
	}

	marshalAssert := func(t *testing.T, g *marshalGroup, item *Item) {
		var callCount uint64
		g.GoMarshal(item, func(b []byte) {
			atomic.AddUint64(&callCount, 1)
		})
		assert.Equal(t, uint64(0), callCount)
	}

	tests := []struct {
		name      string
		item      Item
		configure func(t *testing.T, g *marshalGroup) *marshalGroup
		assert    func(t *testing.T, g *marshalGroup, item *Item)
	}{
		{
			name:   "GoMarshal with Value",
			item:   Item{Key: testKey, Value: foobar},
			assert: marshalAssert,
		},
		{
			name:   "GoMarshal with Do",
			item:   Item{Key: testKey, Do: foobarDo},
			assert: marshalAssert,
		},
		{
			name: "GoMarshal with Do and groupDo",
			item: Item{Key: testKey, Do: foobarDo},
			configure: func(t *testing.T, g *marshalGroup) *marshalGroup {
				return g.SetLimit(1)
			},
			assert: marshalAssert,
		},
		{
			name: "GoUnmarshal",
			item: Item{Key: testKey},
			assert: func(t *testing.T, g *marshalGroup, item *Item) {
				b := valueNoError[[]byte](t)(marshal(foobar))
				var got string
				item.Value = &got
				g.GoUnmarshal(b, item)
				assert.Empty(t, got)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := New().WithMarshal(func(v any) ([]byte, error) {
				return nil, io.EOF
			}).WithUnmarshal(func(b []byte, v any) error {
				return io.EOF
			})
			require.NotNil(t, cache)

			ctx := context.Background()
			g := newMarshalGroup(ctx, cache)
			require.NotNil(t, g)
			if tt.configure != nil {
				g = tt.configure(t, g)
				require.NotNil(t, g)
			}

			item := tt.item
			tt.assert(t, g, &item)
			assert.ErrorIs(t, g.Wait(), io.EOF)
		})
	}
}

func TestMarshalGroup_GoMarshal_marshalCancelled(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	ctx, cancel := context.WithCancel(context.Background())
	g := newMarshalGroup(ctx, cache)
	require.NotNil(t, g)

	var doCount uint64
	item := Item{Key: testKey, Do: func(item *Item) (any, error) {
		atomic.AddUint64(&doCount, 1)
		cancel()
		return "foobar", nil
	}}

	var marshalCount uint64
	g.GoMarshal(&item, func(b []byte) {
		atomic.AddUint64(&marshalCount, 1)
	})
	assert.NoError(t, g.Wait())
	assert.Equal(t, uint64(1), doCount)
	assert.Equal(t, uint64(0), marshalCount)
}
