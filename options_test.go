package cache

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithMarshalMaxProcs(t *testing.T) {
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
			makeCache: func(t *testing.T) *Cache { return New(WithMarshalMaxProcs(0)) },
			callCount: uint64(runtime.GOMAXPROCS(0)),
		},
		{
			name:      "limit 1",
			makeCache: func(t *testing.T) *Cache { return New(WithMarshalMaxProcs(1)) },
			callCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.makeCache(t)
			ctx, cancel := context.WithCancel(context.Background())

			var callCount uint64
			var wg sync.WaitGroup
			for cache.marshalers.TryAcquire(1) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					atomic.AddUint64(&callCount, 1)
					<-ctx.Done()
				}()
			}
			cancel()

			wg.Wait()
			assert.Equal(t, tt.callCount, callCount)
		})
	}
}

func TestWithNoErr(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	require.NoError(t, cache.Err())

	wantErr1 := errors.New("test error")
	require.ErrorIs(t, cache.redisCacheError(wantErr1), wantErr1)
	require.ErrorIs(t, cache.Err(), wantErr1)

	cache2 := cache.New(WithNoErr())
	require.NoError(t, cache2.Err())
	require.ErrorIs(t, cache.Err(), wantErr1)
}
