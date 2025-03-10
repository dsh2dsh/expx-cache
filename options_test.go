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
			ctx, cancel := context.WithCancel(t.Context())

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
	assert.False(t, cache.Failed())

	wantErr1 := errors.New("test error")
	require.ErrorIs(t, cache.redisCacheError(wantErr1), wantErr1)
	assert.True(t, cache.Failed())

	cache2 := cache.New(WithNoErr())
	assert.False(t, cache2.Failed())
	assert.True(t, cache.Failed())
}

func TestWithLockNotFound(t *testing.T) {
	const foobar = "foobar"
	var callCount int
	cache := New(WithLockNotFound(func(key, value string) error {
		callCount++
		assert.Equal(t, testKey, key)
		assert.Equal(t, foobar, value)
		return nil
	}))
	require.NotNil(t, cache)

	l := cache.lock(cache.ResolveKeyLock(testKey), cache.ResolveKey(testKey))
	require.NoError(t, l.notFound(testKey, foobar))
	assert.Equal(t, 1, callCount)
}
