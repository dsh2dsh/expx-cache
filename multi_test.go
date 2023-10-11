package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMultiCache(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	multi := NewMultiCache(cache)
	require.NotNil(t, multi)
	assert.Equal(t, &MultiCache{cache: cache}, multi)
}

func TestMultiCache_WithSkipLocalCache(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	m := cache.Multi()
	require.NotNil(t, m)
	assert.False(t, m.skipLocalCache)

	assert.Same(t, m, m.WithSkipLocalCache(true))
	assert.True(t, m.skipLocalCache)
}

func TestMultiCache_WithGroupDo(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	m := cache.Multi()
	require.NotNil(t, m)
	assert.Equal(t, 0, m.groupLimit)

	assert.Same(t, m, m.WithGroupDo(3))
	assert.Equal(t, 3, m.groupLimit)
}

func TestMultiCache_WithSkipRedis(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	m := cache.Multi()
	require.NotNil(t, m)
	assert.False(t, m.skipRedis)

	assert.Same(t, m, m.WithSkipRedis(true))
	assert.True(t, m.skipRedis)
}

func TestMultiCache_GetSet_errorGet(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	obj := CacheableObject{
		Str: "mystring",
		Num: 42,
	}

	ctx, cancel := context.WithCancel(context.Background())
	item := Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: &obj,
	}
	require.NoError(t, cache.Set(&item))

	cancel()
	assert.ErrorIs(t, cache.MGetSet(ctx, []*Item{&item}), context.Canceled)
}
