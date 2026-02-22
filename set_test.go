package cache

import (
	"context"
	"errors"
	"iter"
	"testing"

	cacheRedis "github.com/dsh2dsh/expx-cache-redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_Set_errorMarshal(t *testing.T) {
	localCache := &MoqLocalCache{}
	wantErr := errors.New("expected error")
	cache := New().WithLocalCache(localCache).WithMarshal(
		func(v any) ([]byte, error) { return nil, wantErr })

	ctx := t.Context()
	item := Item{Key: testKey, Value: "foobar"}

	require.ErrorIs(t, cache.Set(ctx, item), wantErr)
	require.ErrorIs(t, cache.Set(ctx, item, item), wantErr)
}

func TestCache_Set_withoutCache(t *testing.T) {
	cache := New()

	ctx := t.Context()
	item := Item{Key: testKey, Value: "foobar"}

	require.NoError(t, cache.Set(ctx, item))
	require.NoError(t, cache.Set(ctx, item, item))
}

func TestCache_Set_redisErr(t *testing.T) {
	ctx := t.Context()
	wantErr := errors.New("test error")

	redisCache := &MoqRedisCache{
		SetFunc: func(ctx context.Context, maxItems int,
			items iter.Seq[cacheRedis.Item],
		) error {
			assert.Equal(t, 1, maxItems)
			return wantErr
		},
	}

	cache := New().WithRedisCache(redisCache)
	item := Item{Key: testKey, Value: "foobar"}
	require.ErrorIs(t, cache.Set(ctx, item), wantErr)
	assert.True(t, cache.Failed())
	assert.Len(t, redisCache.SetCalls(), 1)

	redisCache.SetFunc = func(ctx context.Context, maxItems int,
		items iter.Seq[cacheRedis.Item],
	) error {
		assert.Equal(t, 2, maxItems)
		return wantErr
	}

	cache = New().WithRedisCache(redisCache)
	require.ErrorIs(t, cache.Set(ctx, item, item), wantErr)
	assert.True(t, cache.Failed())
	assert.Len(t, redisCache.SetCalls(), 2)
}

func TestCache_Set_errorCanceled(t *testing.T) {
	localCache := &MoqLocalCache{}
	cache := New().WithLocalCache(localCache)
	assert.NotNil(t, cache)

	item := Item{
		Key:   testKey,
		Value: CacheableObject{Str: "mystring", Num: 42},
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, cache.Set(ctx, item, item), context.Canceled)
}

func TestCache_Set_errorWait(t *testing.T) {
	localCache := &MoqLocalCache{}
	cache := New().WithLocalCache(localCache)
	assert.NotNil(t, cache)

	wantErr := errors.New("expected error")
	item := Item{
		Key: testKey,
		Do: func(ctx context.Context) (any, error) {
			return nil, wantErr
		},
	}

	require.ErrorIs(t, cache.Set(t.Context(), item, item), wantErr)
}
