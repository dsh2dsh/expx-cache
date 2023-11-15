package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
)

func TestGet_withoutCache(t *testing.T) {
	cache := New()
	hit, err := cache.Get(context.Background(), testKey, nil)
	require.NoError(t, err)
	assert.False(t, hit)
}

func TestGet_redisErrAddsMiss(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	redisCache := mocks.NewMockRedisClient(t)
	redisCache.EXPECT().MGet(ctx, 1, mock.Anything).Return(nil, wantErr)

	cache := New().WithStats(true).WithRedisCache(redisCache)
	hit, err := cache.Get(ctx, testKey, nil)
	require.ErrorIs(t, err, wantErr)
	assert.False(t, hit)
	assert.Equal(t, uint64(1), cache.Stats().Misses)
}

func TestGetSkippingLocalCache(t *testing.T) {
	ctx := context.Background()

	localCache := mocks.NewMockLocalCache(t)
	redisCache := mocks.NewMockRedisClient(t)
	redisCache.EXPECT().MGet(ctx, 1, mock.Anything).RunAndReturn(
		func(
			ctx context.Context, maxItems int, keyIter func(itemIdx int) (key string),
		) (func() ([]byte, bool), error) {
			return makeBytesIter([][]byte{nil}), nil
		})

	cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
	hit := valueNoError[bool](t)(cache.GetSkippingLocalCache(ctx, testKey, nil))
	assert.False(t, hit)
}

func TestExists_withoutCache(t *testing.T) {
	cache := New()
	hit, err := cache.Exists(context.Background(), testKey)
	require.NoError(t, err)
	assert.False(t, hit)
}

func TestExists_withError(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	redisCache := mocks.NewMockRedisClient(t)
	redisCache.EXPECT().MGet(ctx, 1, mock.Anything).Return(nil, wantErr)

	cache := New().WithRedisCache(redisCache)
	hit, err := cache.Exists(ctx, testKey)
	require.ErrorIs(t, err, wantErr)
	assert.False(t, hit)
}

func TestCache_Multi(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	multi := cache.Multi()
	require.NotNil(t, multi)
	assert.Same(t, cache, multi.cache)
}
