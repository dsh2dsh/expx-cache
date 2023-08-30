package cache

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	mocks "github.com/dsh2dsh/expx-cache/mocks/cache"
)

func TestGet_errRedisLocalCacheNil(t *testing.T) {
	cache := New()
	hit, err := cache.Get(context.Background(), testKey, nil)
	assert.ErrorIs(t, err, errRedisLocalCacheNil)
	assert.False(t, hit)
}

func TestGet_redisErrAddsMiss(t *testing.T) {
	redisClient := mocks.NewMockRedisClient(t)
	redisClient.EXPECT().Get(mock.Anything, mock.Anything).Return(nil, io.EOF)

	cache := New().WithStats(true).WithRedisCache(redisClient)
	hit, err := cache.Get(context.Background(), testKey, nil)
	assert.ErrorIs(t, err, io.EOF)
	assert.False(t, hit)
	assert.Equal(t, uint64(1), cache.Stats().Misses)
}

func TestGetSkippingLocalCache(t *testing.T) {
	localCache := mocks.NewMockLocalCache(t)

	redisClient := mocks.NewMockRedisClient(t)
	redisClient.EXPECT().Get(mock.Anything, mock.Anything).Return(nil, nil)

	cache := New().WithLocalCache(localCache).WithRedisCache(redisClient)
	hit := valueNoError[bool](t)(
		cache.GetSkippingLocalCache(context.Background(), testKey, nil))
	assert.False(t, hit)
}

func TestExists_errRedisLocalCacheNil(t *testing.T) {
	cache := New()
	hit, err := cache.Exists(context.Background(), testKey)
	assert.ErrorIs(t, err, errRedisLocalCacheNil)
	assert.False(t, hit)
}
