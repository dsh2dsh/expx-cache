package cache

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGet_errRedisLocalCacheNil(t *testing.T) {
	cache := New()
	err := cache.Get(context.Background(), testKey, nil)
	assert.ErrorIs(t, err, errRedisLocalCacheNil)
}

func TestGet_redisErrAddsMiss(t *testing.T) {
	redisClient := NewMockRedisClient(t)
	redisClient.EXPECT().Get(mock.Anything, mock.Anything).Return(nil, io.EOF)

	cache := New().WithStats(true).WithRedisCache(redisClient)
	err := cache.Get(context.Background(), testKey, nil)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, uint64(1), cache.Stats().Misses)
}

func TestGetSkippingLocalCache(t *testing.T) {
	localCache := NewMockLocalCache(t)

	redisClient := NewMockRedisClient(t)
	redisClient.EXPECT().Get(mock.Anything, mock.Anything).Return(nil, nil)

	cache := New().WithLocalCache(localCache).WithRedisCache(redisClient)
	assert.ErrorIs(t,
		cache.GetSkippingLocalCache(context.Background(), testKey, nil),
		ErrCacheMiss)
}
