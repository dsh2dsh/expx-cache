package cache

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
)

func (self *CacheTestSuite) TestDelete() {
	ctx := context.Background()
	err := self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: self.CacheableValue(),
		TTL:   time.Hour,
	})
	self.Require().NoError(err)
	self.True(self.cache.Exists(ctx, testKey))
	self.cacheHit()

	self.Require().NoError(self.cache.Delete(ctx, testKey))
	self.False(valueNoError[bool](self.T())(self.cache.Get(ctx, testKey, nil)))
	self.cacheMiss()
	self.False(self.cache.Exists(ctx, testKey))
	self.cacheMiss()

	self.assertStats()
}

func (self *CacheTestSuite) TestDeleteFromLocalCache() {
	ctx := context.Background()
	err := self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: self.CacheableValue(),
		TTL:   time.Hour,
	})
	self.Require().NoError(err)

	self.cache.DeleteFromLocalCache(testKey)
	if self.cache.localCache != nil {
		self.Nil(self.cache.localCache.Get(testKey))
	}

	if self.cache.redis != nil {
		self.True(self.cache.Exists(ctx, testKey))
		self.cacheHitLocalMiss()
	} else {
		self.False(self.cache.Exists(ctx, testKey))
		self.cacheMiss()
	}

	self.assertStats()
}

func (self *CacheTestSuite) TestDeleteFromRedis() {
	ctx := context.Background()
	err := self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: self.CacheableValue(),
	})
	self.Require().NoError(err)

	self.Require().NoError(self.cache.DeleteFromRedis(ctx, testKey))
	if self.cache.localCache != nil {
		self.True(self.cache.Exists(ctx, testKey))
		self.cacheHit()
	} else {
		self.False(self.cache.Exists(ctx, testKey))
		self.cacheMiss()
	}

	if self.cache.redis != nil {
		self.Nil(valueNoError[[]byte](self.T())(self.cache.redis.Get(ctx, testKey)))
	}
	self.assertStats()
}

func TestDelete_withoutCache(t *testing.T) {
	cache := New()
	assert.NoError(t, cache.Delete(context.Background(), testKey))
}

func TestDeleteFromLocalCache_noCache(t *testing.T) {
	New().DeleteFromLocalCache(testKey)
}

func TestDelete_errFromRedis(t *testing.T) {
	redisClient := mocks.NewMockRedisClient(t)
	redisClient.EXPECT().Del(mock.Anything, mock.Anything).Return(io.EOF)
	cache := New().WithRedisCache(redisClient)
	assert.Error(t, cache.Delete(context.Background(), testKey))
}

func TestCache_Delete_withoutKeys(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.NoError(t, cache.Delete(context.Background()))
}

func TestCache_Delete_withKeyWrapper(t *testing.T) {
	ctx := context.Background()
	const keyPrefix = "baz:"
	wantKey := keyPrefix + testKey

	tests := []struct {
		name     string
		expecter func(t *testing.T, c *Cache) *Cache
	}{
		{
			name: "WithLocalCache",
			expecter: func(t *testing.T, c *Cache) *Cache {
				localCache := mocks.NewMockLocalCache(t)
				localCache.EXPECT().Del(wantKey)
				return c.WithLocalCache(localCache)
			},
		},
		{
			name: "WithRedisCache",
			expecter: func(t *testing.T, c *Cache) *Cache {
				redisCache := mocks.NewMockRedisClient(t)
				redisCache.EXPECT().Del(ctx, wantKey).Return(nil)
				return c.WithRedisCache(redisCache)
			},
		},
		{
			name: "with both",
			expecter: func(t *testing.T, c *Cache) *Cache {
				localCache := mocks.NewMockLocalCache(t)
				localCache.EXPECT().Del(wantKey)

				redisCache := mocks.NewMockRedisClient(t)
				redisCache.EXPECT().Del(ctx, mock.Anything).RunAndReturn(
					func(ctx context.Context, keys ...string) error {
						assert.Equal(t, []string{wantKey}, keys)
						return nil
					})

				return c.WithLocalCache(localCache).WithRedisCache(redisCache)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := New().
				WithKeyWrapper(func(key string) string {
					return keyPrefix + key
				})
			require.NotNil(t, cache)
			cache = tt.expecter(t, cache)
			assert.NoError(t, cache.Delete(ctx, testKey))
		})
	}
}
