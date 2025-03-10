package cache

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
)

func (self *CacheTestSuite) TestCache_Delete() {
	ctx := context.Background()
	item := Item{Key: testKey, Value: self.CacheableValue()}
	self.Require().NoError(self.cache.Set(ctx, item))
	self.True(self.cache.Exists(ctx, item.Key))
	self.expectCacheHit()

	self.Require().NoError(self.cache.Delete(ctx, item.Key))
	self.Equal([]Item{item},
		valueNoError[[]Item](self.T())(self.cache.Get(ctx, item)))
	self.expectCacheMiss()
	self.False(self.cache.Exists(ctx, testKey))
	self.expectCacheMiss()

	self.assertStats()
}

func (self *CacheTestSuite) TestDeleteFromLocalCache() {
	ctx := context.Background()
	err := self.cache.Set(ctx, Item{
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
		self.expectCacheHitLocalMiss()
	} else {
		self.False(self.cache.Exists(ctx, testKey))
		self.expectCacheMiss()
	}

	self.assertStats()
}

func (self *CacheTestSuite) TestDeleteFromRedis() {
	ctx := context.Background()
	err := self.cache.Set(ctx, Item{
		Key:   testKey,
		Value: self.CacheableValue(),
	})
	self.Require().NoError(err)

	self.Require().NoError(self.cache.DeleteFromRedis(ctx, testKey))
	if self.cache.localCache != nil {
		self.True(self.cache.Exists(ctx, testKey))
		self.expectCacheHit()
	} else {
		self.False(self.cache.Exists(ctx, testKey))
		self.expectCacheMiss()
	}

	if self.cache.redis != nil {
		bytesIter := self.cache.redis.Get(context.Background(), 1,
			slices.Values([]string{testKey}))
		for b, err := range bytesIter {
			self.Require().NoError(err)
			self.Nil(b)
		}
	}
	self.assertStats()
}

func TestDelete_withoutCache(t *testing.T) {
	cache := New()
	require.NoError(t, cache.Delete(t.Context(), testKey))
}

func TestDeleteFromLocalCache_noCache(t *testing.T) {
	New().DeleteFromLocalCache(testKey)
}

func TestDelete_errFromRedis(t *testing.T) {
	wantErr := errors.New("test error")
	ctx := t.Context()

	redisClient := mocks.NewMockRedisCache(t)
	redisClient.EXPECT().Del(ctx, []string{testKey}).Return(wantErr)
	cache := New().WithRedisCache(redisClient)

	err := cache.Delete(ctx, testKey)
	require.ErrorIs(t, err, wantErr)
	assert.True(t, cache.Failed())
}

func TestCache_Delete_withoutKeys(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	require.NoError(t, cache.Delete(t.Context()))
}

func TestCache_Delete_withKeyWrapper(t *testing.T) {
	ctx := t.Context()
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
				redisCache := mocks.NewMockRedisCache(t)
				redisCache.EXPECT().Del(ctx, []string{wantKey}).Return(nil)
				return c.WithRedisCache(redisCache)
			},
		},
		{
			name: "with both",
			expecter: func(t *testing.T, c *Cache) *Cache {
				localCache := mocks.NewMockLocalCache(t)
				localCache.EXPECT().Del(wantKey)

				redisCache := mocks.NewMockRedisCache(t)
				redisCache.EXPECT().Del(ctx, []string{wantKey}).Return(nil)

				return c.WithLocalCache(localCache).WithRedisCache(redisCache)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := New().WithKeyWrapper(func(key string) string {
				return keyPrefix + key
			})
			require.NotNil(t, cache)
			cache = tt.expecter(t, cache)
			require.NoError(t, cache.Delete(ctx, testKey))
		})
	}
}
