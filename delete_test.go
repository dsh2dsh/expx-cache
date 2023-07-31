package cache

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

	self.Require().NoError(self.cache.Delete(ctx, testKey))
	self.ErrorIs(self.cache.Get(ctx, testKey, nil), ErrCacheMiss)
	self.False(self.cache.Exists(ctx, testKey))
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
	if self.cache.redis != nil {
		self.True(self.cache.Exists(ctx, testKey))
	} else {
		self.False(self.cache.Exists(ctx, testKey))
	}
}

func TestDelete_errRedisLocalCacheNil(t *testing.T) {
	cache := New()
	err := cache.Delete(context.Background(), testKey)
	assert.ErrorIs(t, err, errRedisLocalCacheNil)
}

func TestDeleteFromLocalCache_noCache(t *testing.T) {
	New().DeleteFromLocalCache(testKey)
}

func TestDelete_errFromRedis(t *testing.T) {
	redisClient := NewMockRedisClient(t)
	redisClient.EXPECT().Del(mock.Anything, mock.Anything).Return(io.EOF)
	cache := New().WithRedisCache(redisClient)
	assert.Error(t, cache.Delete(context.Background(), testKey))
}
