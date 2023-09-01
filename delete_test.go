package cache

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	mocks "github.com/dsh2dsh/expx-cache/mocks/cache"
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
	self.False(valueNoError[bool](self.T())(self.cache.Get(ctx, testKey, nil)))
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
