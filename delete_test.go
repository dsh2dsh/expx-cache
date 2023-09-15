package cache

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
	if self.cache.localCache != nil {
		self.Nil(self.cache.localCache.Get(testKey))
	}

	if self.cache.redis != nil {
		self.True(self.cache.Exists(ctx, testKey))
	} else {
		self.False(self.cache.Exists(ctx, testKey))
	}
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
	} else {
		self.False(self.cache.Exists(ctx, testKey))
	}

	if self.cache.redis != nil {
		self.Nil(valueNoError[[]byte](self.T())(self.cache.redis.Get(ctx, testKey)))
	}
}

func (self *CacheTestSuite) TestDelete_manyKeys() {
	if testing.Short() {
		self.T().Skip("skipping in short mode")
	}

	const numKeys = 11
	ctx := context.Background()
	allKeys := make([]string, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%00d", i)
		allKeys = append(allKeys, key)
		err := self.cache.Set(&Item{
			Ctx:   ctx,
			Key:   key,
			Value: self.CacheableValue(),
		})
		self.Require().NoError(err)
	}
	self.Require().NoError(self.cache.Delete(ctx, allKeys...))
	for _, key := range allKeys {
		self.Require().False(self.cache.Exists(ctx, key))
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

func TestCache_Delete_withoutKeys(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.NoError(t, cache.Delete(context.Background()))
}
