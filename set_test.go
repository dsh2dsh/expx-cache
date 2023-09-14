package cache

import (
	"context"
	"io"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	cacheMocks "github.com/dsh2dsh/expx-cache/mocks/cache"
	redisMocks "github.com/dsh2dsh/expx-cache/mocks/redis"
)

func (self *CacheTestSuite) TestSet_canBeUsedWithIncr() {
	if self.rdb == nil {
		self.T().Skip("requires Redis connection")
	}

	ctx := context.Background()
	value := "123"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: value,
	}))

	n := valueNoError[int64](self.T())(self.rdb.Incr(ctx, testKey).Result())
	self.Equal(int64(124), n)
}

func TestCache_Set_Marshall_error(t *testing.T) {
	localCache := cacheMocks.NewMockLocalCache(t)
	cache := New().WithLocalCache(localCache).
		WithMarshal(func(value any) ([]byte, error) {
			return nil, io.EOF
		})

	err := cache.Set(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: "foobar",
	})
	assert.ErrorIs(t, err, io.EOF)
}

func TestCache_Set_withoutCache(t *testing.T) {
	cache := New()
	assert.NoError(t, cache.Set(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: "foobar",
	}))
}

func TestCache_Set_redisErr(t *testing.T) {
	rdb := redisMocks.NewMockCmdable(t)
	rdb.EXPECT().Set(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redis.NewStatusResult("", io.EOF))

	cache := New().WithRedis(rdb)

	err := cache.Set(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: "foobar",
	})
	assert.ErrorIs(t, err, io.EOF)
}
