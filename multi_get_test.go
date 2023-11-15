package cache

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cacheMocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
	redisMocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func TestMultiCache_Get_errorCanceled(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	obj := CacheableObject{
		Str: "mystring",
		Num: 42,
	}

	ctx, cancel := context.WithCancel(context.Background())
	item := Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: &obj,
	}
	require.NoError(t, cache.Set(&item))

	m := NewMultiCache(cache)
	assert.NotNil(t, m)

	cancel()
	missed, err := m.Get(ctx, []*Item{&item})
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, missed)
}

func TestMultiCache_Get_errorWait(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	ctx := context.Background()
	item := Item{
		Ctx: ctx,
		Key: testKey,
		Value: CacheableObject{
			Str: "mystring",
			Num: 42,
		},
	}
	require.NoError(t, cache.Set(&item))

	m := NewMultiCache(cache)
	assert.NotNil(t, m)

	var got bool
	item.Value = &got
	missed, err := m.Get(ctx, []*Item{&item})
	require.Error(t, err)
	assert.Nil(t, missed)
}

func TestMultiCache_Get_errorRedis(t *testing.T) {
	ctx := context.Background()

	pipe := redisMocks.NewMockPipeliner(t)
	pipe.EXPECT().Get(ctx, testKey).Return(redis.NewStringResult("", io.EOF))

	rdb := redisMocks.NewMockCmdable(t)
	rdb.EXPECT().Pipeline().Return(pipe)

	cache := New().WithRedis(rdb)
	assert.NotNil(t, cache)

	m := NewMultiCache(cache)
	assert.NotNil(t, m)

	obj := CacheableObject{}
	item := Item{
		Key:   testKey,
		Value: &obj,
	}

	missed, err := m.Get(ctx, []*Item{&item})
	require.ErrorIs(t, err, io.EOF)
	assert.Nil(t, missed)
}

func TestMultiCache_Get_localSet(t *testing.T) {
	localCache := cacheMocks.NewMockLocalCache(t)
	redisCache := cacheMocks.NewMockRedisCache(t)

	cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
	assert.NotNil(t, cache)

	ctx := context.Background()
	ttl := time.Minute
	obj := CacheableObject{}
	item := Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          &obj,
		TTL:            ttl,
		SkipLocalCache: true,
	}

	var blob []byte
	redisCache.EXPECT().MSet(ctx, 1, mock.Anything).RunAndReturn(
		func(ctx context.Context, maxItems int,
			iter func(itemIdx int) (key string, b []byte, ttl time.Duration),
		) error {
			for i := 0; i < maxItems; i++ {
				_, blob, _ = iter(i)
			}
			return nil
		},
	)
	require.NoError(t, cache.Set(&item))

	item.SkipLocalCache = false
	localCache.EXPECT().Get(testKey).Return(nil)
	redisCache.EXPECT().Get(ctx, 1, mock.Anything).RunAndReturn(
		func(
			ctx context.Context, maxItems int, keyIter func(int) string,
		) (func() ([]byte, bool), error) {
			return makeBytesIter([][]byte{blob}), nil
		})
	localCache.EXPECT().Set(testKey, blob)
	missed := valueNoError[[]*Item](t)(cache.MGet(ctx, []*Item{&item}))
	assert.Nil(t, missed)
}
