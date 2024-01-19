package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cacheMocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
	redisMocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func TestCache_Get_withoutCache(t *testing.T) {
	cache := New()
	item := Item{Key: testKey}
	missed, err := cache.Get(context.Background(), item)
	require.NoError(t, err)
	assert.Equal(t, []Item{item}, missed)
}

func TestGet_redisErrAddsMiss(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	redisCache := cacheMocks.NewMockRedisCache(t)
	redisCache.EXPECT().Get(ctx, 1, mock.Anything).Return(nil, wantErr)

	cache := New().WithStats(true).WithRedisCache(redisCache)
	item := Item{Key: testKey}
	missed, err := cache.Get(ctx, item)
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, ErrRedisCache)
	assert.Nil(t, missed)
	assert.Equal(t, uint64(1), cache.Stats().Misses)
}

func TestCache_Get_SkipLocalCache(t *testing.T) {
	ctx := context.Background()

	localCache := cacheMocks.NewMockLocalCache(t)
	redisCache := cacheMocks.NewMockRedisCache(t)
	redisCache.EXPECT().Get(ctx, 1, mock.Anything).Return(
		makeBytesIter([][]byte{nil}), nil)

	cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
	item := Item{Key: testKey, SkipLocalCache: true}
	missed := valueNoError[[]Item](t)(cache.Get(ctx, item))
	assert.Equal(t, []Item{item}, missed)

	redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).Return(
		makeBytesIter([][]byte{nil, nil}), nil)
	missed = valueNoError[[]Item](t)(cache.Get(ctx, item, item))
	assert.Equal(t, []Item{item, item}, missed)
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

	redisCache := cacheMocks.NewMockRedisCache(t)
	redisCache.EXPECT().Get(ctx, 1, mock.Anything).Return(nil, wantErr)

	cache := New().WithRedisCache(redisCache)
	hit, err := cache.Exists(ctx, testKey)
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, ErrRedisCache)
	assert.False(t, hit)
}

func TestCache_Get_errorCanceled(t *testing.T) {
	t.Parallel()
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	obj := CacheableObject{Str: "mystring", Num: 42}
	ctx, cancel := context.WithCancel(context.Background())
	item := Item{Key: testKey, Value: &obj}
	require.NoError(t, cache.Set(ctx, item))

	for cache.marshalers.TryAcquire(1) {
	}
	sig := make(chan struct{})
	go func() {
		<-sig
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	close(sig)
	missed, err := cache.Get(ctx, item, item)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, missed)
}

func TestCache_Get_errorWait(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	ctx := context.Background()
	item := Item{
		Key:   testKey,
		Value: CacheableObject{Str: "mystring", Num: 42},
	}
	require.NoError(t, cache.Set(ctx, item))

	var got bool
	item.Value = &got
	missed, err := cache.Get(ctx, item, item)
	require.Error(t, err)
	assert.Nil(t, missed)
}

func TestCache_Get_errorRedis(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	rdb := redisMocks.NewMockCmdable(t)
	pipe := redisMocks.NewMockPipeliner(t)
	var cmds []redis.Cmder
	pipe.EXPECT().Get(mock.Anything, testKey).RunAndReturn(
		func(ctx context.Context, key string) *redis.StringCmd {
			cmd := redis.NewStringResult("", nil)
			cmds = append(cmds, cmd)
			return cmd
		})
	pipe.EXPECT().Len().RunAndReturn(func() int { return len(cmds) })
	pipe.EXPECT().Exec(mock.Anything).Return(
		[]redis.Cmder{
			redis.NewStringResult("", wantErr),
			redis.NewStringResult("", wantErr),
		}, wantErr,
	)
	rdb.EXPECT().Pipeline().Return(pipe)

	cache := New().WithRedis(rdb)
	assert.NotNil(t, cache)

	obj := CacheableObject{}
	item := Item{Key: testKey, Value: &obj}

	missed, err := cache.Get(ctx, item, item)
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, err, ErrRedisCache)
	assert.Nil(t, missed)
}

func TestCache_Get_localSet(t *testing.T) {
	localCache := cacheMocks.NewMockLocalCache(t)
	redisCache := cacheMocks.NewMockRedisCache(t)

	cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
	assert.NotNil(t, cache)

	ctx := context.Background()
	obj := CacheableObject{}
	item := Item{Key: testKey, Value: &obj}
	blob := valueNoError[[]byte](t)(cache.Marshal(&obj))

	localCache.EXPECT().Get(item.Key).Return(nil)
	redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).Return(
		makeBytesIter([][]byte{blob, blob}), nil)
	localCache.EXPECT().Set(item.Key, blob)

	missed := valueNoError[[]Item](t)(cache.Get(ctx, item, item))
	assert.Empty(t, missed)
}

func TestCache_GetSet_errorGetCanceled(t *testing.T) {
	t.Parallel()
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	obj := CacheableObject{Str: "mystring", Num: 42}
	item := Item{Key: testKey, Value: &obj}
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, cache.Set(ctx, item))

	for cache.marshalers.TryAcquire(1) {
	}
	sig := make(chan struct{})
	go func() {
		<-sig
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	close(sig)
	require.ErrorIs(t, cache.GetSet(ctx, item, item), context.Canceled)
}

func TestCache_Get_errorRedisCanceled(t *testing.T) {
	t.Parallel()

	obj := CacheableObject{Str: "mystring", Num: 42}
	blob := valueNoError[[]byte](t)(marshal(&obj))

	redisCache := cacheMocks.NewMockRedisCache(t)
	redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).Return(
		makeBytesIter([][]byte{blob, blob}), nil)

	cache := New().WithRedisCache(redisCache)
	assert.NotNil(t, cache)

	ctx, cancel := context.WithCancel(context.Background())
	for cache.marshalers.TryAcquire(1) {
	}

	sig := make(chan struct{})
	go func() {
		<-sig
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	got := CacheableObject{}
	item := Item{Key: testKey, Value: &got}
	close(sig)
	missed, err := cache.Get(ctx, item, item)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, missed)
}

func TestCache_Get_localGetItemsCanceled(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	obj := CacheableObject{}
	item := Item{Key: testKey, Value: &obj}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := cache.Get(ctx, item, item)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCache_Get_redisGetItemsCanceled(t *testing.T) {
	obj := CacheableObject{Str: "mystring", Num: 42}
	blob := valueNoError[[]byte](t)(marshal(&obj))

	redisCache := cacheMocks.NewMockRedisCache(t)
	redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).Return(
		makeBytesIter([][]byte{blob, blob}), nil)

	cache := New().WithRedisCache(redisCache)
	assert.NotNil(t, cache)

	got := CacheableObject{}
	item := Item{Key: testKey, Value: &got}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := cache.Get(ctx, item, item)
	require.ErrorIs(t, err, context.Canceled)
}
