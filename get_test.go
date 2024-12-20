package cache

import (
	"context"
	"errors"
	"iter"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cacheMocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
	redisMocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
	cacheRedis "github.com/dsh2dsh/expx-cache/redis"
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
	redisCache.EXPECT().Get(ctx, 1, mock.Anything).
		Return(makeBytesIter(nil, wantErr))

	cache := New().WithRedisCache(redisCache)
	item := Item{Key: testKey}
	missed, err := cache.Get(ctx, item)
	require.ErrorIs(t, err, wantErr)
	assert.True(t, cache.Failed())
	assert.Nil(t, missed)
	assert.Equal(t, uint64(1), cache.Stats().Misses)
}

func TestCache_Get_SkipLocalCache(t *testing.T) {
	ctx := context.Background()

	localCache := cacheMocks.NewMockLocalCache(t)
	redisCache := cacheMocks.NewMockRedisCache(t)
	redisCache.EXPECT().Get(ctx, 1, mock.Anything).
		Return(makeBytesIter([][]byte{nil}, nil))

	cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
	item := Item{Key: testKey, SkipLocalCache: true}
	missed := valueNoError[[]Item](t)(cache.Get(ctx, item))
	assert.Equal(t, []Item{item}, missed)

	redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).
		Return(makeBytesIter([][]byte{nil, nil}, nil))
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
	redisCache.EXPECT().Get(ctx, 1, mock.Anything).
		Return(makeBytesIter(nil, wantErr))

	cache := New().WithRedisCache(redisCache)
	hit, err := cache.Exists(ctx, testKey)
	require.ErrorIs(t, err, wantErr)
	assert.True(t, cache.Failed())
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

	sig <- struct{}{}
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
	assert.True(t, cache.Failed())
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
	redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).
		Return(makeBytesIter([][]byte{blob, blob}, nil))
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

	sig <- struct{}{}
	require.ErrorIs(t, cache.GetSet(ctx, item, item), context.Canceled)
}

func TestCache_Get_errorRedisCanceled(t *testing.T) {
	t.Parallel()

	obj := CacheableObject{Str: "mystring", Num: 42}
	blob := valueNoError[[]byte](t)(marshal(&obj))

	redisCache := cacheMocks.NewMockRedisCache(t)
	redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).
		Return(makeBytesIter([][]byte{blob, blob}, nil))

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
	sig <- struct{}{}
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
	redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).
		Return(makeBytesIter([][]byte{blob, blob}, nil))

	cache := New().WithRedisCache(redisCache)
	assert.NotNil(t, cache)

	got := CacheableObject{}
	item := Item{Key: testKey, Value: &got}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := cache.Get(ctx, item, item)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCache_GetSet_withErrRedisCache(t *testing.T) {
	const testKey0, testKey1 = testKey + "0", testKey + "1"
	const foobar, foobaz = "foobar", "foobaz"

	ctx := context.Background()
	testErr := errors.New("test error")
	values1 := []string{foobar}
	values2 := []string{foobar, foobaz}

	tests := []struct {
		name   string
		cache  func() *Cache
		failed bool
		values []string
	}{
		{
			name: "with Err set",
			cache: func() *Cache {
				redisCache := cacheMocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				_ = cache.redisCacheError(testErr)
				return cache
			},
			failed: true,
			values: values1,
		},
		{
			name: "with Err set: 2 items",
			cache: func() *Cache {
				redisCache := cacheMocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				_ = cache.redisCacheError(testErr)
				return cache
			},
			failed: true,
			values: values2,
		},
		{
			name: "Get ErrRedisCache",
			cache: func() *Cache {
				redisCache := cacheMocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().Get(ctx, 1, mock.Anything).
					Return(makeBytesIter(nil, testErr))
				return cache
			},
			failed: true,
			values: values1,
		},
		{
			name: "Get ErrRedisCache: 2 items",
			cache: func() *Cache {
				redisCache := cacheMocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).
					Return(makeBytesIter(nil, testErr))
				return cache
			},
			failed: true,
			values: values2,
		},
		{
			name: "set ErrRedisCache",
			cache: func() *Cache {
				localCache := cacheMocks.NewMockLocalCache(t)
				localCache.EXPECT().Get(testKey0).Return(nil)
				localCache.EXPECT().Set(testKey0, []byte(foobar))
				redisCache := cacheMocks.NewMockRedisCache(t)
				cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
				redisCache.EXPECT().Get(ctx, 1, mock.Anything).
					Return(makeBytesIter([][]byte{nil}, nil))
				redisCache.EXPECT().Set(ctx, 1, mock.Anything).Return(testErr)
				return cache
			},
			failed: true,
			values: values1,
		},
		{
			name: "set ErrRedisCache: 2 items",
			cache: func() *Cache {
				localCache := cacheMocks.NewMockLocalCache(t)
				localCache.EXPECT().Get(testKey0).Return(nil)
				localCache.EXPECT().Get(testKey1).Return(nil)
				localCache.EXPECT().Set(testKey0, []byte(foobar))
				localCache.EXPECT().Set(testKey1, []byte(foobaz))
				redisCache := cacheMocks.NewMockRedisCache(t)
				cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
				redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).
					Return(makeBytesIter([][]byte{nil, nil}, nil))
				redisCache.EXPECT().Set(ctx, 2, mock.Anything).Return(testErr)
				return cache
			},
			failed: true,
			values: values2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			items := make([]Item, len(tt.values))
			got := make([]string, len(tt.values))
			for i, v := range tt.values {
				items[i] = Item{
					Key:   testKey + strconv.Itoa(i),
					Value: &got[i],
					Do: func(ctx context.Context) (any, error) {
						return v, nil
					},
				}
			}
			cache := tt.cache()
			err := cache.GetSet(ctx, items...)
			require.NoError(t, err)
			assert.Equal(t, tt.failed, cache.Failed())
			assert.Equal(t, tt.values, got)
		})
	}
}

func TestCache_GetSet_itemDoNil(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name  string
		cache func() *Cache
		items int
	}{
		{
			name: "1 item",
			cache: func() *Cache {
				redisCache := cacheMocks.NewMockRedisCache(t)
				redisCache.EXPECT().Get(ctx, 1, mock.Anything).
					Return(makeBytesIter([][]byte{nil}, nil))
				return New().WithRedisCache(redisCache)
			},
			items: 1,
		},
		{
			name: "2 items",
			cache: func() *Cache {
				redisCache := cacheMocks.NewMockRedisCache(t)
				redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).
					Return(makeBytesIter([][]byte{nil, nil}, nil))
				redisCache.EXPECT().Set(ctx, 2, mock.Anything).RunAndReturn(
					func(ctx context.Context, maxItems int,
						items iter.Seq[cacheRedis.Item],
					) error {
						pipe := redisMocks.NewMockPipeliner(t)
						pipe.EXPECT().Len().Return(0)
						rdb := redisMocks.NewMockCmdable(t)
						rdb.EXPECT().Pipeline().Return(pipe)
						redisCache := cacheRedis.New(rdb)
						return redisCache.Set(ctx, maxItems, items)
					})
				return New().WithRedisCache(redisCache)
			},
			items: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.cache()
			items := make([]Item, tt.items)
			want := make([]string, tt.items)
			got := make([]string, tt.items)
			for i := 0; i < tt.items; i++ {
				items[i] = Item{
					Key:   testKey + strconv.Itoa(i),
					Value: &got[i],
					Do: func(ctx context.Context) (any, error) {
						return nil, nil
					},
				}
			}
			require.NoError(t, cache.GetSet(ctx, items...))
			assert.Equal(t, want, got)
		})
	}
}

func TestCache_GetSet_marshalErr(t *testing.T) {
	redisCache := cacheMocks.NewMockRedisCache(t)
	redisCache.EXPECT().Get(mock.Anything, 2, mock.Anything).
		Return(makeBytesIter([][]byte{nil, nil}, nil))
	cache := New().WithRedisCache(redisCache)

	ctx := context.Background()
	testErr := errors.New("test error")
	err := cache.GetSet(ctx, Item{
		Key: testKey + "0",
		Do:  func(ctx context.Context) (any, error) { return nil, testErr },
	}, Item{
		Key: testKey + "1",
		Do:  func(ctx context.Context) (any, error) { return nil, testErr },
	})
	require.ErrorIs(t, err, testErr)
}
