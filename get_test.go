package cache

import (
	"context"
	"errors"
	"iter"
	"strconv"
	"testing"
	"time"

	cacheRedis "github.com/dsh2dsh/expx-cache-redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_Get_withoutCache(t *testing.T) {
	cache := New()
	item := Item{Key: testKey}
	missed, err := cache.Get(t.Context(), item)
	require.NoError(t, err)
	assert.Equal(t, []Item{item}, missed)
}

func TestGet_redisErrAddsMiss(t *testing.T) {
	ctx := t.Context()
	wantErr := errors.New("test error")

	redisCache := &MoqRedisCache{
		GetFunc: func(ctx context.Context, maxItems int, keys iter.Seq[string],
		) iter.Seq2[[]byte, error] {
			assert.Equal(t, 1, maxItems)
			return makeBytesIter(nil, wantErr)
		},
	}

	cache := New().WithRedisCache(redisCache)
	item := Item{Key: testKey}
	missed, err := cache.Get(ctx, item)
	require.ErrorIs(t, err, wantErr)
	assert.True(t, cache.Failed())
	assert.Nil(t, missed)
	assert.Equal(t, uint64(1), cache.Stats().Misses)
}

func TestCache_Get_SkipLocalCache(t *testing.T) {
	ctx := t.Context()

	localCache := &MoqLocalCache{}
	redisCache := &MoqRedisCache{
		GetFunc: func(ctx context.Context, maxItems int, keys iter.Seq[string],
		) iter.Seq2[[]byte, error] {
			assert.Equal(t, 1, maxItems)
			return makeBytesIter([][]byte{nil}, nil)
		},
	}

	cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
	item := Item{Key: testKey, SkipLocalCache: true}
	missed := mustValue[[]Item](t)(cache.Get(ctx, item))
	assert.Equal(t, []Item{item}, missed)
	assert.Len(t, redisCache.GetCalls(), 1)

	redisCache.GetFunc = func(ctx context.Context, maxItems int,
		keys iter.Seq[string],
	) iter.Seq2[[]byte, error] {
		assert.Equal(t, 2, maxItems)
		return makeBytesIter([][]byte{nil, nil}, nil)
	}
	missed = mustValue[[]Item](t)(cache.Get(ctx, item, item))
	assert.Equal(t, []Item{item, item}, missed)
	assert.Len(t, redisCache.GetCalls(), 2)
}

func TestExists_withoutCache(t *testing.T) {
	cache := New()
	hit, err := cache.Exists(t.Context(), testKey)
	require.NoError(t, err)
	assert.False(t, hit)
}

func TestExists_withError(t *testing.T) {
	ctx := t.Context()
	wantErr := errors.New("test error")

	redisCache := &MoqRedisCache{
		GetFunc: func(ctx context.Context, maxItems int, keys iter.Seq[string],
		) iter.Seq2[[]byte, error] {
			assert.Equal(t, 1, maxItems)
			return makeBytesIter(nil, wantErr)
		},
	}

	cache := New().WithRedisCache(redisCache)
	hit, err := cache.Exists(ctx, testKey)
	require.ErrorIs(t, err, wantErr)
	assert.Len(t, redisCache.GetCalls(), 1)
	assert.True(t, cache.Failed())
	assert.False(t, hit)
}

func TestCache_Get_errorCanceled(t *testing.T) {
	t.Parallel()
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	obj := CacheableObject{Str: "mystring", Num: 42}
	ctx, cancel := context.WithCancel(t.Context())
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

	ctx := t.Context()
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
	ctx := t.Context()
	wantErr := errors.New("test error")

	redisCache := &MoqRedisCache{
		GetFunc: func(ctx context.Context, maxItems int, keys iter.Seq[string],
		) iter.Seq2[[]byte, error] {
			assert.Equal(t, 2, maxItems)
			return makeBytesIter([][]byte{nil, nil}, wantErr)
		},
	}

	cache := New().WithRedisCache(redisCache)
	assert.NotNil(t, cache)

	obj := CacheableObject{}
	item := Item{Key: testKey, Value: &obj}

	missed, err := cache.Get(ctx, item, item)
	require.ErrorIs(t, err, wantErr)
	assert.True(t, cache.Failed())
	assert.Nil(t, missed)
}

func TestCache_Get_localSet(t *testing.T) {
	obj := CacheableObject{}
	item := Item{Key: testKey, Value: &obj}

	cache := New()
	assert.NotNil(t, cache)
	blob := mustValue[[]byte](t)(cache.Marshal(&obj))

	localCache := &MoqLocalCache{
		GetFunc: func(key string) []byte {
			assert.Equal(t, item.Key, key)
			return nil
		},
		SetFunc: func(key string, data []byte) {
			assert.Equal(t, item.Key, key)
			assert.Equal(t, blob, data)
		},
	}
	cache.WithLocalCache(localCache)

	redisCache := &MoqRedisCache{
		GetFunc: func(ctx context.Context, maxItems int, keys iter.Seq[string],
		) iter.Seq2[[]byte, error] {
			assert.Equal(t, 2, maxItems)
			return makeBytesIter([][]byte{blob, blob}, nil)
		},
	}
	cache.WithRedisCache(redisCache)

	missed := mustValue[[]Item](t)(cache.Get(t.Context(), item, item))
	assert.Empty(t, missed)
	assert.Len(t, localCache.GetCalls(), 2)
	assert.Len(t, redisCache.GetCalls(), 1)
	assert.Len(t, localCache.SetCalls(), 2)
}

func TestCache_GetSet_errorGetCanceled(t *testing.T) {
	t.Parallel()
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	obj := CacheableObject{Str: "mystring", Num: 42}
	item := Item{Key: testKey, Value: &obj}
	ctx, cancel := context.WithCancel(t.Context())
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
	blob := mustValue[[]byte](t)(marshal(&obj))

	redisCache := &MoqRedisCache{
		GetFunc: func(ctx context.Context, maxItems int, keys iter.Seq[string],
		) iter.Seq2[[]byte, error] {
			assert.Equal(t, 2, maxItems)
			return makeBytesIter([][]byte{blob, blob}, nil)
		},
	}

	cache := New().WithRedisCache(redisCache)
	assert.NotNil(t, cache)

	ctx, cancel := context.WithCancel(t.Context())
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
	assert.Len(t, redisCache.GetCalls(), 1)
}

func TestCache_Get_localGetItemsCanceled(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	obj := CacheableObject{}
	item := Item{Key: testKey, Value: &obj}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := cache.Get(ctx, item, item)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCache_Get_redisGetItemsCanceled(t *testing.T) {
	obj := CacheableObject{Str: "mystring", Num: 42}
	blob := mustValue[[]byte](t)(marshal(&obj))

	redisCache := &MoqRedisCache{
		GetFunc: func(ctx context.Context, maxItems int, keys iter.Seq[string],
		) iter.Seq2[[]byte, error] {
			assert.Equal(t, 2, maxItems)
			return makeBytesIter([][]byte{blob, blob}, nil)
		},
	}

	cache := New().WithRedisCache(redisCache)
	assert.NotNil(t, cache)

	got := CacheableObject{}
	item := Item{Key: testKey, Value: &got}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := cache.Get(ctx, item, item)
	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, redisCache.GetCalls(), 1)
}

func TestCache_GetSet_withErrRedisCache(t *testing.T) {
	const testKey0, testKey1 = testKey + "0", testKey + "1"
	const foobar, foobaz = "foobar", "foobaz"

	testErr := errors.New("test error")
	values1 := []string{foobar}
	values2 := []string{foobar, foobaz}

	tests := []struct {
		name      string
		configure func(t *testing.T, c *Cache) func()
		cache     func() *Cache
		failed    bool
		values    []string
	}{
		{
			name: "with Err set",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{}
				_ = c.WithRedisCache(redisCache).redisCacheError(testErr)
				return nil
			},
			failed: true,
			values: values1,
		},
		{
			name: "with Err set: 2 items",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{}
				_ = c.WithRedisCache(redisCache).redisCacheError(testErr)
				return nil
			},
			failed: true,
			values: values2,
		},
		{
			name: "Get ErrRedisCache",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					GetFunc: func(ctx context.Context, maxItems int,
						keys iter.Seq[string],
					) iter.Seq2[[]byte, error] {
						assert.Equal(t, 1, maxItems)
						return makeBytesIter(nil, testErr)
					},
				}
				c.WithRedisCache(redisCache)
				return func() { assert.Len(t, redisCache.GetCalls(), 1) }
			},
			failed: true,
			values: values1,
		},
		{
			name: "Get ErrRedisCache: 2 items",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					GetFunc: func(ctx context.Context, maxItems int,
						keys iter.Seq[string],
					) iter.Seq2[[]byte, error] {
						assert.Equal(t, 2, maxItems)
						return makeBytesIter(nil, testErr)
					},
				}
				c.WithRedisCache(redisCache)
				return func() { assert.Len(t, redisCache.GetCalls(), 1) }
			},
			failed: true,
			values: values2,
		},
		{
			name: "set ErrRedisCache",
			configure: func(t *testing.T, c *Cache) func() {
				localCache := &MoqLocalCache{
					GetFunc: func(key string) []byte {
						assert.Equal(t, testKey0, key)
						return nil
					},
					SetFunc: func(key string, data []byte) {
						assert.Equal(t, testKey0, key)
						assert.Equal(t, []byte(foobar), data)
					},
				}
				c.WithLocalCache(localCache)

				redisCache := &MoqRedisCache{
					GetFunc: func(ctx context.Context, maxItems int,
						keys iter.Seq[string],
					) iter.Seq2[[]byte, error] {
						assert.Equal(t, 1, maxItems)
						return makeBytesIter([][]byte{nil}, nil)
					},
					SetFunc: func(ctx context.Context, maxItems int,
						items iter.Seq[cacheRedis.Item],
					) error {
						assert.Equal(t, 1, maxItems)
						return testErr
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Len(t, localCache.GetCalls(), 1)
					assert.Len(t, redisCache.GetCalls(), 1)
					assert.Len(t, localCache.SetCalls(), 1)
					assert.Len(t, redisCache.SetCalls(), 1)
				}
			},
			failed: true,
			values: values1,
		},
		{
			name: "set ErrRedisCache: 2 items",
			configure: func(t *testing.T, c *Cache) func() {
				localCache := &MoqLocalCache{
					GetFunc: func(key string) []byte { return nil },
					SetFunc: func(key string, data []byte) {},
				}
				c.WithLocalCache(localCache)

				redisCache := &MoqRedisCache{
					GetFunc: func(ctx context.Context, maxItems int,
						keys iter.Seq[string],
					) iter.Seq2[[]byte, error] {
						assert.Equal(t, 2, maxItems)
						return makeBytesIter([][]byte{nil, nil}, nil)
					},
					SetFunc: func(ctx context.Context, maxItems int,
						items iter.Seq[cacheRedis.Item],
					) error {
						assert.Equal(t, 2, maxItems)
						return testErr
					},
				}
				c.WithRedisCache(redisCache)

				return func() {
					assert.Equal(t, []struct {
						Key string
					}{{testKey0}, {testKey1}}, localCache.GetCalls())

					assert.Len(t, redisCache.GetCalls(), 1)

					assert.Equal(t, []struct {
						Key  string
						Data []byte
					}{
						{testKey0, []byte(foobar)},
						{testKey1, []byte(foobaz)},
					}, localCache.SetCalls())

					assert.Len(t, redisCache.SetCalls(), 1)
				}
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
					Do:    func(ctx context.Context) (any, error) { return v, nil },
				}
			}

			c := New()
			assertFunc := tt.configure(t, c)

			require.NoError(t, c.GetSet(t.Context(), items...))
			assert.Equal(t, tt.failed, c.Failed())
			assert.Equal(t, tt.values, got)

			if assertFunc != nil {
				assertFunc()
			}
		})
	}
}

func TestCache_GetSet_itemDoNil(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name      string
		configure func(t *testing.T, c *Cache) func()
		cache     func() *Cache
		items     int
	}{
		{
			name: "1 item",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					GetFunc: func(ctx context.Context, maxItems int,
						keys iter.Seq[string],
					) iter.Seq2[[]byte, error] {
						assert.Equal(t, 1, maxItems)
						return makeBytesIter([][]byte{nil}, nil)
					},
				}
				c.WithRedisCache(redisCache)
				return func() {
					assert.Len(t, redisCache.GetCalls(), 1)
				}
			},
			items: 1,
		},
		{
			name: "2 items",
			configure: func(t *testing.T, c *Cache) func() {
				redisCache := &MoqRedisCache{
					GetFunc: func(ctx context.Context, maxItems int,
						keys iter.Seq[string],
					) iter.Seq2[[]byte, error] {
						assert.Equal(t, 2, maxItems)
						return makeBytesIter([][]byte{nil, nil}, nil)
					},
					SetFunc: func(ctx context.Context, maxItems int,
						items iter.Seq[cacheRedis.Item],
					) error {
						assert.Equal(t, 2, maxItems)
						return nil
					},
				}
				c.WithRedisCache(redisCache)
				return func() {
					assert.Len(t, redisCache.GetCalls(), 1)
					assert.Len(t, redisCache.SetCalls(), 1)
				}
			},
			items: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New()
			assertFunc := tt.configure(t, c)
			items := make([]Item, tt.items)
			want := make([]string, tt.items)
			got := make([]string, tt.items)
			for i := range tt.items {
				items[i] = Item{
					Key:   testKey + strconv.Itoa(i),
					Value: &got[i],
					Do:    func(ctx context.Context) (any, error) { return nil, nil },
				}
			}
			require.NoError(t, c.GetSet(ctx, items...))
			assert.Equal(t, want, got)
			assertFunc()
		})
	}
}

func TestCache_GetSet_marshalErr(t *testing.T) {
	redisCache := &MoqRedisCache{
		GetFunc: func(ctx context.Context, maxItems int, keys iter.Seq[string],
		) iter.Seq2[[]byte, error] {
			assert.Equal(t, 2, maxItems)
			return makeBytesIter([][]byte{nil, nil}, nil)
		},
	}
	cache := New().WithRedisCache(redisCache)

	testErr := errors.New("test error")
	err := cache.GetSet(t.Context(), Item{
		Key: testKey + "0",
		Do:  func(ctx context.Context) (any, error) { return nil, testErr },
	}, Item{
		Key: testKey + "1",
		Do:  func(ctx context.Context) (any, error) { return nil, testErr },
	})
	require.ErrorIs(t, err, testErr)
	assert.Len(t, redisCache.GetCalls(), 1)
}
