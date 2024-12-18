package cache

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
)

func (self *CacheTestSuite) TestOnce_funcFails() {
	ctx := context.Background()
	wantErr := errors.New("expected error")
	perform(100, func(i int) {
		var got bool
		err := self.cache.Once(ctx, Item{
			Key:   testKey,
			Value: &got,
			Do: func(ctx context.Context) (any, error) {
				self.expectCacheMiss()
				return nil, wantErr
			},
		})
		self.Require().ErrorIs(err, wantErr)
		self.False(got)
	})

	var got bool
	item := Item{Key: testKey, Value: &got}
	self.Equal([]Item{item},
		valueNoError[[]Item](self.T())(self.cache.Get(ctx, item)))
	self.expectCacheMiss()

	err := self.cache.Once(ctx, Item{
		Key:   testKey,
		Value: &got,
		Do: func(ctx context.Context) (any, error) {
			return true, nil
		},
	})
	self.Require().NoError(err)
	self.True(got)
	self.expectCacheMiss()

	self.assertStats()
}

func perform(n int, callbacks ...func(int)) {
	var wg sync.WaitGroup
	for _, cb := range callbacks {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(cb func(int), i int) {
				defer wg.Done()
				cb(i)
			}(cb, i)
		}
	}
	wg.Wait()
}

func (self *CacheTestSuite) TestOnce_withValue() {
	var callCount uint64
	ctx := context.Background()
	obj := self.CacheableValue()
	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	perform(n, func(int) {
		var got CacheableObject
		hit := true
		wg.Done()
		err := self.cache.Once(ctx, Item{
			Key:   testKey,
			Value: &got,
			Do: func(ctx context.Context) (any, error) {
				wg.Wait()
				time.Sleep(100 * time.Millisecond)
				atomic.AddUint64(&callCount, 1)
				self.expectCacheMiss()
				hit = false
				return obj, nil
			},
		})
		self.Require().NoError(err)
		self.Equal(&obj, &got)
		if hit && self.cache.statsEnabled {
			self.stats.localHit()
		}
	})
	self.Equal(uint64(1), callCount)
	self.assertStats()
}

func (self *CacheTestSuite) TestOnce_withPtrNonPtr() {
	var callCount uint64
	ctx := context.Background()
	obj := self.CacheableValue()
	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	perform(n, func(int) {
		var got CacheableObject
		hit := true
		wg.Done()
		err := self.cache.Once(ctx, Item{
			Key:   testKey,
			Value: &got,
			Do: func(ctx context.Context) (any, error) {
				wg.Wait()
				time.Sleep(100 * time.Millisecond)
				atomic.AddUint64(&callCount, 1)
				self.expectCacheMiss()
				hit = false
				return obj, nil
			},
		})
		self.Require().NoError(err)
		self.Equal(&obj, &got)
		if hit && self.cache.statsEnabled {
			self.stats.localHit()
		}
	})
	self.Equal(uint64(1), callCount)
	self.assertStats()
}

func (self *CacheTestSuite) TestOnce_withBool() {
	var callCount uint64
	ctx := context.Background()
	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	perform(n, func(int) {
		var got bool
		hit := true
		wg.Done()
		err := self.cache.Once(ctx, Item{
			Key:   testKey,
			Value: &got,
			Do: func(ctx context.Context) (any, error) {
				wg.Wait()
				time.Sleep(100 * time.Millisecond)
				atomic.AddUint64(&callCount, 1)
				self.expectCacheMiss()
				hit = false
				return true, nil
			},
		})
		self.Require().NoError(err)
		self.True(got)
		if hit && self.cache.statsEnabled {
			self.stats.localHit()
		}
	})
	self.Equal(uint64(1), callCount)
	self.assertStats()
}

func (self *CacheTestSuite) TestOnce_withoutValueAndNil() {
	var callCount uint64
	ctx := context.Background()
	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	perform(n, func(int) {
		hit := true
		wg.Done()
		err := self.cache.Once(ctx, Item{
			Key: testKey,
			Do: func(ctx context.Context) (any, error) {
				wg.Wait()
				time.Sleep(100 * time.Millisecond)
				atomic.AddUint64(&callCount, 1)
				self.expectCacheMiss()
				hit = false
				return nil, nil
			},
		})
		self.Require().NoError(err)
		if hit && self.cache.statsEnabled {
			self.stats.localHit()
		}
	})
	self.Equal(uint64(1), callCount)
	self.assertStats()
}

func (self *CacheTestSuite) TestOnce_withoutValueAndErr() {
	var callCount uint64
	ctx := context.Background()
	errStub := errors.New("error stub")
	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	perform(n, func(int) {
		wg.Done()
		err := self.cache.Once(ctx, Item{
			Key: testKey,
			Do: func(ctx context.Context) (any, error) {
				wg.Wait()
				time.Sleep(100 * time.Millisecond)
				atomic.AddUint64(&callCount, 1)
				self.expectCacheMiss()
				return nil, errStub
			},
		})
		self.Require().ErrorIs(err, errStub)
	})
	self.Equal(uint64(1), callCount)
	self.assertStats()
}

func (self *CacheTestSuite) TestOnce_doesntCacheErr() {
	var callCount uint64
	ctx := context.Background()
	errStub := errors.New("error stub")
	var wg sync.WaitGroup
	do := func(sleep time.Duration) (int, error) {
		var n int
		hit := true
		wg.Done()
		err := self.cache.Once(ctx, Item{
			Key:   testKey,
			Value: &n,
			Do: func(ctx context.Context) (any, error) {
				wg.Wait()
				time.Sleep(sleep)
				self.expectCacheMiss()
				hit = false
				n := atomic.AddUint64(&callCount, 1)
				if n == 1 {
					return nil, errStub
				}
				return 42, nil
			},
		})
		if err != nil {
			return 0, err
		} else if hit && self.cache.statsEnabled {
			self.stats.localHit()
		}
		return n, nil
	}

	procs := 100
	wg.Add(procs)
	perform(procs, func(int) {
		n, err := do(100 * time.Millisecond)
		self.Require().ErrorIs(err, errStub)
		self.Equal(0, n)
	})

	wg.Add(procs)
	perform(procs, func(int) {
		n, err := do(100 * time.Millisecond)
		self.Require().NoError(err)
		self.Equal(42, n)
	})

	self.Equal(uint64(2), callCount)
	self.assertStats()
}

func (self *CacheTestSuite) TestOnce_withNegTTL() {
	key := testKey + "-skip-set"
	ctx := context.Background()

	var value string
	self.Require().NoError(self.cache.Once(ctx, Item{
		Key:   key,
		Value: &value,
		TTL:   -1,
		Do: func(ctx context.Context) (any, error) {
			return "hello", nil
		},
	}))
	self.Equal("hello", value)
	self.expectCacheMiss()

	if self.rdb != nil {
		ttl := valueNoError[time.Duration](self.T())(self.rdb.TTL(
			ctx, self.cache.ResolveKey(key)).Result())
		self.Equal(self.cache.DefaultTTL(), ttl)
	}
	self.assertStats()
}

func TestOnce_errUnmarshal(t *testing.T) {
	localCache := mocks.NewMockLocalCache(t)
	localCache.EXPECT().Get(mock.Anything).Return(nil)
	localCache.EXPECT().Set(mock.Anything, mock.Anything).Once()

	cache := New().WithLocalCache(localCache)
	var got bool
	err := cache.Once(context.Background(), Item{
		Key:   testKey,
		Value: &got,
		Do: func(ctx context.Context) (any, error) {
			return int64(0), nil
		},
	})
	require.Error(t, err)
	assert.False(t, got)
}

func TestOnce_withoutCache(t *testing.T) {
	cache := New()
	callCount := 0
	got := false
	require.NoError(t, cache.Once(context.Background(), Item{
		Key:   testKey,
		Value: &got,
		Do: func(ctx context.Context) (any, error) {
			callCount++
			return true, nil
		},
	}))
	assert.Equal(t, 1, callCount)
	assert.True(t, got)
}

func TestCache_Once_withKeyWrapper(t *testing.T) {
	t.Parallel()

	const keyPrefix = "baz:"
	wantKey := keyPrefix + testKey

	cache := New().WithKeyWrapper(func(key string) string {
		return keyPrefix + key
	})

	onceSlipping := make(chan struct{})
	onceErr := make(chan error)
	var got bool

	go func() {
		onceErr <- cache.Once(context.Background(), Item{
			Key:   testKey,
			Value: &got,
			Do: func(ctx context.Context) (any, error) {
				onceSlipping <- struct{}{}
				time.Sleep(100 * time.Millisecond)
				return true, nil
			},
		})
	}()

	<-onceSlipping
	_, err, shared := cache.group.Do(wantKey, func() (any, error) {
		return false, io.EOF
	})
	require.NoError(t, <-onceErr)
	assert.True(t, got)

	require.NoError(t, err, "Once() uses wrong key for Do()")
	assert.True(t, shared)
}

func TestCache_Once_withErrRedisCache(t *testing.T) {
	const foobar = "foobar"
	ctx := context.Background()
	testErr := errors.New("test error")

	tests := []struct {
		name   string
		cache  func() *Cache
		itemDo func()
	}{
		{
			name: "with Err set",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				_ = cache.redisCacheError(testErr)
				return cache
			},
		},
		{
			name: "redisGet ErrRedisCache",
			cache: func() *Cache {
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithRedisCache(redisCache)
				redisCache.EXPECT().Get(ctx, 1, mock.Anything).Return(nil, testErr)
				return cache
			},
		},
		{
			name: "redisSet ErrRedisCache",
			cache: func() *Cache {
				localCache := mocks.NewMockLocalCache(t)
				localCache.EXPECT().Get(testKey).Return(nil)
				localCache.EXPECT().Set(testKey, []byte(foobar))
				redisCache := mocks.NewMockRedisCache(t)
				cache := New().WithLocalCache(localCache).WithRedisCache(redisCache)
				redisCache.EXPECT().Get(ctx, 1, mock.Anything).Return(
					makeBytesIter([][]byte{nil}), nil)
				redisCache.EXPECT().Set(ctx, 1, mock.Anything).Return(testErr)
				return cache
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := tt.cache()
			var got string
			err := cache.Once(ctx, Item{
				Key:   testKey,
				Value: &got,
				Do: func(ctx context.Context) (any, error) {
					if tt.itemDo != nil {
						tt.itemDo()
					}
					return foobar, nil
				},
			})
			require.NoError(t, err)
			assert.True(t, cache.Failed())
			assert.Equal(t, foobar, got)
		})
	}
}
