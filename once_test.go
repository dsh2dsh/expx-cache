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

func (self *CacheTestSuite) TestOnce_cacheFails() {
	ctx := context.Background()
	item := Item{Key: testKey, Value: int64(0)}
	self.Require().NoError(self.cache.Set(ctx, item))

	var got bool
	item.Value = &got
	_, err := self.cache.Get(ctx, item)
	self.Require().ErrorContains(err, "msgpack: invalid code=d3 decoding bool")
	self.expectCacheHit()

	item.Do = func(ctx context.Context) (any, error) { return true, nil }
	self.Require().NoError(self.cache.Once(ctx, item))
	self.True(got)
	self.expectCacheHit()
	self.expectCacheMiss()

	got = false
	item.Do = nil
	self.Empty(valueNoError[[]Item](self.T())(self.cache.Get(ctx, item)))
	self.True(got)
	self.expectCacheHit()

	self.assertStats()
}

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
	perform(100, func(int) {
		got := new(CacheableObject)
		hit := true
		err := self.cache.Once(ctx, Item{
			Key:   testKey,
			Value: got,
			Do: func(ctx context.Context) (any, error) {
				atomic.AddUint64(&callCount, 1)
				self.expectCacheMiss()
				hit = false
				return obj, nil
			},
		})
		self.Require().NoError(err)
		self.Equal(&obj, got)
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
	perform(100, func(int) {
		got := new(CacheableObject)
		hit := true
		err := self.cache.Once(ctx, Item{
			Key:   testKey,
			Value: got,
			Do: func(ctx context.Context) (any, error) {
				atomic.AddUint64(&callCount, 1)
				self.expectCacheMiss()
				hit = false
				return obj, nil
			},
		})
		self.Require().NoError(err)
		self.Equal(&obj, got)
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
	perform(100, func(int) {
		var got bool
		hit := true
		err := self.cache.Once(ctx, Item{
			Key:   testKey,
			Value: &got,
			Do: func(ctx context.Context) (any, error) {
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
	perform(100, func(int) {
		hit := true
		err := self.cache.Once(ctx, Item{
			Key: testKey,
			Do: func(ctx context.Context) (any, error) {
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
	perform(100, func(int) {
		err := self.cache.Once(ctx, Item{
			Key: testKey,
			Do: func(ctx context.Context) (any, error) {
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
	do := func(sleep time.Duration) (int, error) {
		var n int
		hit := true
		err := self.cache.Once(ctx, Item{
			Key:   testKey,
			Value: &n,
			Do: func(ctx context.Context) (any, error) {
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

	perform(100, func(int) {
		n, err := do(100 * time.Millisecond)
		self.Require().ErrorIs(err, errStub)
		self.Equal(0, n)
	})

	perform(100, func(int) {
		n, err := do(0)
		self.Require().NoError(err)
		self.Equal(42, n)
	})

	self.Equal(uint64(2), callCount)
	self.assertStats()
}

func (self *CacheTestSuite) TestOnce_withNegTTL() {
	key := "skip-set"
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
		ttl := valueNoError[time.Duration](self.T())(self.rdb.TTL(ctx, key).Result())
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

func TestOnce_errDelete(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	redisCache := mocks.NewMockRedisCache(t)
	redisCache.EXPECT().Get(ctx, 1, mock.Anything).Return(
		makeBytesIter([][]byte{{0x1}}), nil)
	redisCache.EXPECT().Del(mock.Anything, mock.Anything).Return(wantErr)

	cache := New().WithRedisCache(redisCache)
	var got bool
	err := cache.Once(ctx, Item{
		Key:   testKey,
		Value: &got,
		Do: func(ctx context.Context) (any, error) {
			return int64(0), nil
		},
	})
	require.ErrorIs(t, err, wantErr)
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
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
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
