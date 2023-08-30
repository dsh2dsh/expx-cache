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

	mocks "github.com/dsh2dsh/expx-cache/mocks/cache"
)

func (self *CacheTestSuite) TestOnce_cacheFails() {
	ctx := context.Background()
	err := self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: int64(0),
	})
	self.Require().NoError(err)

	var got bool
	_, err = self.cache.Get(ctx, testKey, &got)
	self.Require().ErrorContains(err, "msgpack: invalid code=d3 decoding bool")

	err = self.cache.Once(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: &got,
		Do: func(*Item) (any, error) {
			return true, nil
		},
	})
	self.Require().NoError(err)
	self.True(got)

	got = false
	self.True(valueNoError[bool](self.T())(self.cache.Get(ctx, testKey, &got)))
	self.True(got)
}

func (self *CacheTestSuite) TestOnce_funcFails() {
	ctx := context.Background()
	perform(100, func(i int) {
		var got bool
		err := self.cache.Once(&Item{
			Ctx:   ctx,
			Key:   testKey,
			Value: &got,
			Do: func(*Item) (any, error) {
				return nil, io.EOF
			},
		})
		self.Require().ErrorIs(err, io.EOF)
		self.False(got)
	})

	var got bool
	self.False(valueNoError[bool](self.T())(self.cache.Get(ctx, testKey, &got)))

	err := self.cache.Once(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: &got,
		Do: func(*Item) (any, error) {
			return true, nil
		},
	})
	self.Require().NoError(err)
	self.True(got)
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
	var callCount int64
	ctx := context.Background()
	obj := self.CacheableValue()
	perform(100, func(int) {
		got := new(CacheableObject)
		err := self.cache.Once(&Item{
			Ctx:   ctx,
			Key:   testKey,
			Value: got,
			Do: func(*Item) (any, error) {
				atomic.AddInt64(&callCount, 1)
				return obj, nil
			},
		})
		self.Require().NoError(err)
		self.Equal(obj, got)
	})
	self.Equal(int64(1), callCount)
}

func (self *CacheTestSuite) TestOnce_withPtrNonPtr() {
	var callCount int64
	ctx := context.Background()
	obj := self.CacheableValue()
	perform(100, func(int) {
		got := new(CacheableObject)
		err := self.cache.Once(&Item{
			Ctx:   ctx,
			Key:   testKey,
			Value: got,
			Do: func(*Item) (any, error) {
				atomic.AddInt64(&callCount, 1)
				return *obj, nil
			},
		})
		self.Require().NoError(err)
		self.Equal(obj, got)
	})
	self.Equal(int64(1), callCount)
}

func (self *CacheTestSuite) TestOnce_withBool() {
	var callCount int64
	ctx := context.Background()
	perform(100, func(int) {
		var got bool
		err := self.cache.Once(&Item{
			Ctx:   ctx,
			Key:   testKey,
			Value: &got,
			Do: func(*Item) (any, error) {
				atomic.AddInt64(&callCount, 1)
				return true, nil
			},
		})
		self.Require().NoError(err)
		self.True(got)
	})
	self.Equal(int64(1), callCount)
}

func (self *CacheTestSuite) TestOnce_withoutValueAndNil() {
	var callCount int64
	ctx := context.Background()
	perform(100, func(int) {
		err := self.cache.Once(&Item{
			Ctx: ctx,
			Key: testKey,
			Do: func(*Item) (any, error) {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&callCount, 1)
				return nil, nil
			},
		})
		self.Require().NoError(err)
	})
	self.Equal(int64(1), callCount)
}

func (self *CacheTestSuite) TestOnce_withoutValueAndErr() {
	var callCount int64
	ctx := context.Background()
	perform(100, func(int) {
		err := self.cache.Once(&Item{
			Ctx: ctx,
			Key: testKey,
			Do: func(*Item) (any, error) {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&callCount, 1)
				return nil, errors.New("error stub")
			},
		})
		self.ErrorContains(err, "error stub")
	})
	self.Equal(int64(1), callCount)
}

func (self *CacheTestSuite) TestOnce_doesntCacheErr() {
	var callCount int64
	ctx := context.Background()
	do := func(sleep time.Duration) (int, error) {
		var n int
		err := self.cache.Once(&Item{
			Ctx:   ctx,
			Key:   testKey,
			Value: &n,
			Do: func(*Item) (any, error) {
				time.Sleep(sleep)
				n := atomic.AddInt64(&callCount, 1)
				if n == 1 {
					return nil, errors.New("error stub")
				}
				return 42, nil
			},
		})
		if err != nil {
			return 0, err
		}
		return n, nil
	}

	perform(100, func(int) {
		n, err := do(100 * time.Millisecond)
		self.ErrorContains(err, "error stub")
		self.Equal(0, n)
	})

	perform(100, func(int) {
		n, err := do(0)
		self.NoError(err)
		self.Equal(42, n)
	})

	self.Equal(int64(2), callCount)
}

func (self *CacheTestSuite) TestOnce_skipSetTTLNeg() {
	key := "skip-set"
	ctx := context.Background()

	var value string
	self.Require().NoError(self.cache.Once(&Item{
		Ctx:   ctx,
		Key:   key,
		Value: &value,
		Do: func(item *Item) (any, error) {
			item.TTL = -1
			return "hello", nil
		},
	}))
	self.Equal("hello", value)

	if self.rdb != nil {
		exists := valueNoError[int64](self.T())(self.rdb.Exists(ctx, key).Result())
		self.Equal(int64(0), exists)
	}
}

func TestOnce_errUnmarshal(t *testing.T) {
	localCache := mocks.NewMockLocalCache(t)
	localCache.EXPECT().Get(mock.Anything).Return(nil)
	localCache.EXPECT().Set(mock.Anything, mock.Anything).Once()

	cache := New().WithLocalCache(localCache)
	var got bool
	err := cache.Once(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: &got,
		Do: func(*Item) (any, error) {
			return int64(0), nil
		},
	})
	require.Error(t, err)
	assert.False(t, got)
}

func TestOnce_errDelete(t *testing.T) {
	redisClient := mocks.NewMockRedisClient(t)
	redisClient.EXPECT().Get(mock.Anything, mock.Anything).Return([]byte{0x1}, nil)
	redisClient.EXPECT().Del(mock.Anything, mock.Anything).Return(io.EOF)

	cache := New().WithRedisCache(redisClient)
	var got bool
	err := cache.Once(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: &got,
		Do: func(*Item) (any, error) {
			return int64(0), nil
		},
	})
	require.ErrorIs(t, err, io.EOF)
	assert.False(t, got)
}

func TestOnce_errRedisLocalCacheNil(t *testing.T) {
	cache := New()
	var got bool
	err := cache.Once(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: &got,
		Do: func(*Item) (any, error) {
			return true, nil
		},
	})
	require.NoError(t, err)
	assert.True(t, got)
}
