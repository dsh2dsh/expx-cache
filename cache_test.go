package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/vmihailenco/msgpack/v5"

	mocks "github.com/dsh2dsh/expx-cache/mocks/cache"
	redisMocks "github.com/dsh2dsh/expx-cache/mocks/redis"
)

const testKey = "mykey"

func valueNoError[V any](t *testing.T) func(val V, err error) V {
	return func(val V, err error) V {
		require.NoError(t, err)
		return val
	}
}

type CacheableObject struct {
	Str string
	Num int
}

type CacheTestSuite struct {
	suite.Suite

	cache *Cache
	rdb   redis.Cmdable

	newCache func() *Cache
}

func (self *CacheTestSuite) SetupTest() {
	self.cache = self.NewCache()
}

func (self *CacheTestSuite) NewCache() *Cache {
	if self.rdb != nil {
		self.Require().NoError(self.rdb.FlushDB(context.Background()).Err())
	}
	return self.newCache()
}

func (self *CacheTestSuite) CacheableValue() *CacheableObject {
	return &CacheableObject{
		Str: "mystring",
		Num: 42,
	}
}

// --------------------------------------------------

func (self *CacheTestSuite) TestGetSet_nil() {
	err := self.cache.Set(&Item{
		Key: testKey,
		TTL: time.Hour,
	})
	self.Require().NoError(err)

	ctx := context.Background()
	self.Require().ErrorIs(self.cache.Get(ctx, testKey, nil), ErrCacheMiss)

	self.False(self.cache.Exists(ctx, testKey), "nil value shouldn't be cached")
}

func (self *CacheTestSuite) TestGetSet_data() {
	ctx := context.Background()
	val := self.CacheableValue()
	err := self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: val,
		TTL:   time.Hour,
	})
	self.Require().NoError(err)

	gotValue := self.CacheableValue()
	self.Require().NoError(self.cache.Get(ctx, testKey, gotValue))
	self.Equal(val, gotValue)
	self.True(self.cache.Exists(ctx, testKey))
}

func (self *CacheTestSuite) TestGetSet_stringAsIs() {
	ctx := context.Background()
	value := "str_value"
	err := self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: value,
	})
	self.Require().NoError(err)

	var gotValue string
	self.Require().NoError(self.cache.Get(ctx, testKey, &gotValue))
	self.Equal(value, gotValue)
}

func (self *CacheTestSuite) TestGetSet_bytesAsIs() {
	ctx := context.Background()
	value := []byte("str_value")
	err := self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: value,
	})
	self.Require().NoError(err)

	var gotValue []byte
	self.Require().NoError(self.cache.Get(ctx, testKey, &gotValue))
	self.Equal(value, gotValue)
}

// --------------------------------------------------

func NewRedisClient() (redis.Cmdable, error) {
	cfg := struct {
		WithRedis string `env:"WITH_REDIS"`
	}{
		WithRedis: "skip", // "redis://localhost:6379/1",
	}
	envLoader := dotenv.New()
	if err := envLoader.WithDepth(1).WithEnvSuffix("test").LoadTo(&cfg); err != nil {
		return nil, fmt.Errorf("load .env: %w", err)
	} else if cfg.WithRedis == "skip" {
		return nil, nil
	}

	opt, err := redis.ParseURL(cfg.WithRedis)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL %q: %w", cfg.WithRedis, err)
	}

	rdb := redis.NewClient(opt)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %q: %w", cfg.WithRedis, err)
	}

	return rdb, nil
}

func TestCacheSuite(t *testing.T) {
	var rdb redis.Cmdable
	if !testing.Short() {
		rdb = valueNoError[redis.Cmdable](t)(NewRedisClient())
	}

	tests := []struct {
		name      string
		withStats bool
		withRedis func(*Cache) *Cache
	}{
		{
			name: "without Stats",
		},
		{
			name: "with RefreshRedis",
			withRedis: func(c *Cache) *Cache {
				return c.WithRedisCache(NewRefreshRedis(rdb, time.Second))
			},
		},
		{
			name:      "with Stats",
			withStats: true,
		},
		{
			name:      "with Stats and RefreshRedis",
			withStats: true,
			withRedis: func(c *Cache) *Cache {
				return c.WithRedisCache(NewRefreshRedis(rdb, time.Second))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			skipRedisTests(t, tt.name, tt.withRedis != nil, rdb != nil)
			runCacheTestSuite(t, rdb, tt.withStats, tt.withRedis)
		})
	}
}

func skipRedisTests(t *testing.T, name string, withRedis, hasRedis bool) {
	if testing.Short() && withRedis {
		t.Skipf("skip %q in short mode, because it uses Redis", name)
	}
	if withRedis && !hasRedis {
		t.Skipf("skip %q, because no Redis connection", name)
	}
}

func runCacheTestSuite(t *testing.T, rdb redis.Cmdable, withStats bool,
	withRedis func(*Cache) *Cache,
) {
	tests := []struct {
		name      string
		rdb       redis.Cmdable
		withLocal bool
		withRedis bool
	}{
		{
			name:      "without LocalCache",
			rdb:       rdb,
			withRedis: true,
		},
		{
			name:      "with LocalCache",
			rdb:       rdb,
			withLocal: true,
			withRedis: true,
		},
		{
			name:      "with LocalCache and without Redis",
			withLocal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			skipRedisTests(t, tt.name, tt.withRedis, rdb != nil)
			suite.Run(t, &CacheTestSuite{
				rdb: tt.rdb,
				newCache: func() *Cache {
					cache := New().WithStats(withStats)
					if tt.withLocal {
						cache = cache.WithTinyLFU(1000, time.Minute)
					}
					if tt.withRedis && withRedis != nil {
						cache = withRedis(cache)
					} else if tt.withRedis {
						cache = cache.WithRedis(rdb)
					}
					return cache
				},
			})
		})
	}
}

// --------------------------------------------------

func TestWithLocalCache(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.Nil(t, cache.localCache)

	localCache := NewTinyLFU(1000, time.Minute)
	assert.Same(t, cache, cache.WithLocalCache(localCache))
	assert.NotNil(t, cache.localCache)
	assert.Same(t, localCache, cache.localCache)

	assert.Same(t, cache, cache.WithLocalCache(nil))
	assert.Nil(t, cache.localCache)
}

func TestWithRedis(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.Nil(t, cache.redis)

	assert.Same(t, cache, cache.WithRedis(redis.NewClient(&redis.Options{})))
	require.NotNil(t, cache)
	assert.NotNil(t, cache.redis)

	assert.Same(t, cache, cache.WithRedis(nil))
	assert.Nil(t, cache.redis)
}

func TestWithRedisCache(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.Nil(t, cache.redis)

	redisCache := NewStdRedis(nil)
	assert.Same(t, cache, cache.WithRedisCache(redisCache))
	assert.NotNil(t, cache.redis)
	assert.Same(t, cache.redis, redisCache)

	assert.Same(t, cache, cache.WithRedisCache(nil))
	assert.Nil(t, cache.redis)
}

func TestWithStats(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.False(t, cache.statsEnabled)
	require.Same(t, cache, cache.WithStats(true))
	assert.True(t, cache.statsEnabled)
}

func TestWithTinyLFU(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.Nil(t, cache.localCache)
	assert.Same(t, cache, cache.WithTinyLFU(1000, time.Minute))
	assert.NotNil(t, cache.localCache)
}

// --------------------------------------------------

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
	self.ErrorIs(self.cache.Get(ctx, testKey, nil), ErrCacheMiss)
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

func TestDelete_errRedisLocalCacheNil(t *testing.T) {
	cache := New()
	err := cache.Delete(context.Background(), testKey)
	assert.ErrorIs(t, err, errRedisLocalCacheNil)
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

// --------------------------------------------------

func TestGet_errRedisLocalCacheNil(t *testing.T) {
	cache := New()
	err := cache.Get(context.Background(), testKey, nil)
	assert.ErrorIs(t, err, errRedisLocalCacheNil)
}

func TestGet_redisErrAddsMiss(t *testing.T) {
	redisClient := mocks.NewMockRedisClient(t)
	redisClient.EXPECT().Get(mock.Anything, mock.Anything).Return(nil, io.EOF)

	cache := New().WithStats(true).WithRedisCache(redisClient)
	err := cache.Get(context.Background(), testKey, nil)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, uint64(1), cache.Stats().Misses)
}

func TestGetSkippingLocalCache(t *testing.T) {
	localCache := mocks.NewMockLocalCache(t)

	redisClient := mocks.NewMockRedisClient(t)
	redisClient.EXPECT().Get(mock.Anything, mock.Anything).Return(nil, nil)

	cache := New().WithLocalCache(localCache).WithRedisCache(redisClient)
	assert.ErrorIs(t,
		cache.GetSkippingLocalCache(context.Background(), testKey, nil),
		ErrCacheMiss)
}

// --------------------------------------------------

func TestItem_ttlTooSmall(t *testing.T) {
	item := &Item{TTL: 500 * time.Millisecond}
	assert.Equal(t, time.Second, item.ttl())
}

// --------------------------------------------------

func TestCache_WithMarshal(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	var called bool
	cache.WithMarshal(func(v any) ([]byte, error) {
		called = true
		return marshal(v)
	})

	ctx := context.Background()
	value := "abc"
	require.NoError(t, cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: value,
	}))

	assert.True(t, called, "custom marshall func wasn't called")
}

func TestCache_WithUnmarshal(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	ctx := context.Background()
	value := "abc"
	require.NoError(t, cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: value,
	}))

	var called bool
	cache.WithUnmarshal(func(b []byte, v any) error {
		called = true
		return unmarshal(b, v)
	})
	require.NoError(t, cache.Get(ctx, testKey, &value))
	assert.True(t, called, "custom unmarshall func wasn't called")
}

func TestCache_Marshal_nil(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)
	assert.Nil(t, valueNoError[[]byte](t)(cache.Marshal(nil)))
}

func TestCache_Marshal_compression(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	s := strings.Repeat("foobar", 100)
	b := valueNoError[[]byte](t)(cache.Marshal(&s))
	assert.NotNil(t, b)
	assert.Equal(t, s2Compression, int(b[len(b)-1]))
}

func TestCache_Marshal_noCompression(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	s := "foobar" //nolint:goconst // I don't want use const for this
	b := valueNoError[[]byte](t)(cache.Marshal(&s))
	assert.NotNil(t, b)
	assert.Equal(t, noCompression, int(b[len(b)-1]))
}

type msgpackErrItem struct {
	Foo string
}

func (self *msgpackErrItem) EncodeMsgpack(enc *msgpack.Encoder) error {
	return io.EOF
}

func TestCache_Marshal_msgpackErr(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	b, err := cache.Marshal(&msgpackErrItem{"bar"})
	assert.Error(t, err)
	assert.Nil(t, b)
}

func TestCache_Unmarshal_nil(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	assert.NoError(t, cache.Unmarshal([]byte{}, nil))
	assert.NoError(t, cache.Unmarshal([]byte("foobar"), nil))
}

func TestCache_Unmarshal_compression(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	require.NotNil(t, cache)

	type fooItem struct {
		Foo string
	}
	item := fooItem{
		Foo: strings.Repeat("foobar", 100),
	}

	b := valueNoError[[]byte](t)(cache.Marshal(&item))
	assert.Equal(t, s2Compression, int(b[len(b)-1]))

	gotItem := fooItem{}
	assert.NoError(t, cache.Unmarshal(b, &gotItem))
	assert.Equal(t, item, gotItem)

	assert.ErrorContains(t, cache.Unmarshal([]byte{0x0, 0xff}, &gotItem),
		"unknown compression method")

	assert.ErrorContains(t, cache.Unmarshal([]byte{0x1, s2Compression}, &gotItem),
		"unmarshal: decompress error")
}

// --------------------------------------------------

func (self *CacheTestSuite) TestOnce_cacheFails() {
	ctx := context.Background()
	err := self.cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: int64(0),
	})
	self.Require().NoError(err)

	var got bool
	err = self.cache.Get(ctx, testKey, &got)
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
	self.Require().NoError(self.cache.Get(ctx, testKey, &got))
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
	err := self.cache.Get(ctx, testKey, &got)
	self.Require().ErrorIs(err, ErrCacheMiss)

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
		self.EqualError(err, "error stub")
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
		self.EqualError(err, "error stub")
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

// --------------------------------------------------

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

func (self *CacheTestSuite) TestSetNX() {
	if self.rdb == nil {
		self.T().Skip("requires Redis connection")
	}

	ctx := context.Background()
	value := "foobar"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value,
		SetNX:          true,
		SkipLocalCache: true,
	}))

	var got string
	self.Require().NoError(self.cache.GetSkippingLocalCache(ctx, testKey, &got))
	self.Equal(value, got)

	value2 := "barfoo"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value2,
		SetNX:          true,
		SkipLocalCache: true,
	}))

	self.Require().NoError(self.cache.GetSkippingLocalCache(ctx, testKey, &got))
	self.Equal(value, got)
}

func (self *CacheTestSuite) TestSetXX() {
	if self.rdb == nil {
		self.T().Skip("requires Redis connection")
	}

	ctx := context.Background()
	value := "foobar"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value,
		SetXX:          true,
		SkipLocalCache: true,
	}))

	self.ErrorIs(self.cache.GetSkippingLocalCache(ctx, testKey, nil), ErrCacheMiss)

	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value,
		SkipLocalCache: true,
	}))

	var got string
	self.Require().NoError(self.cache.GetSkippingLocalCache(ctx, testKey, &got))
	self.Equal(value, got)

	value = "barfoo"
	self.Require().NoError(self.cache.Set(&Item{
		Ctx:            ctx,
		Key:            testKey,
		Value:          value,
		SetXX:          true,
		SkipLocalCache: true,
	}))

	self.Require().NoError(self.cache.GetSkippingLocalCache(ctx, testKey, &got))
	self.Equal(value, got)
}

func TestCache_Set_Marshall_error(t *testing.T) {
	cache := New().WithMarshal(func(value any) ([]byte, error) {
		return nil, io.EOF
	})

	err := cache.Set(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: "foobar",
	})
	assert.ErrorIs(t, err, io.EOF)
}

func TestCache_Set_errRedisLocalCacheNil(t *testing.T) {
	cache := New()

	err := cache.Set(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: "foobar",
	})
	assert.ErrorIs(t, err, errRedisLocalCacheNil)
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

// --------------------------------------------------

func TestCache_Stats_statsEnabled(t *testing.T) {
	cache := New()
	assert.Nil(t, cache.Stats())

	cache = cache.WithStats(true)
	require.NotNil(t, cache.Stats())
	assert.IsType(t, &Stats{}, cache.Stats())
}
