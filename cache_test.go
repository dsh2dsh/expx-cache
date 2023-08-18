package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
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
