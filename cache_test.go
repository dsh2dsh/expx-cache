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

func (self *CacheTestSuite) SetupSuite() {
	if self.rdb != nil {
		self.Require().NoError(self.rdb.FlushDB(context.Background()).Err())
	}
}

func (self *CacheTestSuite) SetupTest() {
	self.cache = self.newCache()
}

func (self *CacheTestSuite) TearDownTest() {
	if self.rdb != nil && self.cache.redis != nil {
		self.Require().NoError(self.rdb.FlushDB(context.Background()).Err())
	}
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
	self.False(valueNoError[bool](self.T())(self.cache.Get(ctx, testKey, nil)))

	self.False(valueNoError[bool](self.T())(self.cache.Exists(ctx, testKey)),
		"nil value shouldn't be cached")
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
	self.Require().True(
		valueNoError[bool](self.T())(self.cache.Get(ctx, testKey, gotValue)))
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
	self.Require().True(
		valueNoError[bool](self.T())(self.cache.Get(ctx, testKey, &gotValue)))
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
	self.Require().True(
		valueNoError[bool](self.T())(self.cache.Get(ctx, testKey, &gotValue)))
	self.Equal(value, gotValue)
}

// --------------------------------------------------

func NewRedisClient() (*redis.Client, error) {
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

type cacheSubTest func(t *testing.T, cfg func(*Cache) *Cache,
	suiteRun cacheSubTest, subTests []cacheSubTest)

func TestCacheSuite(t *testing.T) {
	t.Parallel()

	var rdb *redis.Client
	if !testing.Short() {
		rdb = valueNoError[*redis.Client](t)(NewRedisClient())
	}

	tests := []struct {
		name       string
		needsLocal bool
		needsRedis bool
		subTests   func(rdb redis.Cmdable) []cacheSubTest
	}{
		{
			name:       "with LocalCache",
			needsLocal: true,
			needsRedis: true,
			subTests: func(rdb redis.Cmdable) []cacheSubTest {
				return []cacheSubTest{subTestsWithRedis(rdb), subTestsWithStats}
			},
		},
		{
			name:       "without LocalCache",
			needsRedis: true,
			subTests: func(rdb redis.Cmdable) []cacheSubTest {
				return []cacheSubTest{subTestsWithRedis(rdb), subTestsWithStats}
			},
		},
		{
			name:       "without Redis",
			needsLocal: true,
			subTests: func(rdb redis.Cmdable) []cacheSubTest {
				return []cacheSubTest{subTestsWithStats}
			},
		},
	}

	db := clientDB(t, rdb)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			skipRedisTests(t, tt.name, tt.needsRedis, rdb != nil)
			var r redis.Cmdable
			if tt.needsRedis && rdb != nil {
				cn := rdb.Conn()
				t.Cleanup(func() {
					require.NoError(t, cn.Close())
				})
				require.NoError(t, cn.Select(context.Background(), db).Err())
				db++
				r = cn
			}
			cfg := func(c *Cache) *Cache {
				if tt.needsLocal {
					return c.WithTinyLFU(1000, time.Minute)
				}
				return c
			}
			suiteRunSubTests(t, r, cfg, tt.subTests(r))
		})
	}
}

func clientDB(t *testing.T, rdb *redis.Client) int {
	if rdb != nil {
		info := valueNoError[*redis.ClientInfo](t)(
			rdb.ClientInfo(context.Background()).Result())
		return info.DB
	}
	return 0
}

func skipRedisTests(t *testing.T, name string, needsRedis, hasRedis bool) {
	if needsRedis {
		if testing.Short() {
			t.Skipf("skip %q in short mode, because it requires Redis", name)
		} else if !hasRedis {
			t.Skipf("skip %q, because no Redis connection", name)
		}
	}
}

func suiteRunSubTests(t *testing.T, rdb redis.Cmdable, cfg func(*Cache) *Cache,
	subTests []cacheSubTest,
) {
	t.Parallel()
	runCacheSubTests(t, cfg,
		func(t *testing.T, cfg func(*Cache) *Cache, _ cacheSubTest,
			subTests []cacheSubTest,
		) {
			suite.Run(t, &CacheTestSuite{
				rdb: rdb,
				newCache: func() *Cache {
					return cfg(New())
				},
			})
		}, subTests)
}

func runCacheSubTests(t *testing.T, cfg func(*Cache) *Cache,
	suiteRun cacheSubTest, subTests []cacheSubTest,
) {
	if len(subTests) > 0 {
		subTests[0](t, cfg, suiteRun, subTests[1:])
	} else {
		suiteRun(t, cfg, nil, nil)
	}
}

func subTestsWithRedis(rdb redis.Cmdable) cacheSubTest {
	return func(t *testing.T, parentCfg func(*Cache) *Cache,
		suiteRun cacheSubTest, subTests []cacheSubTest,
	) {
		tests := []struct {
			name      string
			withRedis func(c *Cache) *Cache
		}{
			{
				name: "with StdRedis",
				withRedis: func(c *Cache) *Cache {
					return c.WithRedis(rdb)
				},
			},
			{
				name: "with RefreshRedis",
				withRedis: func(c *Cache) *Cache {
					return c.WithRedisCache(NewRefreshRedis(rdb, time.Minute))
				},
			},
		}

		for _, tt := range tests {
			cfg := func(c *Cache) *Cache {
				return tt.withRedis(parentCfg(c))
			}
			t.Run(tt.name, func(t *testing.T) {
				runCacheSubTests(t, cfg, suiteRun, subTests)
			})
		}
	}
}

func subTestsWithStats(t *testing.T, parentCfg func(*Cache) *Cache,
	suiteRun cacheSubTest, subTests []cacheSubTest,
) {
	tests := []struct {
		name      string
		withStats bool
	}{
		{
			name: "without Stats",
		},
		{
			name:      "with Stats",
			withStats: true,
		},
	}

	for _, tt := range tests {
		cfg := func(c *Cache) *Cache {
			return parentCfg(c).WithStats(tt.withStats)
		}
		t.Run(tt.name, func(t *testing.T) {
			runCacheSubTests(t, cfg, suiteRun, subTests)
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

func TestWithDefaultTTL(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	assert.Equal(t, defaultTTL, cache.defaultTTL)
	assert.Equal(t, cache.defaultTTL, cache.DefaultTTL())

	ttl := cache.DefaultTTL() + time.Hour
	assert.Same(t, cache, cache.WithDefaultTTL(ttl))
	assert.Equal(t, ttl, cache.defaultTTL)
	assert.Equal(t, cache.defaultTTL, cache.DefaultTTL())
}

func TestCache_ItemTTL(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	tests := []struct {
		name    string
		TTL     time.Duration
		wantTTL time.Duration
	}{
		{
			name:    "too small",
			TTL:     time.Millisecond,
			wantTTL: time.Second,
		},
		{
			name:    "negative",
			TTL:     -time.Second,
			wantTTL: time.Duration(0),
		},
		{
			name:    "ok",
			TTL:     time.Minute,
			wantTTL: time.Minute,
		},
		{
			name:    "defaultTTL",
			wantTTL: cache.DefaultTTL(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantTTL, cache.ItemTTL(&Item{TTL: tt.TTL}))
		})
	}
}

func TestWithBatchSize(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	assert.Equal(t, defaultBatchSize, cache.batchSize)

	batchSize := cache.batchSize * 2
	assert.Same(t, cache, cache.WithBatchSize(batchSize))
	assert.Equal(t, batchSize, cache.batchSize)
}
