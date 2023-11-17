package cache

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caarlos0/env/v10"
	dotenv "github.com/dsh2dsh/expx-dotenv"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/dsh2dsh/expx-cache/local"
	"github.com/dsh2dsh/expx-cache/redis/classic"
)

const testKey = "mykey"

func valueNoError[V any](t *testing.T) func(val V, err error) V {
	return func(val V, err error) V {
		require.NoError(t, err)
		return val
	}
}

func mgetIter3(
	ctx context.Context, keys []string,
) (context.Context, int, func(itemIdx int) string) {
	return ctx, len(keys), func(itemIdx int) string { return keys[itemIdx] }
}

func makeBytesIter(blobs [][]byte) func() ([]byte, bool) {
	var nextItem int
	return func() (b []byte, ok bool) {
		if nextItem < len(blobs) {
			b, ok = blobs[nextItem], true
			nextItem++
		}
		return
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
	stats *Stats

	newCache func() *Cache
}

func (self *CacheTestSuite) SetupSuite() {
	if self.rdb != nil {
		self.Require().NoError(self.rdb.FlushDB(context.Background()).Err())
	}
}

func (self *CacheTestSuite) SetupTest() {
	self.cache = self.newCache()
	self.stats = &Stats{}
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

func (self *CacheTestSuite) cacheHit() {
	if self.cache.statsEnabled {
		if self.cache.localCache != nil {
			self.stats.localHit()
		} else if self.cache.redis != nil {
			self.stats.hit()
		}
	}
}

func (self *CacheTestSuite) cacheMiss() {
	if self.cache.statsEnabled {
		if self.cache.localCache != nil {
			self.stats.localMiss()
		}
		if self.cache.redis != nil {
			self.stats.miss()
		}

	}
}

func (self *CacheTestSuite) cacheHitLocalMiss() {
	if self.cache.statsEnabled {
		self.stats.hit()
		if self.cache.localCache != nil {
			self.stats.localMiss()
		}
	}
}

func (self *CacheTestSuite) assertStats() {
	s := self.cache.Stats()
	self.Equal(self.stats, &s, "unexpected cache Stats")
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
	self.cacheMiss()
	self.False(valueNoError[bool](self.T())(self.cache.Exists(ctx, testKey)),
		"nil value shouldn't be cached")
	self.cacheMiss()
	self.assertStats()
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
	self.cacheHit()
	self.Equal(val, gotValue)
	self.True(self.cache.Exists(ctx, testKey))
	self.cacheHit()
	self.assertStats()
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
	self.cacheHit()
	self.Equal(value, gotValue)
	self.assertStats()
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
	self.cacheHit()
	self.Equal(value, gotValue)
	self.assertStats()
}

func (self *CacheTestSuite) TestGetSet_many() {
	if testing.Short() {
		self.T().Skip("skipping in short mode")
	}

	maxItems := 21
	allKeys := make([]string, maxItems)
	allValues := make([]CacheableObject, maxItems)

	allItems := make([]*Item, maxItems)
	for i := 0; i < maxItems; i++ {
		key := fmt.Sprintf("key-%00d", i)
		allKeys[i] = key
		allValues[i].Str = key
		allValues[i].Num = i
		allItems[i] = &Item{
			Key:   key,
			Value: &allValues[i],
		}
	}

	ctx := context.Background()
	self.Require().NoError(self.cache.MSet(ctx, allItems))

	gotValues := make([]CacheableObject, maxItems)
	for i, item := range allItems {
		item.Value = &gotValues[i]
		self.cacheHit()
	}
	missed := valueNoError[[]*Item](self.T())(self.cache.MGet(ctx, allItems))
	for range missed {
		self.cacheMiss()
	}
	self.Equal(allValues, gotValues)
	self.Nil(missed)

	self.Require().NoError(self.cache.Delete(ctx, allKeys...))

	clear(gotValues)
	expectedValues := make([]CacheableObject, maxItems)
	missed = valueNoError[[]*Item](self.T())(self.cache.MGet(ctx, allItems))
	for range missed {
		self.cacheMiss()
	}
	self.Equal(expectedValues, gotValues)
	self.Equal(allItems, missed)

	self.assertStats()
}

func (self *CacheTestSuite) TestMGetSet() {
	if testing.Short() {
		self.T().Skip("skipping in short mode")
	}

	maxItems := 21
	allKeys := make([]string, maxItems)
	allValues := make([]CacheableObject, maxItems)

	allItems := make([]*Item, maxItems)
	var callCount uint64
	for i := 0; i < maxItems; i++ {
		key := fmt.Sprintf("key-%00d", i)
		allKeys[i] = key
		allValues[i].Str = key
		allValues[i].Num = i
		valueRef := &allValues[i]
		allItems[i] = &Item{
			Key:   key,
			Value: valueRef,
			Do: func(*Item) (any, error) {
				atomic.AddUint64(&callCount, 1)
				return *valueRef, nil
			},
		}
	}

	ctx := context.Background()
	self.Require().NoError(self.cache.MGetSet(ctx, allItems))
	self.Equal(uint64(maxItems), callCount)

	callCount = 0
	gotValues := make([]CacheableObject, maxItems)
	for i, item := range allItems {
		item.Value = &gotValues[i]
		self.cacheMiss()
		self.cacheHit()
	}
	self.Require().NoError(self.cache.MGetSet(ctx, allItems))
	self.Equal(uint64(0), callCount)
	self.Equal(allValues, gotValues)

	self.assertStats()
}

// --------------------------------------------------

func (self *CacheTestSuite) TestWithKeyWrapper() {
	const keyPrefix = "baz:"
	wantKey := keyPrefix + testKey

	cache := self.cache.New().WithKeyWrapper(func(key string) string {
		return keyPrefix + self.cache.WrapKey(key)
	})

	ctx := context.Background()
	self.Require().NoError(cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: self.CacheableValue(),
	}))
	self.True(keyExists(self.T(), cache, wantKey))
	self.True(keyNotExists(self.T(), cache, testKey))

	tests := []struct {
		name   string
		assert func(t *testing.T)
	}{
		{
			name: "Exists",
			assert: func(t *testing.T) {
				assert.True(t, valueNoError[bool](t)(cache.Exists(ctx, testKey)))
			},
		},
		{
			name: "Get",
			assert: func(t *testing.T) {
				got := new(CacheableObject)
				assert.True(t, valueNoError[bool](t)(cache.Get(ctx, testKey, got)))
				assert.Equal(t, self.CacheableValue(), got)
			},
		},
		{
			name: "Once",
			assert: func(t *testing.T) {
				got := new(CacheableObject)
				require.NoError(t, cache.Once(&Item{
					Ctx:   ctx,
					Key:   testKey,
					Value: got,
				}))
				assert.Equal(t, self.CacheableValue(), got)
			},
		},
		{
			name: "Delete",
			assert: func(t *testing.T) {
				require.NoError(t, cache.Delete(ctx, testKey))
				assert.True(t, keyNotExists(t, cache, wantKey))
			},
		},
		{
			name: "Once after Delete",
			assert: func(t *testing.T) {
				got := new(CacheableObject)
				require.NoError(t, cache.Once(&Item{
					Ctx:   ctx,
					Key:   testKey,
					Value: got,
					Do: func(*Item) (any, error) {
						return self.CacheableValue(), nil
					},
				}))
				assert.Equal(t, self.CacheableValue(), got)
			},
		},
		{
			name: "Once with Unmarshal error",
			assert: func(t *testing.T) {
				var got bool
				require.NoError(t, cache.Once(&Item{
					Ctx:   ctx,
					Key:   testKey,
					Value: &got,
					Do: func(*Item) (any, error) {
						return true, nil
					},
				}), "did Once() delete wrong cache key after Unmarshal() error?")
				assert.True(t, got)
			},
		},
	}

	for _, tt := range tests {
		self.T().Run(tt.name, tt.assert)
	}
}

func keyExists(t *testing.T, c *Cache, key string) bool {
	if c.localCache != nil {
		if c.localCache.Get(key) == nil {
			return false
		}
	}

	if c.redis != nil {
		bytesIter := valueNoError[func() ([]byte, bool)](t)(
			c.redis.Get(mgetIter3(context.Background(), []string{key})))
		b, _ := bytesIter()
		if b == nil {
			return false
		}
	}

	return true
}

func keyNotExists(t *testing.T, c *Cache, key string) bool {
	if c.localCache != nil {
		if c.localCache.Get(key) != nil {
			return false
		}
	}

	if c.redis != nil {
		bytesIter := valueNoError[func() ([]byte, bool)](t)(
			c.redis.Get(mgetIter3(context.Background(), []string{key})))
		b, _ := bytesIter()
		if b != nil {
			return false
		}
	}

	return true
}

// --------------------------------------------------

func NewRedisClient() (*redis.Client, error) {
	cfg := struct {
		WithRedis string `env:"WITH_REDIS"`
	}{
		WithRedis: "skip", // "redis://localhost:6379/1",
	}

	//nolint:wrapcheck
	err := dotenv.New().WithDepth(1).WithEnvSuffix("test").Load(func() error {
		return env.Parse(&cfg)
	})
	if err != nil {
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
		t.Logf("env WITH_REDIS: %q", os.Getenv("WITH_REDIS"))
		rdb = valueNoError[*redis.Client](t)(NewRedisClient())
	}

	tests := []struct {
		name       string
		needsLocal bool
		needsRedis bool
		subTests   func(rdb redis.Cmdable) []cacheSubTest
	}{
		{
			name:       "LocalCache and RedisCache",
			needsLocal: true,
			needsRedis: true,
			subTests: func(rdb redis.Cmdable) []cacheSubTest {
				return []cacheSubTest{subTestsWithRedis(rdb), subTestsWithStats}
			},
		},
		{
			name:       "RedisCache",
			needsRedis: true,
			subTests: func(rdb redis.Cmdable) []cacheSubTest {
				return []cacheSubTest{subTestsWithRedis(rdb), subTestsWithStats}
			},
		},
		{
			name:       "LocalCache",
			needsLocal: true,
			subTests: func(rdb redis.Cmdable) []cacheSubTest {
				return []cacheSubTest{subTestsWithStats}
			},
		},
	}

	cfgDB := clientDB(t, rdb)
	curDB := cfgDB

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			skipRedisTests(t, tt.name, tt.needsRedis, rdb != nil)
			var r redis.Cmdable
			if tt.needsRedis && rdb != nil {
				cn := rdb.Conn()
				t.Cleanup(func() {
					require.NoError(t, cn.Select(context.Background(), cfgDB).Err())
					require.NoError(t, cn.Close())
				})
				require.NoError(t, cn.Select(context.Background(), curDB).Err())
				curDB++
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
				name: "Classic",
				withRedis: func(c *Cache) *Cache {
					return c.WithRedis(rdb)
				},
			},
		}

		for _, tt := range tests {
			cfg := func(c *Cache) *Cache {
				return tt.withRedis(parentCfg(c))
			}
			if len(tests) > 1 {
				t.Run(tt.name, func(t *testing.T) {
					runCacheSubTests(t, cfg, suiteRun, subTests)
				})
			} else {
				runCacheSubTests(t, cfg, suiteRun, subTests)
			}
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
		if tt.withStats {
			t.Run(tt.name, func(t *testing.T) {
				runCacheSubTests(t, cfg, suiteRun, subTests)
			})
		} else {
			runCacheSubTests(t, cfg, suiteRun, subTests)
		}
	}
}

// --------------------------------------------------

func TestWithLocalCache(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.Nil(t, cache.localCache)

	localCache := local.NewTinyLFU(1000, time.Minute)
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

	redisCache := classic.New(nil)
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

func TestCache_New(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.NotSame(t, cache, cache.New())
}

func TestCache_WithKeyWrapper(t *testing.T) {
	const foobar = "foobar"
	tests := []struct {
		name   string
		expect string
		before func(c *Cache) *Cache
	}{
		{
			name:   "default",
			expect: foobar,
			before: func(c *Cache) *Cache {
				return c
			},
		},
		{
			name:   "1level",
			expect: "1level:" + foobar,
			before: func(c *Cache) *Cache {
				return c.New().WithKeyWrapper(func(key string) string {
					return "1level:" + c.WrapKey(key)
				})
			},
		},
		{
			name:   "2level",
			expect: "2level:1level:" + foobar,
			before: func(c *Cache) *Cache {
				return c.New().WithKeyWrapper(func(key string) string {
					return "2level:" + c.WrapKey(key)
				})
			},
		},
	}

	cache := New()
	require.NotNil(t, cache)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache = tt.before(cache)
			assert.Equal(t, tt.expect, cache.WrapKey(foobar))
		})
	}
}

func TestCache_WithNamespace(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	assert.Equal(t, "", cache.namespace)
	assert.Equal(t, cache.namespace, cache.Namespace())

	const abc = "abc"
	const foobar = "foobar"
	assert.Same(t, cache, cache.WithNamespace(foobar))
	assert.Equal(t, foobar, cache.Namespace())
	assert.Equal(t, abc, cache.WrapKey("abc"))
	assert.Equal(t, foobar+abc, cache.ResolveKey(abc))

	assert.Equal(t, "abc-"+foobar,
		cache.WithNamespace("test/").WithKeyWrapper(func(key string) string {
			return "abc-" + key
		}).WrapKey(foobar))
	assert.Equal(t, "test/abc-"+foobar, cache.ResolveKey(foobar))

	assert.Equal(t, "test/test2/abc-def-"+foobar,
		cache.New().WithNamespace(cache.Namespace()+"test2/").
			WithKeyWrapper(func(key string) string {
				return cache.WrapKey("def-" + key)
			}).ResolveKey(foobar))
}
