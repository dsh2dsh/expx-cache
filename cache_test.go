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

const (
	rdbOffset = 1
	testKey   = "mykey"
)

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

func (self *CacheTestSuite) CacheableValue() CacheableObject {
	return CacheableObject{
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

func (self *CacheTestSuite) TestCache_GetSet_nil() {
	ctx := context.Background()
	item := Item{Key: testKey}
	self.Require().NoError(self.cache.Set(ctx, item))

	self.Equal([]Item{item}, valueNoError[[]Item](self.T())(
		self.cache.Get(ctx, item)))
	self.cacheMiss()

	self.False(valueNoError[bool](self.T())(self.cache.Exists(ctx, item.Key)),
		"nil value shouldn't be cached")
	self.cacheMiss()

	self.assertStats()
}

func (self *CacheTestSuite) TestCache_GetSet_data() {
	ctx := context.Background()
	val := self.CacheableValue()
	item := Item{Key: testKey, Value: &val}
	self.Require().NoError(self.cache.Set(ctx, item))

	var gotValue CacheableObject
	item.Value = &gotValue
	self.Require().Empty(valueNoError[[]Item](self.T())(self.cache.Get(ctx, item)))
	self.cacheHit()
	self.Equal(&val, &gotValue)

	self.True(self.cache.Exists(ctx, testKey))
	self.cacheHit()
	self.assertStats()
}

func (self *CacheTestSuite) TestCache_GetSet_stringAsIs() {
	ctx := context.Background()
	value := "str_value"
	item := Item{Key: testKey, Value: value}
	self.Require().NoError(self.cache.Set(ctx, item))

	var gotValue string
	item.Value = &gotValue
	self.Require().Empty(valueNoError[[]Item](self.T())(self.cache.Get(ctx, item)))
	self.cacheHit()
	self.Equal(value, gotValue)
	self.assertStats()
}

func (self *CacheTestSuite) TestCache_GetSet_bytesAsIs() {
	ctx := context.Background()
	value := []byte("str_value")
	item := Item{Key: testKey, Value: value}
	self.Require().NoError(self.cache.Set(ctx, item))

	var gotValue []byte
	item.Value = &gotValue
	self.Require().Empty(valueNoError[[]Item](self.T())(self.cache.Get(ctx, item)))
	self.cacheHit()
	self.Equal(value, gotValue)
	self.assertStats()
}

func (self *CacheTestSuite) TestCache_setGetItems() {
	const maxItems = 21
	allKeys := make([]string, maxItems)
	allValues := make([]CacheableObject, maxItems)
	allItems := make([]Item, maxItems)

	for i := 0; i < maxItems; i++ {
		key := fmt.Sprintf("key-%00d", i)
		allKeys[i] = key
		allValues[i] = CacheableObject{Str: key, Num: i}
		allItems[i] = Item{Key: key, Value: &allValues[i]}
	}

	ctx := context.Background()
	self.Require().NoError(self.cache.Set(ctx, allItems...))

	gotValues := make([]CacheableObject, maxItems)
	for i := range allItems {
		item := &allItems[i]
		item.Value = &gotValues[i]
		self.cacheHit()
	}
	missed := valueNoError[[]Item](self.T())(self.cache.Get(ctx, allItems...))
	for range missed {
		self.cacheMiss()
	}
	self.Equal(allValues, gotValues)
	self.Empty(missed)

	self.Require().NoError(self.cache.Delete(ctx, allKeys...))

	clear(gotValues)
	expectedValues := make([]CacheableObject, maxItems)
	missed = valueNoError[[]Item](self.T())(self.cache.Get(ctx, allItems...))
	for range missed {
		self.cacheMiss()
	}
	self.Equal(expectedValues, gotValues)
	self.Equal(allItems, missed)

	self.assertStats()
}

func (self *CacheTestSuite) TestCache_GetSet() {
	if testing.Short() {
		self.T().Skip("skipping in short mode")
	}

	maxItems := 21
	allKeys := make([]string, maxItems)
	allValues := make([]CacheableObject, maxItems)

	allItems := make([]Item, maxItems)
	var callCount uint64
	for i := 0; i < maxItems; i++ {
		i := i
		key := fmt.Sprintf("key-%00d", i)
		allKeys[i] = key
		allValues[i] = CacheableObject{Str: key, Num: i}
		allItems[i] = Item{
			Key:   key,
			Value: &allValues[i],
			Do: func(ctx context.Context) (any, error) {
				atomic.AddUint64(&callCount, 1)
				return &allValues[i], nil
			},
		}
	}

	ctx := context.Background()
	self.Require().NoError(self.cache.GetSet(ctx, allItems...))
	self.Equal(uint64(maxItems), callCount)

	callCount = 0
	gotValues := make([]CacheableObject, maxItems)
	for i := range allItems {
		item := &allItems[i]
		item.Value = &gotValues[i]
		self.cacheMiss()
		self.cacheHit()
	}
	self.Require().NoError(self.cache.GetSet(ctx, allItems...))
	self.Equal(uint64(0), callCount)
	self.Equal(allValues, gotValues)

	self.assertStats()
}

// --------------------------------------------------

func (self *CacheTestSuite) TestCache_WithKeyWrapper() {
	const keyPrefix = "baz:"
	wantKey := keyPrefix + testKey

	cache := self.cache.New().WithKeyWrapper(func(key string) string {
		return keyPrefix + self.cache.WrapKey(key)
	})

	ctx := context.Background()
	self.Require().NoError(cache.Set(ctx, Item{
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
			name: "Get 1",
			assert: func(t *testing.T) {
				got := CacheableObject{}
				item := Item{Key: testKey, Value: &got}
				assert.Empty(t, valueNoError[[]Item](t)(cache.Get(ctx, item)))
				assert.Equal(t, self.CacheableValue(), got)
			},
		},
		{
			name: "Get 2",
			assert: func(t *testing.T) {
				got1 := CacheableObject{}
				got2 := CacheableObject{}
				item1 := Item{Key: testKey, Value: &got1}
				item2 := Item{Key: testKey, Value: &got2}
				assert.Empty(t, valueNoError[[]Item](t)(cache.Get(ctx, item1, item2)))
				assert.Equal(t, self.CacheableValue(), got1)
				assert.Equal(t, self.CacheableValue(), got2)
			},
		},
		{
			name: "Once",
			assert: func(t *testing.T) {
				got := CacheableObject{}
				require.NoError(t, cache.Once(ctx, Item{
					Key:   testKey,
					Value: &got,
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
				got := CacheableObject{}
				require.NoError(t, cache.Once(ctx, Item{
					Key:   testKey,
					Value: &got,
					Do: func(ctx context.Context) (any, error) {
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
				require.NoError(t, cache.Once(ctx, Item{
					Key:   testKey,
					Value: &got,
					Do: func(ctx context.Context) (any, error) {
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

	err := dotenv.Load(func() error { return env.Parse(&cfg) })
	if err != nil {
		return nil, fmt.Errorf("load .env: %w", err)
	} else if cfg.WithRedis == "skip" {
		return nil, nil
	}

	opt, err := redis.ParseURL(cfg.WithRedis)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL %q: %w", cfg.WithRedis, err)
	}
	opt.DB += rdbOffset

	rdb := redis.NewClient(opt)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %q: %w", cfg.WithRedis, err)
	}

	return rdb, nil
}

type cacheSubTest func(t *testing.T, cfg func(*testing.T, *Cache) *Cache,
	suiteRun cacheSubTest, subTests []cacheSubTest)

func TestCacheSuite(t *testing.T) {
	t.Parallel()

	var rdb *redis.Client
	if !testing.Short() {
		t.Logf("env WITH_REDIS: %q", os.Getenv("WITH_REDIS"))
		rdb = valueNoError[*redis.Client](t)(NewRedisClient())
		if rdb != nil {
			t.Cleanup(func() { require.NoError(t, rdb.Close()) })
		}
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
				return []cacheSubTest{withRedis(rdb), withStats}
			},
		},
		{
			name:       "RedisCache",
			needsRedis: true,
			subTests: func(rdb redis.Cmdable) []cacheSubTest {
				return []cacheSubTest{withRedis(rdb), withStats}
			},
		},
		{
			name:       "LocalCache",
			needsLocal: true,
			subTests: func(rdb redis.Cmdable) []cacheSubTest {
				return []cacheSubTest{withStats}
			},
		},
	}

	cfgDB := clientDB(t, rdb)
	ctx := context.Background()

	for i, tt := range tests {
		i, tt := i, tt
		t.Run(tt.name, func(t *testing.T) {
			skipRedisTests(t, tt.name, tt.needsRedis, rdb != nil)
			t.Parallel()
			nextDB := cfgDB + i
			var r redis.Cmdable
			if tt.needsRedis && rdb != nil {
				cn := rdb.Conn()
				t.Cleanup(func() {
					require.NoError(t, cn.Select(ctx, cfgDB).Err())
					require.NoError(t, cn.Close())
				})
				t.Logf("use redis DB %v", nextDB)
				require.NoError(t, cn.Select(ctx, nextDB).Err())
				r = cn
			}
			cfg := func(t *testing.T, c *Cache) *Cache {
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
		info := valueNoError[*redis.ClientInfo](t)(rdb.ClientInfo(
			context.Background()).Result())
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

func suiteRunSubTests(t *testing.T, rdb redis.Cmdable,
	cfg func(*testing.T, *Cache) *Cache, subTests []cacheSubTest,
) {
	runCacheSubTests(t, cfg,
		func(t *testing.T, cfg func(*testing.T, *Cache) *Cache, _ cacheSubTest,
			subTests []cacheSubTest,
		) {
			suite.Run(t, &CacheTestSuite{
				rdb: rdb,
				newCache: func() *Cache {
					return cfg(t, New())
				},
			})
		}, subTests)
}

func runCacheSubTests(t *testing.T, cfg func(*testing.T, *Cache) *Cache,
	suiteRun cacheSubTest, subTests []cacheSubTest,
) {
	if len(subTests) > 0 {
		subTests[0](t, cfg, suiteRun, subTests[1:])
	} else {
		suiteRun(t, cfg, nil, nil)
	}
}

func withRedis(rdb redis.Cmdable) cacheSubTest {
	return func(t *testing.T, parentCfg func(*testing.T, *Cache) *Cache,
		suiteRun cacheSubTest, subTests []cacheSubTest,
	) {
		cfg := func(t *testing.T, c *Cache) *Cache {
			return parentCfg(t, c).WithRedis(rdb)
		}
		runCacheSubTests(t, cfg, suiteRun, subTests)
	}
}

func withStats(t *testing.T, parentCfg func(*testing.T, *Cache) *Cache,
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
		cfg := func(t *testing.T, c *Cache) *Cache {
			return parentCfg(t, c).WithStats(tt.withStats)
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
			wantTTL: time.Millisecond,
		},
		{
			name:    "negative",
			TTL:     -time.Second,
			wantTTL: cache.DefaultTTL(),
		},
		{
			name:    "zero",
			TTL:     0,
			wantTTL: cache.DefaultTTL(),
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
			item := Item{TTL: tt.TTL}
			assert.Equal(t, tt.wantTTL, item.ttl(cache.DefaultTTL()))
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
