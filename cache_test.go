package cache

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"slices"
	"strconv"
	"strings"
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
	cacheRedis "github.com/dsh2dsh/expx-cache/redis"
)

const (
	rdbOffset = 1
	testKey   = "test-key"
)

func valueNoError[V any](t *testing.T) func(val V, err error) V {
	return func(val V, err error) V {
		require.NoError(t, err)
		return val
	}
}

func makeBytesIter(blobs [][]byte, err error) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		if err != nil {
			yield(nil, err)
			return
		}
		for _, b := range blobs {
			if !yield(b, nil) {
				return
			}
		}
	}
}

type CacheableObject struct {
	Str string
	Num int
}

type CacheTestSuite struct {
	suite.Suite

	cache *Cache
	rdb   cacheRedis.Cmdable
	stats *Stats

	newCache func(opts ...Option) *Cache
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

func (self *CacheTestSuite) expectCacheHit() {
	if self.cache.localCache != nil {
		self.stats.localHit()
	} else if self.cache.redis != nil {
		self.stats.hit()
	}
}

func (self *CacheTestSuite) expectCacheMiss() {
	if self.cache.localCache != nil {
		self.stats.localMiss()
	}
	if self.cache.redis != nil {
		self.stats.miss()
	}
}

func (self *CacheTestSuite) expectCacheHitLocalMiss() {
	self.stats.hit()
	if self.cache.localCache != nil {
		self.stats.localMiss()
	}
}

func (self *CacheTestSuite) assertStats() {
	s := self.cache.Stats()
	self.Equal(self.stats, &s, "unexpected cache Stats")
}

// --------------------------------------------------

func (self *CacheTestSuite) needsRedis() {
	if self.rdb == nil {
		self.T().Skipf("skip %q, because no Redis connection", self.T().Name())
	}
}

func (self *CacheTestSuite) TestCache_GetSet_nil() {
	ctx := context.Background()
	item := Item{Key: testKey}
	self.Require().NoError(self.cache.Set(ctx, item))

	self.Equal([]Item{item}, valueNoError[[]Item](self.T())(
		self.cache.Get(ctx, item)))
	self.expectCacheMiss()

	self.False(valueNoError[bool](self.T())(self.cache.Exists(ctx, item.Key)),
		"nil value shouldn't be cached")
	self.expectCacheMiss()

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
	self.expectCacheHit()
	self.Equal(&val, &gotValue)

	self.True(self.cache.Exists(ctx, testKey))
	self.expectCacheHit()
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
	self.expectCacheHit()
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
	self.expectCacheHit()
	self.Equal(value, gotValue)
	self.assertStats()
}

func (self *CacheTestSuite) TestCache_setGetItems() {
	const maxItems = 21
	var allKeys [maxItems]string
	var allValues [maxItems]CacheableObject
	var allItems [maxItems]Item

	for i := range maxItems {
		key := fmt.Sprintf("key-%00d", i)
		allKeys[i] = key
		allValues[i] = CacheableObject{Str: key, Num: i}
		allItems[i] = Item{Key: key, Value: &allValues[i]}
	}

	ctx := context.Background()
	self.Require().NoError(self.cache.Set(ctx, allItems[:]...))

	var gotValues [maxItems]CacheableObject
	for i := range allItems {
		item := &allItems[i]
		item.Value = &gotValues[i]
		self.expectCacheHit()
	}
	missed := valueNoError[[]Item](self.T())(self.cache.Get(ctx, allItems[:]...))
	for range missed {
		self.expectCacheMiss()
	}
	self.Equal(allValues, gotValues)
	self.Empty(missed)

	self.Require().NoError(self.cache.Delete(ctx, allKeys[:]...))

	clear(gotValues[:])
	var expectedValues [maxItems]CacheableObject
	missed = valueNoError[[]Item](self.T())(self.cache.Get(ctx, allItems[:]...))
	for range missed {
		self.expectCacheMiss()
	}
	self.Equal(expectedValues, gotValues)
	self.Equal(allItems[:], missed)

	self.assertStats()
}

func (self *CacheTestSuite) TestCache_GetSet() {
	const maxItems = 21
	var allValues [maxItems]CacheableObject
	var gotValues [maxItems]CacheableObject

	var allItems [maxItems]Item
	var callCount uint32
	for i := range maxItems {
		key := fmt.Sprintf("key-%00d", i)
		allValues[i] = CacheableObject{Str: key, Num: i}
		allItems[i] = Item{
			Key:   key,
			Value: &gotValues[i],
			Do: func(ctx context.Context) (any, error) {
				atomic.AddUint32(&callCount, 1)
				return &allValues[i], nil
			},
		}
		self.expectCacheMiss()
	}

	ctx := context.Background()
	self.Require().NoError(self.cache.GetSet(ctx, allItems[:]...))
	self.Equal(uint32(maxItems), callCount)
	self.Equal(allValues, gotValues)

	clear(gotValues[:])
	for range allItems {
		self.expectCacheHit()
	}
	callCount = 0
	self.Require().NoError(self.cache.GetSet(ctx, allItems[:]...))
	self.Equal(uint32(0), callCount)
	self.Equal(allValues, gotValues)

	self.assertStats()
}

// --------------------------------------------------

func (self *CacheTestSuite) TestCache_WithKeyWrapper() {
	const keyPrefix = "baz:"
	wantKey := self.cache.ResolveKey(keyPrefix + testKey)
	self.T().Log(wantKey)

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
		assert func()
	}{
		{
			name: "Exists",
			assert: func() {
				self.True(valueNoError[bool](self.T())(cache.Exists(ctx, testKey)))
			},
		},
		{
			name: "Get 1",
			assert: func() {
				got := CacheableObject{}
				item := Item{Key: testKey, Value: &got}
				self.Empty(valueNoError[[]Item](self.T())(cache.Get(ctx, item)))
				self.Equal(self.CacheableValue(), got)
			},
		},
		{
			name: "Get 2",
			assert: func() {
				got1 := CacheableObject{}
				got2 := CacheableObject{}
				item1 := Item{Key: testKey, Value: &got1}
				item2 := Item{Key: testKey, Value: &got2}
				self.Empty(valueNoError[[]Item](self.T())(cache.Get(ctx, item1, item2)))
				self.Equal(self.CacheableValue(), got1)
				self.Equal(self.CacheableValue(), got2)
			},
		},
		{
			name: "Once",
			assert: func() {
				got := CacheableObject{}
				self.Require().NoError(cache.Once(ctx, Item{
					Key:   testKey,
					Value: &got,
				}))
				self.Equal(self.CacheableValue(), got)
			},
		},
		{
			name: "Delete",
			assert: func() {
				self.Require().NoError(cache.Delete(ctx, testKey))
				self.True(keyNotExists(self.T(), cache, wantKey))
			},
		},
		{
			name: "Once after Delete",
			assert: func() {
				got := CacheableObject{}
				self.Require().NoError(cache.Once(ctx, Item{
					Key:   testKey,
					Value: &got,
					Do: func(ctx context.Context) (any, error) {
						return self.CacheableValue(), nil
					},
				}))
				self.Equal(self.CacheableValue(), got)
			},
		},
	}

	for _, tt := range tests {
		self.Run(tt.name, tt.assert)
	}
}

func keyExists(t *testing.T, c *Cache, key string) bool {
	if c.localCache != nil {
		if c.localCache.Get(key) == nil {
			return false
		}
	}

	if c.redis != nil {
		bytesIter := c.redis.Get(t.Context(), 1, slices.Values([]string{key}))
		for b, err := range bytesIter {
			require.NoError(t, err)
			return b != nil
		}
		return false
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
		bytesIter := c.redis.Get(t.Context(), 1, slices.Values([]string{key}))
		for b, err := range bytesIter {
			require.NoError(t, err)
			return b == nil
		}
		return true
	}
	return true
}

// --------------------------------------------------

func NewRedisClient(db int) (*redis.Client, error) {
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
	opt.DB += rdbOffset + db

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

	tests := []struct {
		name       string
		needsLocal bool
		needsRedis bool
		subTests   func(rdb cacheRedis.Cmdable) []cacheSubTest
	}{
		{
			name:       "LocalCache and RedisCache",
			needsLocal: true,
			needsRedis: true,
			subTests: func(rdb cacheRedis.Cmdable) []cacheSubTest {
				return []cacheSubTest{withRedis(rdb)}
			},
		},
		{
			name:       "RedisCache",
			needsRedis: true,
			subTests: func(rdb cacheRedis.Cmdable) []cacheSubTest {
				return []cacheSubTest{withRedis(rdb)}
			},
		},
		{
			name:       "LocalCache",
			needsLocal: true,
			subTests:   func(rdb cacheRedis.Cmdable) []cacheSubTest { return nil },
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var rdbCmdable cacheRedis.Cmdable
			if tt.needsRedis {
				rdb := valueNoError[*redis.Client](t)(NewRedisClient(i))
				t.Logf("env WITH_REDIS: %q", os.Getenv("WITH_REDIS"))
				if rdb != nil {
					t.Cleanup(func() { require.NoError(t, rdb.Close()) })
					rdbCmdable = rdb
				} else {
					t.Skipf("skip %q, because no Redis connection", tt.name)
				}
			}
			cacheNamespace := "expx-cache-test-" + strconv.Itoa(i) + ":"
			t.Logf("cacheNamespace = %q", cacheNamespace)
			cfg := func(t *testing.T, c *Cache) *Cache {
				if tt.needsLocal {
					c.WithTinyLFU(1000, time.Minute)
				}
				return c.WithNamespace(cacheNamespace)
			}
			suiteRunSubTests(t, rdbCmdable, cfg, tt.subTests(rdbCmdable))
		})
	}
}

func suiteRunSubTests(t *testing.T, rdb cacheRedis.Cmdable,
	cfg func(*testing.T, *Cache) *Cache, subTests []cacheSubTest,
) {
	runCacheSubTests(t, cfg,
		func(t *testing.T, cfg func(*testing.T, *Cache) *Cache, _ cacheSubTest,
			subTests []cacheSubTest,
		) {
			suite.Run(t, &CacheTestSuite{
				rdb: rdb,
				newCache: func(opts ...Option) *Cache {
					return cfg(t, New(opts...))
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

func withRedis(rdb cacheRedis.Cmdable) cacheSubTest {
	return func(t *testing.T, parentCfg func(*testing.T, *Cache) *Cache,
		suiteRun cacheSubTest, subTests []cacheSubTest,
	) {
		cfg := func(t *testing.T, c *Cache) *Cache {
			return parentCfg(t, c).WithRedis(rdb)
		}
		runCacheSubTests(t, cfg, suiteRun, subTests)
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

	redisCache := cacheRedis.New(nil)
	assert.Same(t, cache, cache.WithRedisCache(redisCache))
	assert.NotNil(t, cache.redis)
	assert.Same(t, cache.redis, redisCache)

	assert.Same(t, cache, cache.WithRedisCache(nil))
	assert.Nil(t, cache.redis)
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

	assert.Empty(t, cache.namespace)
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

func TestCache_WithLocalStats(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	stats, err := cache.WithLocalStats(func(c *Cache) error {
		c.addHit()
		c.addLocalMiss()
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, Stats{Hits: 1, LocalMisses: 1}, stats)

	wantErr := errors.New("test error")
	_, err = cache.WithLocalStats(func(c *Cache) error {
		c.addHit()
		c.addLocalMiss()
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
}

func TestCache_WithRequestId(t *testing.T) {
	const foobar = "foobar"
	c := New().WithRequestId(foobar)
	assert.Equal(t, foobar, c.requestId)
}

func TestCache_WithPrefixLock(t *testing.T) {
	c := New()
	const foobar = "foobar"
	require.Same(t, c, c.WithPrefixLock(foobar))
	assert.Equal(t, foobar, c.prefixLock)
	assert.True(t, strings.HasPrefix(c.ResolveKeyLock(testKey), foobar))
}
