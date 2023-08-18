//go:generate mockery
package cache

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/singleflight"
)

const (
	defaultTTL = time.Hour

	compressionThreshold = 64
	noCompression        = 0x0
	s2Compression        = 0x1
)

var (
	ErrCacheMiss          = errors.New("cache: key is missing")
	errRedisLocalCacheNil = errors.New("cache: both Redis and LocalCache are nil")
)

type LocalCache interface {
	Set(key string, data []byte)
	Get(key string) []byte
	Del(key string)
}

type RedisClient interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Del(ctx context.Context, keys ...string) error

	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) error
	SetXX(ctx context.Context, key string, value any, ttl time.Duration) error
}

type (
	MarshalFunc   func(any) ([]byte, error)
	UnmarshalFunc func([]byte, any) error
)

// --------------------------------------------------

type Item struct {
	Ctx context.Context

	Key   string
	Value any

	// TTL is the cache expiration time.
	// Default TTL is 1 hour.
	TTL time.Duration

	// Do returns value to be cached.
	Do func(*Item) (any, error)

	// SetXX only sets the key if it already exists.
	SetXX bool

	// SetNX only sets the key if it does not already exist.
	SetNX bool

	// SkipLocalCache skips local cache as if it is not set.
	SkipLocalCache bool
}

func (self *Item) Context() context.Context {
	if self.Ctx == nil {
		return context.Background()
	}
	return self.Ctx
}

func (self *Item) value() (any, error) {
	if self.Do != nil {
		return self.Do(self)
	}
	return self.Value, nil
}

func (self *Item) ttl() time.Duration {
	switch {
	case self.TTL < 0:
		return 0
	case self.TTL == 0:
		return defaultTTL
	case self.TTL < time.Second:
		return time.Second
	}
	return self.TTL
}

// --------------------------------------------------

func New() *Cache {
	c := &Cache{
		marshal:   marshal,
		unmarshal: unmarshal,
	}

	return c
}

type Cache struct {
	redis      RedisClient
	localCache LocalCache

	marshal   MarshalFunc
	unmarshal UnmarshalFunc

	statsEnabled bool
	hits         uint64
	misses       uint64
	localHits    uint64
	localMisses  uint64

	group singleflight.Group
}

func (self *Cache) WithLocalCache(client LocalCache) *Cache {
	self.localCache = client
	return self
}

func (self *Cache) WithMarshal(fn MarshalFunc) *Cache {
	self.marshal = fn
	return self
}

func (self *Cache) WithRedis(rdb redis.Cmdable) *Cache {
	if rdb != nil {
		self.redis = NewStdRedis(rdb)
	} else {
		self.redis = nil
	}
	return self
}

func (self *Cache) WithRedisCache(client RedisClient) *Cache {
	self.redis = client
	return self
}

func (self *Cache) WithStats(val bool) *Cache {
	self.statsEnabled = val
	return self
}

func (self *Cache) WithTinyLFU(size int, ttl time.Duration) *Cache {
	self.localCache = NewTinyLFU(size, ttl)
	return self
}

func (self *Cache) WithUnmarshal(fn UnmarshalFunc) *Cache {
	self.unmarshal = fn
	return self
}

// --------------------------------------------------

func (self *Cache) Delete(ctx context.Context, key string) error {
	if self.localCache != nil {
		self.localCache.Del(key)
	}

	if self.redis == nil {
		if self.localCache == nil {
			return errRedisLocalCacheNil
		}
		return nil
	}

	if err := self.redis.Del(ctx, key); err != nil {
		return fmt.Errorf("delete %q from redis: %w", key, err)
	}

	return nil
}

func (self *Cache) DeleteFromLocalCache(key string) {
	if self.localCache != nil {
		self.localCache.Del(key)
	}
}

// --------------------------------------------------

// Exists reports whether value for the given key exists.
func (self *Cache) Exists(ctx context.Context, key string) bool {
	_, err := self.getBytes(ctx, key, false)
	return err == nil
}

// Get gets the value for the given key.
func (self *Cache) Get(ctx context.Context, key string, value any) error {
	return self.get(ctx, key, value, false)
}

// Get gets the value for the given key skipping local cache.
func (self *Cache) GetSkippingLocalCache(
	ctx context.Context, key string, value any,
) error {
	return self.get(ctx, key, value, true)
}

func (self *Cache) get(
	ctx context.Context,
	key string,
	value any,
	skipLocalCache bool,
) error {
	b, err := self.getBytes(ctx, key, skipLocalCache)
	if err != nil {
		return err
	}
	return self.unmarshal(b, value)
}

func (self *Cache) getBytes(
	ctx context.Context, key string, skipLocalCache bool,
) ([]byte, error) {
	if !skipLocalCache && self.localCache != nil {
		b := self.localCache.Get(key)
		if b != nil {
			self.addLocalHit()
			return b, nil
		}
		self.addLocalMiss()
	}

	if self.redis == nil {
		if self.localCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return nil, ErrCacheMiss
	}

	b, err := self.redis.Get(ctx, key)
	if err != nil {
		self.addMiss()
		return nil, fmt.Errorf("get %q from redis: %w", key, err)
	} else if b == nil {
		self.addMiss()
		return nil, ErrCacheMiss
	}

	self.addHit()

	if !skipLocalCache && self.localCache != nil {
		self.localCache.Set(key, b)
	}
	return b, nil
}

// --------------------------------------------------

func (self *Cache) Marshal(value any) ([]byte, error) {
	return self.marshal(value)
}

func (self *Cache) Unmarshal(b []byte, value any) error {
	return self.unmarshal(b, value)
}

func marshal(value any) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	}

	b, err := msgpack.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal: msgpack error: %w", err)
	}

	return compress(b), nil
}

func unmarshal(b []byte, value any) error {
	if len(b) == 0 {
		return nil
	}

	switch value := value.(type) {
	case nil:
		return nil
	case *[]byte:
		clone := make([]byte, len(b))
		copy(clone, b)
		*value = clone
		return nil
	case *string:
		*value = string(b)
		return nil
	}

	switch c := b[len(b)-1]; c {
	case noCompression:
		b = b[:len(b)-1]
	case s2Compression:
		b = b[:len(b)-1]
		if decoded, err := s2.Decode(nil, b); err != nil {
			return fmt.Errorf("unmarshal: decompress error: %w", err)
		} else {
			b = decoded
		}
	default:
		return fmt.Errorf("unmarshal: unknown compression method: %x", c)
	}

	if err := msgpack.Unmarshal(b, value); err != nil {
		return fmt.Errorf("unmarshal msgpack error: %w", err)
	}

	return nil
}

func compress(data []byte) []byte {
	if len(data) < compressionThreshold {
		b := make([]byte, len(data)+1)
		copy(b, data)
		b = b[:len(data)]               // make a sub-slice for safe append
		return append(b, noCompression) //nolint:makezero // b is a sub-slice
	}

	b := make([]byte, s2.MaxEncodedLen(len(data))+1)
	b = s2.Encode(b, data)
	return append(b, s2Compression) //nolint:makezero // b is a sub-slice
}

// --------------------------------------------------

// Once gets the item.Value for the given item.Key from the cache or executes,
// caches, and returns the results of the given item.Func, making sure that only
// one execution is in-flight for a given item.Key at a time. If a duplicate
// comes in, the duplicate caller waits for the original to complete and
// receives the same results.
func (self *Cache) Once(item *Item) error {
	b, fromCache, err := self.getSetItemBytesOnce(item)
	if err != nil {
		return err
	} else if len(b) == 0 {
		return nil
	}

	if err := self.Unmarshal(b, item.Value); err != nil {
		if fromCache {
			if err := self.Delete(item.Context(), item.Key); err != nil {
				return err
			}
			return self.Once(item)
		}
		return err
	}

	return nil
}

func (self *Cache) getSetItemBytesOnce(item *Item) ([]byte, bool, error) {
	if self.localCache != nil {
		b := self.localCache.Get(item.Key)
		if b != nil {
			return b, true, nil
		}
	}

	fromCache := false
	v, err, _ := self.group.Do(item.Key, func() (any, error) {
		b, err := self.getBytes(item.Context(), item.Key, item.SkipLocalCache)
		if err == nil {
			fromCache = true
			return b, nil
		}

		b, err = self.set(item)
		if err == nil || err == errRedisLocalCacheNil { //nolint:errorlint // local err
			return b, nil
		}
		return nil, err
	})
	if err != nil {
		return nil, false, err //nolint:wrapcheck // err from our getBytes
	}

	return v.([]byte), fromCache, nil
}

// --------------------------------------------------

// Set caches the item.
func (self *Cache) Set(item *Item) error {
	_, err := self.set(item)
	return err
}

func (self *Cache) set(item *Item) ([]byte, error) {
	value, err := item.value()
	if err != nil {
		return nil, err
	} else if value == nil {
		return nil, nil
	}

	b, err := self.Marshal(value)
	if err != nil {
		return nil, err
	}

	if self.localCache != nil && !item.SkipLocalCache {
		self.localCache.Set(item.Key, b)
	}

	if self.redis == nil {
		if self.localCache == nil {
			return b, errRedisLocalCacheNil
		}
		return b, nil
	}

	ttl := item.ttl()
	if ttl == 0 {
		return b, nil
	}

	setFn := self.redis.Set
	switch {
	case item.SetXX:
		setFn = self.redis.SetXX
	case item.SetNX:
		setFn = self.redis.SetNX
	}

	if err := setFn(item.Context(), item.Key, b, ttl); err != nil {
		return nil, fmt.Errorf("cache set %q with ttl %v: %w", item.Key, ttl, err)
	}

	return b, nil
}

// --------------------------------------------------

// Stats returns cache statistics.
func (self *Cache) Stats() *Stats {
	if !self.statsEnabled {
		return nil
	}
	return &Stats{
		Hits:        atomic.LoadUint64(&self.hits),
		Misses:      atomic.LoadUint64(&self.misses),
		LocalHits:   atomic.LoadUint64(&self.localHits),
		LocalMisses: atomic.LoadUint64(&self.localMisses),
	}
}

type Stats struct {
	Hits        uint64
	Misses      uint64
	LocalHits   uint64
	LocalMisses uint64
}

func (self *Cache) addLocalHit() {
	if self.statsEnabled {
		atomic.AddUint64(&self.localHits, 1)
	}
}

func (self *Cache) addLocalMiss() {
	if self.statsEnabled {
		atomic.AddUint64(&self.localMisses, 1)
	}
}

func (self *Cache) addHit() {
	if self.statsEnabled {
		atomic.AddUint64(&self.hits, 1)
	}
}

func (self *Cache) addMiss() {
	if self.statsEnabled {
		atomic.AddUint64(&self.misses, 1)
	}
}
