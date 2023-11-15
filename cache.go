//go:generate mockery
package cache

import (
	"context"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

const defaultTTL = time.Hour

type LocalCache interface {
	Set(key string, data []byte)
	Get(key string) []byte
	Del(key string)
}

type RedisCache interface {
	Del(ctx context.Context, keys ...string) error
	Get(ctx context.Context, maxItems int,
		keyIter func(itemIdx int) string) (func() ([]byte, bool), error)
	MSet(ctx context.Context, maxItems int,
		iter func(itemIdx int) (key string, b []byte, ttl time.Duration)) error
}

type (
	MarshalFunc   func(any) ([]byte, error)
	UnmarshalFunc func([]byte, any) error
)

func New() *Cache {
	c := &Cache{
		defaultTTL: defaultTTL,
		marshal:    marshal,
		unmarshal:  unmarshal,

		stats:   new(Stats),
		group:   &singleflight.Group{},
		workers: semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0))),
	}

	return c
}

type Cache struct {
	redis      RedisCache
	localCache LocalCache

	defaultTTL time.Duration
	namespace  string

	marshal    MarshalFunc
	unmarshal  UnmarshalFunc
	keyWrapper func(keys string) string

	stats        *Stats
	statsEnabled bool

	group   *singleflight.Group
	workers *semaphore.Weighted
}

func (self *Cache) New() *Cache {
	c := *self
	return &c
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

func (self *Cache) WithRedisCache(client RedisCache) *Cache {
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

func (self *Cache) WithDefaultTTL(ttl time.Duration) *Cache {
	self.defaultTTL = ttl
	return self
}

// --------------------------------------------------

func (self *Cache) Marshal(value any) ([]byte, error) {
	return self.marshal(value)
}

func (self *Cache) Unmarshal(b []byte, value any) error {
	return self.unmarshal(b, value)
}

func (self *Cache) DefaultTTL() time.Duration {
	return self.defaultTTL
}

func (self *Cache) ItemTTL(item *Item) time.Duration {
	switch {
	case item.TTL < 0:
		return 0
	case item.TTL == 0:
		return self.DefaultTTL()
	case item.TTL < time.Second:
		return time.Second
	}
	return item.TTL
}

// --------------------------------------------------

func (self *Cache) WithNamespace(namespace string) *Cache {
	self.namespace = namespace
	return self
}

func (self *Cache) Namespace() string {
	return self.namespace
}

func (self *Cache) WithKeyWrapper(fn func(key string) string) *Cache {
	self.keyWrapper = fn
	return self
}

func (self *Cache) WrapKey(key string) string {
	if self.keyWrapper != nil {
		key = self.keyWrapper(key)
	}
	return key
}

func (self *Cache) ResolveKey(key string) string {
	return self.Namespace() + self.WrapKey(key)
}

// --------------------------------------------------

func (self *Cache) Multi() *MultiCache {
	return NewMultiCache(self)
}

func (self *Cache) MGet(ctx context.Context, items []*Item) ([]*Item, error) {
	return self.Multi().Get(ctx, items)
}

func (self *Cache) MSet(ctx context.Context, items []*Item) error {
	return self.Multi().Set(ctx, items)
}

func (self *Cache) MGetSet(ctx context.Context, items []*Item) error {
	return self.Multi().GetSet(ctx, items)
}
