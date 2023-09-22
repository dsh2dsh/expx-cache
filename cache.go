//go:generate mockery
package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const defaultTTL = time.Hour

type LocalCache interface {
	Set(key string, data []byte)
	Get(key string) []byte
	Del(key string)
}

type RedisClient interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Del(ctx context.Context, keys ...string) error
	Set(ctx context.Context, key string, value any, ttl time.Duration) error

	MGet(ctx context.Context, keys ...string) ([][]byte, error)
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

		stats: new(Stats),
		group: &singleflight.Group{},
	}

	return c
}

type Cache struct {
	redis      RedisClient
	localCache LocalCache

	defaultTTL time.Duration
	marshal    MarshalFunc
	unmarshal  UnmarshalFunc
	keyWrapper func(keys string) string

	stats        *Stats
	statsEnabled bool

	group *singleflight.Group
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

func (self *Cache) WithKeyWrapper(fn func(key string) string) *Cache {
	self.keyWrapper = fn
	return self
}

func (self *Cache) WrapKey(key string) string {
	if self.keyWrapper != nil {
		return self.keyWrapper(key)
	}
	return key
}
