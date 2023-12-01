//go:generate mockery
package cache

import (
	"context"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"

	"github.com/dsh2dsh/expx-cache/local"
	"github.com/dsh2dsh/expx-cache/redis/classic"
)

const defaultTTL = time.Hour

type LocalCache interface {
	Set(key string, data []byte)
	Get(key string) []byte
	Del(key string)
}

type RedisCache interface {
	Del(ctx context.Context, keys []string) error
	Get(ctx context.Context, maxItems int,
		keyIter func(itemIdx int) string) (func() ([]byte, bool), error)
	Set(ctx context.Context, maxItems int,
		iter func(itemIdx int) (key string, b []byte, ttl time.Duration)) error
}

type (
	Option        func(c *Cache)
	MarshalFunc   func(any) ([]byte, error)
	UnmarshalFunc func([]byte, any) error
)

func New(opts ...Option) *Cache {
	c := &Cache{
		defaultTTL: defaultTTL,
		marshal:    marshal,
		unmarshal:  unmarshal,

		stats: new(Stats),
		group: new(singleflight.Group),
	}
	return c.applyOptions(opts...)
}

func WithMarshalMaxProcs(n int) Option {
	return func(c *Cache) {
		if n < 1 {
			c.marshalers = semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0)))
		} else {
			c.marshalers = semaphore.NewWeighted(int64(n))
		}
	}
}

func WithItemMaxProcs(n int) Option {
	return func(c *Cache) { c.valueProcs = n }
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

	group      *singleflight.Group
	marshalers *semaphore.Weighted
	valueProcs int
}

func (self *Cache) applyOptions(opts ...Option) *Cache {
	for _, fn := range opts {
		fn(self)
	}

	if self.marshalers == nil {
		self.marshalers = semaphore.NewWeighted(int64(runtime.GOMAXPROCS(0)))
	}

	return self
}

func (self *Cache) New(opts ...Option) *Cache {
	c := *self
	return c.applyOptions(opts...)
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
		self.redis = classic.New(rdb)
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
	self.localCache = local.NewTinyLFU(size, ttl)
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

func (self *Cache) DefaultTTL() time.Duration {
	return self.defaultTTL
}

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

func (self *Cache) WithItemMaxProcs(n int) *Cache {
	c := self.New()
	c.valueProcs = n
	return c
}
