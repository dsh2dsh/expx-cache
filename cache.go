//go:generate mockery
package cache

import (
	"context"
	"iter"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"

	"github.com/dsh2dsh/expx-cache/local"
	"github.com/dsh2dsh/expx-cache/redis"
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
		keys iter.Seq[string]) iter.Seq2[[]byte, error]
	Set(ctx context.Context, maxItems int, items iter.Seq[redis.Item]) error

	LockGet(ctx context.Context, keySet, value string, ttl time.Duration,
		keyGet string) (ok bool, b []byte, err error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Unlock(ctx context.Context, key, value string) (bool, error)
	Listen(ctx context.Context, key string, ready ...func() error) (string, error)
}

type (
	MarshalFunc   func(any) ([]byte, error)
	UnmarshalFunc func([]byte, any) error
)

func New(opts ...Option) *Cache {
	c := &Cache{
		defaultTTL: defaultTTL,
		marshal:    marshal,
		unmarshal:  unmarshal,

		stats:      new(Stats),
		group:      new(singleflight.Group),
		prefixLock: prefixLock,
		gotErr:     new(atomic.Bool),
	}
	return c.applyOptions(opts...)
}

type Cache struct {
	redis      RedisCache
	localCache LocalCache

	defaultTTL time.Duration
	namespace  string
	requestId  string

	marshal    MarshalFunc
	unmarshal  UnmarshalFunc
	keyWrapper func(keys string) string

	stats *Stats

	group      *singleflight.Group
	marshalers *semaphore.Weighted
	valueProcs int

	cfgLock      cfgLock
	lockNotFound func(key, value string) error
	prefixLock   string

	gotErr *atomic.Bool
}

func (self *Cache) applyOptions(opts ...Option) *Cache {
	for _, fn := range opts {
		fn(self)
	}

	if self.marshalers == nil {
		WithMarshalMaxProcs(0)(self)
	}

	if !self.cfgLock.Valid() {
		WithLock(lockTTL, lockTick, func() WaitLockIter {
			return NewWaitLockIter(waitLockStart, waitLockSeq...)
		})(self)
	}

	if self.lockNotFound == nil {
		WithLockNotFound(func(key, value string) error { return nil })(self)
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
		self.redis = redis.New(rdb)
	} else {
		self.redis = nil
	}
	return self
}

func (self *Cache) WithRedisCache(client RedisCache) *Cache {
	self.redis = client
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
	return self.New(WithItemMaxProcs(n))
}

func (self *Cache) useRedis() bool {
	return self.redis != nil && !self.Failed()
}

func (self *Cache) WithLocalStats(fn func(c *Cache) error, opts ...Option,
) (Stats, error) {
	stats := Stats{}
	c := self.New(opts...)
	c.stats = &stats
	if err := fn(c); err != nil {
		return stats, err
	}
	self.stats.Merge(&stats)
	return stats, nil
}

func (self *Cache) WithRequestId(id string) *Cache {
	self.requestId = id
	return self
}

func (self *Cache) useLocalCache(item *Item) bool {
	return self.localCache != nil && !item.SkipLocalCache
}

func (self *Cache) WithPrefixLock(s string) *Cache {
	self.prefixLock = s
	return self
}
