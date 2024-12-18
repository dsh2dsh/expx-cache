package cache

import (
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

type Option func(c *Cache)

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

func WithNoErr() Option {
	return func(c *Cache) { c.gotErr = new(atomic.Bool) }
}

func WithLock(ttl, tick time.Duration, iter func() WaitLockIter) Option {
	return func(c *Cache) { c.cfgLock = newCfgLock(ttl, tick, iter) }
}

func WithLockNotFound(fn func(key, value string) error) Option {
	return func(c *Cache) { c.lockNotFound = fn }
}
