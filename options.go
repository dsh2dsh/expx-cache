package cache

import (
	"runtime"

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
	return func(c *Cache) { c.errOnce = newErrOnce() }
}
