package cache

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	mathRand "math/rand/v2"
	"strings"
	"time"
)

const (
	prefixLock = "lock:"
	lockSep    = ":"
)

var (
	lockTTL     = 10 * time.Second
	lockTick    = 3 * time.Second
	lockPoll    = [2]time.Duration{time.Second, 10 * time.Second}
	lockMinKeep = 3
)

func NewLockWaitIter(minTime time.Duration, keepMin int, maxTime time.Duration,
) LockWaitIter {
	value, count := minTime, 0
	return func() time.Duration {
		if count < keepMin {
			count++
		} else if minTime < maxTime {
			if value < maxTime {
				value = min(value*2, maxTime)
			}
			return minTime + mathRand.N(value-minTime+1)
		}
		return value
	}
}

type LockWaitIter func() time.Duration

func newCfgLock(ttl, tick time.Duration, iter func() LockWaitIter) cfgLock {
	return cfgLock{
		TTL:   ttl,
		Tick:  tick,
		Iter:  iter,
		valid: true,
	}
}

type cfgLock struct {
	TTL  time.Duration
	Tick time.Duration
	Iter func() LockWaitIter

	valid bool
}

func (self *Cache) OnceLock(ctx context.Context, item Item) error {
	if !self.useRedis() {
		return self.once(ctx, &item)
	}

	b, err := self.getSetItemBytesOnce(ctx, &item, self.redisLockGetSet)
	if err != nil || len(b) == 0 {
		return err
	}
	return self.unmarshal(b, item.Value)
}

func (self *Cache) redisLockGetSet(ctx context.Context, item *Item,
) (b []byte, err error) {
	l, b, err := self.redisLockGet(ctx, item)
	if err != nil {
		if errors.Is(err, ErrRedisCache) {
			b, err = self.set(ctx, item)
		}
		return
	} else if len(b) != 0 {
		return
	}

	err = l.WithLock(ctx, self.cfgLock.TTL, self.cfgLock.Tick, func() error {
		b, err = self.set(ctx, item)
		return err
	})
	if errors.Is(err, ErrRedisCache) {
		err = nil
	}
	return
}

func (self *Cache) redisLockGet(ctx context.Context, item *Item,
) (l lock, b []byte, err error) {
	keyLock, keyGet := self.ResolveKeyLock(item.Key), self.ResolveKey(item.Key)
	l = self.lock(keyLock, keyGet)

	b, err = l.Get(ctx, self.cfgLock.TTL, self.cfgLock.Iter())
	if err != nil || len(b) == 0 {
		self.addMiss()
		return
	}
	self.addHit()

	if self.useLocalCache(item) {
		self.localCache.Set(keyGet, b)
	}
	return
}

func (self *Cache) ResolveKeyLock(key string) string {
	return self.ResolveKey(prefixLock + key)
}

// --------------------------------------------------

func (self *Cache) lock(keyLock, keyGet string) lock {
	return lock{
		keyLock: keyLock,
		keyGet:  keyGet,

		redis:           self.redis,
		redisCacheError: self.redisCacheError,

		notFound:  self.lockNotFound,
		requestId: self.requestId,

		randRead: rand.Read,
	}
}

type lock struct {
	keyLock string
	value   string
	keyGet  string

	redis           RedisCache
	redisCacheError func(err error) error

	notFound  func(key, value string) error
	requestId string

	randRead func(b []byte) (int, error)
}

func (self *lock) Get(ctx context.Context, ttl time.Duration,
	waitIter LockWaitIter,
) ([]byte, error) {
	if err := self.makeValue(); err != nil {
		return nil, err
	}
	ok, b, err := self.lockGet(ctx, ttl, waitIter)
	if err != nil {
		return nil, err
	} else if b != nil && ok {
		return b, self.release(ctx)
	}
	return b, nil
}

func (self *lock) makeValue() error {
	var b [16]byte
	if _, err := self.randRead(b[:]); err != nil {
		return fmt.Errorf("failed make random value for lock %q: %w", self.keyLock, err)
	}
	values := [...]string{
		self.requestId,
		base64.StdEncoding.EncodeToString(b[:]),
	}
	if values[0] == "" {
		self.value = values[1]
	} else {
		self.value = strings.Join(values[:], lockSep)
	}
	return nil
}

func (self *lock) lockGet(ctx context.Context, ttl time.Duration,
	waitIter LockWaitIter,
) (ok bool, b []byte, err error) {
	done := func() bool {
		ok, b, err = self.redisLockGet(ctx, ttl)
		return err != nil || ok || b != nil
	}
	if done() {
		return
	}

	t := time.NewTimer(waitIter())
	for {
		select {
		case <-ctx.Done():
			t.Stop()
		case <-t.C:
		}

		if ctx.Err() != nil {
			err = fmt.Errorf("lock %q get %q: %w", self.keyLock, self.keyGet,
				context.Cause(ctx))
			return
		}

		if done() {
			return
		}
		t.Reset(waitIter())
	}
}

func (self *lock) redisLockGet(ctx context.Context, ttl time.Duration,
) (ok bool, b []byte, err error) {
	ok, b, err = self.redis.LockGet(ctx, self.keyLock, self.value, ttl, self.keyGet)
	if err != nil {
		err = self.redisCacheError(fmt.Errorf("lock get: %w", err))
	}
	return
}

func (self *lock) release(ctx context.Context) error {
	if self.value == "" {
		return fmt.Errorf("release lock %q: empty value", self.keyLock)
	}
	ok, err := self.redis.Unlock(ctx, self.keyLock, self.value)
	if err != nil {
		return self.redisCacheError(fmt.Errorf("release lock %q: %w", self.keyLock, err))
	} else if !ok {
		return self.lockNotFound()
	}
	return nil
}

func (self *lock) lockNotFound() error {
	return self.notFound(self.keyLock, self.value)
}

func (self *lock) WithLock(ctx context.Context, ttl, freq time.Duration,
	fn func() error,
) error {
	ctxKeep, cancelKeep := context.WithCancel(ctx)
	keepErr := make(chan error)
	go func() { keepErr <- self.keepLock(ctxKeep, ttl, freq) }()

	const errMsg = "with lock %q: %w"
	if err := fn(); err != nil {
		cancelKeep()
		<-keepErr
		return fmt.Errorf(errMsg, self.keyLock, err)
	}

	cancelKeep()
	if err := <-keepErr; err != nil {
		return fmt.Errorf(errMsg, self.keyLock, err)
	}

	if ctx.Err() != nil {
		return fmt.Errorf(errMsg, self.keyLock, context.Cause(ctx))
	} else if err := self.release(ctx); err != nil {
		return fmt.Errorf(errMsg, self.keyLock, err)
	}
	return nil
}

func (self *lock) keepLock(ctx context.Context, ttl, freq time.Duration) error {
	tick := time.NewTicker(freq)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			ok, err := self.redis.Expire(ctx, self.keyLock, ttl)
			if err != nil {
				return self.redisCacheError(fmt.Errorf(
					"keep lock %q: %w", self.keyLock, err))
			} else if !ok {
				return self.lockNotFound()
			}
		}
	}
}
