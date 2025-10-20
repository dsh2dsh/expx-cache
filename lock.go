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
	lockTTL  = 10 * time.Second
	lockTick = 3 * time.Second

	waitLockStart = time.Second
	waitLockSeq   = []time.Duration{10 * time.Second}
)

func NewWaitLockIter(start time.Duration, seq ...time.Duration) WaitLockIter {
	if len(seq) == 0 {
		return func() time.Duration { return start }
	}

	var minTime, maxTime, curTime time.Duration
	var i int

	return func() time.Duration {
		switch {
		case i == 0:
			minTime, maxTime = start, seq[i]
			i++
			curTime = minTime
		case i < len(seq):
			minTime, maxTime = maxTime, seq[i]
			i++
			curTime = minTime
		case minTime < maxTime:
			if curTime < maxTime {
				curTime = min(curTime*2, maxTime)
			}
			return minTime + mathRand.N(curTime-minTime+1)
		}
		return curTime
	}
}

type WaitLockIter func() time.Duration

func newCfgLock(ttl, tick time.Duration, iter func() WaitLockIter) cfgLock {
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
	Iter func() WaitLockIter

	valid bool
}

func (self *cfgLock) Valid() bool { return self.valid }

// --------------------------------------------------

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
		if self.Failed() {
			b, err = self.set(ctx, item)
		}
		return b, err
	} else if len(b) != 0 {
		return b, err
	}

	err = l.WithLock(ctx, self.cfgLock.TTL, self.cfgLock.Tick, func() error {
		b, err = self.set(ctx, item)
		return err
	})
	if self.Failed() {
		err = nil
	}
	return b, err
}

func (self *Cache) redisLockGet(ctx context.Context, item *Item,
) (l lock, b []byte, err error) {
	keyLock, keyGet := self.ResolveKeyLock(item.Key), self.ResolveKey(item.Key)
	l = self.lock(keyLock, keyGet)

	b, err = l.Get(ctx, self.cfgLock.TTL, self.cfgLock.Iter())
	if err != nil || len(b) == 0 {
		self.addMiss()
		return l, b, err
	}
	self.addHit()

	if self.useLocalCache(item) {
		self.localCache.Set(keyGet, b)
	}
	return l, b, err
}

func (self *Cache) ResolveKeyLock(key string) string {
	return self.ResolveKey(self.keyLocked(key))
}

func (self *Cache) keyLocked(key string) string {
	return self.prefixLock + key
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
	waitIter WaitLockIter,
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
	waitIter WaitLockIter,
) (ok bool, b []byte, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	get := func() bool {
		ok, b, err = self.redisLockGet(ctx, ttl)
		return err != nil || ok || b != nil
	}
	if get() {
		return ok, b, err
	}

	unlockChan := self.subscribe(ctx)
	if err2 := self.waitUnlock(ctx, unlockChan, get, waitIter); err2 != nil {
		err = self.redisCacheError(fmt.Errorf("lock %q get %q: %w", self.keyLock,
			self.keyGet, err2))
	}

	cancel()
	<-unlockChan
	return ok, b, err
}

func (self *lock) redisLockGet(ctx context.Context, ttl time.Duration,
) (ok bool, b []byte, err error) {
	ok, b, err = self.redis.LockGet(ctx, self.keyLock, self.value, ttl, self.keyGet)
	if err != nil {
		err = self.redisCacheError(fmt.Errorf("lock get: %w", err))
	}
	return ok, b, err
}

func (self *lock) subscribe(ctx context.Context) <-chan unlockMessage {
	notifyChan := make(chan unlockMessage, 1)
	ready := make(chan struct{})
	go func() {
		value, err := self.redis.Listen(ctx, self.keyLock, func() error {
			close(ready)
			return nil
		})
		if err != nil {
			err = fmt.Errorf("listen for unlock %q: %w", self.keyLock, err)
		}
		notifyChan <- unlockMessage{Value: value, Err: err}
		close(notifyChan)
	}()
	<-ready
	return notifyChan
}

type unlockMessage struct {
	Value string
	Err   error
}

func (self *lock) waitUnlock(ctx context.Context,
	unlockChan <-chan unlockMessage, doneFn func() bool, waitIter WaitLockIter,
) (err error) {
	t := time.NewTimer(waitIter())
	for {
		var unlocked bool
		var msgValue string

		select {
		case <-ctx.Done():
			t.Stop()
			err = fmt.Errorf("waiting for unlock: %w", context.Cause(ctx))
		case <-t.C:
		case msg := <-unlockChan:
			t.Stop()
			if msg.Err != nil {
				err = msg.Err
			} else {
				unlocked = true
				msgValue = msg.Value
			}
		}

		if err != nil || doneFn() {
			break
		} else if unlocked {
			err = fmt.Errorf("unexpected unlock message: %q", msgValue)
			break
		}
		t.Reset(waitIter())
	}
	return err
}

func (self *lock) release(ctx context.Context) error {
	if self.value == "" {
		return fmt.Errorf("release lock %q: empty value", self.keyLock)
	}
	ok, err := self.redis.Unlock(ctx, self.keyLock, self.value)
	if err != nil {
		return self.redisCacheError(fmt.Errorf("release lock %q: %w",
			self.keyLock, err))
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
	ctx2, cancel := context.WithCancel(ctx)
	keepErr := make(chan error)
	go func() { keepErr <- self.keepLock(ctx2, ttl, freq) }()

	const errMsg = "with lock %q: %w"
	if err := fn(); err != nil {
		cancel()
		<-keepErr
		return fmt.Errorf(errMsg, self.keyLock, err)
	}

	cancel()
	if err := <-keepErr; err != nil && !errors.Is(err, context.Canceled) {
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

	const errMsg = "keep lock %q: %w"
	for {
		select {
		case <-ctx.Done():
		case <-tick.C:
		}

		if ctx.Err() != nil {
			return fmt.Errorf(errMsg, self.keyLock, context.Cause(ctx))
		}

		if ok, err := self.redis.Expire(ctx, self.keyLock, ttl); err != nil {
			return self.redisCacheError(fmt.Errorf(errMsg, self.keyLock, err))
		} else if !ok {
			return self.lockNotFound()
		}
	}
}
