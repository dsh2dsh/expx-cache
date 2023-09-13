package cache

import (
	"math/rand"
	"sync"
	"time"

	"github.com/dsh2dsh/go-tinylfu"
)

type LFU interface {
	Get(key string) (any, bool)
	Set(*tinylfu.Item)
	Del(key string)
}

func NewTinyLFU(size int, ttl time.Duration) *TinyLFU {
	const maxOffset = 10 * time.Second

	offset := ttl / 10
	if offset > maxOffset {
		offset = maxOffset
	}

	return &TinyLFU{
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // weak rnd is ok for us
		lfu:    tinylfu.New(size, 100000),
		ttl:    ttl,
		offset: offset,
	}
}

type TinyLFU struct {
	mu     sync.RWMutex
	rand   *rand.Rand
	lfu    LFU
	ttl    time.Duration
	offset time.Duration
}

func (self *TinyLFU) UseRandomizedTTL(offset time.Duration) {
	self.offset = offset
}

func (self *TinyLFU) Set(key string, b []byte) {
	if b == nil {
		return
	}
	self.mu.Lock()
	defer self.mu.Unlock()

	ttl := self.ttl
	if self.offset > 0 {
		ttl += time.Duration(self.rand.Int63n(int64(self.offset)))
	}

	self.lfu.Set(&tinylfu.Item{
		Key:      key,
		Value:    b,
		ExpireAt: time.Now().Add(ttl),
	})
}

func (self *TinyLFU) Get(key string) []byte {
	self.mu.RLock()
	defer self.mu.RUnlock()

	val, ok := self.lfu.Get(key)
	if !ok {
		return nil
	}

	b := val.([]byte)
	return b
}

func (self *TinyLFU) Del(key string) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.lfu.Del(key)
}
