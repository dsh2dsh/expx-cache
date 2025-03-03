package local

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/dsh2dsh/go-tinylfu"
)

type LFU interface {
	Get(key string) ([]byte, bool)
	Set(*tinylfu.Item[[]byte])
	Del(key string)
}

func NewTinyLFU(size int, ttl time.Duration) *TinyLFU {
	const maxOffset = 10 * time.Second
	offset := min(maxOffset, ttl/10)

	return &TinyLFU{
		lfu:    tinylfu.New[[]byte](size, 100000),
		ttl:    ttl,
		offset: offset,
	}
}

type TinyLFU struct {
	mu     sync.Mutex
	lfu    LFU
	ttl    time.Duration
	offset time.Duration
}

func (self *TinyLFU) UseRandomizedTTL(offset time.Duration) {
	self.offset = offset
}

func (self *TinyLFU) Set(key string, b []byte) {
	if len(b) == 0 {
		return
	}

	ttl := self.ttl
	//nolint:gosec // I think weak rand is ok here
	if self.offset > 0 {
		ttl += rand.N(self.offset)
	}

	self.mu.Lock()
	defer self.mu.Unlock()

	self.lfu.Set(tinylfu.NewItemExpire(key, b, time.Now().Add(ttl)))
}

func (self *TinyLFU) Get(key string) []byte {
	self.mu.Lock()
	defer self.mu.Unlock()

	b, ok := self.lfu.Get(key)
	if !ok {
		return nil
	}
	return b
}

func (self *TinyLFU) Del(key string) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.lfu.Del(key)
}
