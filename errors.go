package cache

import (
	"errors"
	"sync"
)

var ErrRedisCache = &RedisCacheError{error: errors.New("redis cache error")}

func newRedisCacheError(err error) error {
	return &RedisCacheError{error: err}
}

type RedisCacheError struct {
	error
}

func (self *RedisCacheError) Unwrap() error { return self.error }

func (self *RedisCacheError) Is(target error) bool {
	_, ok := target.(*RedisCacheError)
	return ok
}

// --------------------------------------------------

func newErrOnce() *errOnce {
	return &errOnce{}
}

type errOnce struct {
	err error
	mu  sync.RWMutex
}

func (self *errOnce) Once(err error) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.err == nil {
		self.err = err
	}
	return self.err
}

func (self *errOnce) Err() error {
	self.mu.RLock()
	defer self.mu.RUnlock()
	return self.err
}

func (self *errOnce) Reset() error {
	self.mu.Lock()
	defer self.mu.Unlock()
	err := self.err
	self.err = nil
	return err
}
