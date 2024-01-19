package cache

import "errors"

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
