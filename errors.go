package cache

import (
	"context"
	"errors"
)

func (self *Cache) Failed() bool { return self.gotErr.Load() }

func (self *Cache) redisCacheError(err error) error {
	if !errors.Is(err, context.Canceled) {
		self.gotErr.Store(true)
	}
	return err
}

func (self *Cache) Unfail() bool { return self.gotErr.Swap(false) }
