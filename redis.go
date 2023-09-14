package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisGetter func(ctx context.Context, rdb redis.Cmdable, key string) ([]byte, error)

func NewStdRedis(rdb redis.Cmdable) *StdRedis {
	return &StdRedis{rdb: rdb, getter: defaultRedisGetter}
}

type StdRedis struct {
	rdb    redis.Cmdable
	getter RedisGetter
}

// --------------------------------------------------

var defaultRedisGetter RedisGetter = func(ctx context.Context,
	rdb redis.Cmdable, key string,
) ([]byte, error) {
	if b, err := rdb.Get(ctx, key).Bytes(); err == nil {
		return b, nil
	} else {
		return nil, fmt.Errorf("redis get: %w", err)
	}
}

func (self *StdRedis) WithGetter(getter RedisGetter) *StdRedis {
	self.getter = getter
	return self
}

func (self *StdRedis) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := self.getter(ctx, self.rdb, key)
	if errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("getter get: %w", err)
	}
	return b, nil
}

// --------------------------------------------------

func (self *StdRedis) Del(ctx context.Context, keys ...string) error {
	if err := self.rdb.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("delete keys from redis: %w", err)
	}
	return nil
}

func (self *StdRedis) Set(ctx context.Context, key string, value any,
	ttl time.Duration,
) error {
	if err := self.rdb.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("redis set %q: %w", key, err)
	}
	return nil
}

// --------------------------------------------------

func NewRefreshRedis(rdb redis.Cmdable, ttl time.Duration) *StdRedis {
	return NewStdRedis(rdb).WithGetter(refreshRedisGet(ttl).Get)
}

type refreshRedisGet time.Duration

func (self refreshRedisGet) Get(ctx context.Context, rdb redis.Cmdable,
	key string,
) ([]byte, error) {
	if b, err := rdb.GetEx(ctx, key, time.Duration(self)).Bytes(); err == nil {
		return b, nil
	} else {
		return nil, fmt.Errorf("redis getex: %w", err)
	}
}
