package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewStdRedis(rdb redis.Cmdable) *StdRedis {
	return &StdRedis{rdb: rdb}
}

type StdRedis struct {
	rdb redis.Cmdable
}

func (self *StdRedis) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := self.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil { //nolint:errorlint // safe according to manual
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("get %q from redis: %w", key, err)
	}
	return b, nil
}

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
		return fmt.Errorf("set %q with ttl %v using redis: %w", key, ttl, err)
	}
	return nil
}

func (self *StdRedis) SetNX(ctx context.Context, key string, value any,
	ttl time.Duration,
) error {
	if err := self.rdb.SetNX(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("setnx %q with ttl %v using redis: %w", key, ttl, err)
	}
	return nil
}

func (self *StdRedis) SetXX(ctx context.Context, key string, value any,
	ttl time.Duration,
) error {
	if err := self.rdb.SetXX(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("setxx %q with ttl %v using redis: %w", key, ttl, err)
	}
	return nil
}

// --------------------------------------------------

func NewRefreshRedis(rdb redis.Cmdable, ttl time.Duration) *RefreshRedis {
	return &RefreshRedis{
		StdRedis:   NewStdRedis(rdb),
		refreshTTL: ttl,
	}
}

type RefreshRedis struct {
	refreshTTL time.Duration
	*StdRedis
}

func (self *RefreshRedis) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := self.rdb.GetEx(ctx, key, self.refreshTTL).Bytes()
	if err == redis.Nil { //nolint:errorlint // safe by manual
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("getex %q from redis: %w", key, err)
	}
	return b, nil
}
