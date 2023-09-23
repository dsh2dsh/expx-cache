package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultBatchSize = 1000

type RedisGetter func(ctx context.Context, rdb redis.Cmdable, key string) ([]byte, error)

func NewStdRedis(rdb redis.Cmdable) *StdRedis {
	return &StdRedis{
		rdb:       rdb,
		getter:    defaultRedisGetter,
		batchSize: defaultBatchSize,
	}
}

type StdRedis struct {
	rdb       redis.Cmdable
	getter    RedisGetter
	batchSize int
}

func (self *StdRedis) WithBatchSize(size int) *StdRedis {
	self.batchSize = size
	return self
}

// --------------------------------------------------

var defaultRedisGetter RedisGetter = func(ctx context.Context,
	rdb redis.Cmdable, key string,
) ([]byte, error) {
	b, err := rdb.Get(ctx, key).Bytes()
	if err != nil && errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("redis get: %w", err)
	}
	return b, nil
}

func (self *StdRedis) WithGetter(getter RedisGetter) *StdRedis {
	self.getter = getter
	return self
}

func (self *StdRedis) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := self.getter(ctx, self.rdb, key)
	if err != nil && errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("getter get: %w", err)
	}
	return b, nil
}

// --------------------------------------------------

func (self *StdRedis) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	blobs := make([][]byte, 0, len(keys))
	for low := 0; low < len(keys); low += self.batchSize {
		high := min(len(keys), low+self.batchSize)
		if err := self.mgetBatch(ctx, keys[low:high], blobs[low:high]); err != nil {
			return nil, err
		}
	}
	return blobs, nil
}

func (self *StdRedis) mgetBatch(ctx context.Context, keys []string, blobs [][]byte) error {
	cmds, err := self.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			if _, err := self.getter(ctx, pipe, key); err != nil {
				return fmt.Errorf("getter get: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("pipelined: %w", err)
	}

	for i, cmd := range cmds {
		if strCmd, ok := cmd.(interface{ Bytes() ([]byte, error) }); ok {
			if b, err := strCmd.Bytes(); err == nil {
				blobs[i] = b
			} else if errors.Is(err, redis.Nil) {
				blobs[i] = nil
			} else {
				return fmt.Errorf("pipelined bytes %q: %w", cmd.Name(), err)
			}
		} else if cmd.Err() != nil {
			return fmt.Errorf("pipelined cmd %q: %w", cmd.Name(), cmd.Err())
		} else {
			return fmt.Errorf("pipelined: unexpected type=%T for Bytes()", cmd)
		}
	}

	return nil
}

// --------------------------------------------------

func (self *StdRedis) Del(ctx context.Context, keys ...string) error {
	for low := 0; low < len(keys); low += self.batchSize {
		high := min(len(keys), low+self.batchSize)
		if err := self.rdb.Del(ctx, keys[low:high]...).Err(); err != nil {
			return fmt.Errorf("redis del: %w", err)
		}
	}
	return nil
}

func (self *StdRedis) Set(ctx context.Context, key string, blob []byte,
	ttl time.Duration,
) error {
	if err := self.rdb.Set(ctx, key, blob, ttl).Err(); err != nil {
		return fmt.Errorf("redis set %q: %w", key, err)
	}
	return nil
}

// --------------------------------------------------

func (self *StdRedis) MSet(
	ctx context.Context, keys []string, blobs [][]byte, ttls []time.Duration,
) error {
	for low := 0; low < len(keys); low += self.batchSize {
		high := min(len(keys), low+self.batchSize)
		if err := self.msetBatch(
			ctx, keys[low:high], blobs[low:high], ttls[low:high],
		); err != nil {
			return err
		}
	}
	return nil
}

func (self *StdRedis) msetBatch(
	ctx context.Context, keys []string, blobs [][]byte, ttls []time.Duration,
) error {
	cmds, err := self.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, key := range keys {
			if err := pipe.Set(ctx, key, blobs[i], ttls[i]).Err(); err != nil {
				return fmt.Errorf("set: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("pipelined: %w", err)
	}

	for _, cmd := range cmds {
		if cmd.Err() != nil {
			return fmt.Errorf("pipelined cmd %q: %w", cmd.Name(), cmd.Err())
		}
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
