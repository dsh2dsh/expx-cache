package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultBatchSize = 1000

func NewStdRedis(rdb redis.Cmdable) *StdRedis {
	return &StdRedis{
		rdb:       rdb,
		batchSize: defaultBatchSize,
	}
}

type StdRedis struct {
	rdb        redis.Cmdable
	batchSize  int
	refreshTTL time.Duration
}

func (self *StdRedis) WithBatchSize(size int) *StdRedis {
	self.batchSize = size
	return self
}

func (self *StdRedis) WithGetRefreshTTL(ttl time.Duration) *StdRedis {
	self.refreshTTL = ttl
	return self
}

// --------------------------------------------------

func (self *StdRedis) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := self.getter(ctx, self.rdb, key)
	if err != nil && errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("getter get: %w", err)
	}
	return b, nil
}

//nolint:wrapcheck // wrap it later
func (self *StdRedis) getter(
	ctx context.Context, rdb redis.Cmdable, key string,
) ([]byte, error) {
	if self.refreshTTL > 0 {
		return rdb.GetEx(ctx, key, self.refreshTTL).Bytes()
	}
	return rdb.Get(ctx, key).Bytes()
}

func (self *StdRedis) Del(ctx context.Context, keys ...string) error {
	for low := 0; low < len(keys); low += self.batchSize {
		high := min(len(keys), low+self.batchSize)
		if err := self.rdb.Del(ctx, keys[low:high]...).Err(); err != nil {
			return fmt.Errorf("redis del: %w", err)
		}
	}
	return nil
}

func (self *StdRedis) Set(
	ctx context.Context, key string, blob []byte, ttl time.Duration,
) error {
	if len(blob) > 0 && ttl > 0 {
		if err := self.rdb.Set(ctx, key, blob, ttl).Err(); err != nil {
			return fmt.Errorf("redis set %q: %w", key, err)
		}
	}
	return nil
}

// --------------------------------------------------

func (self *StdRedis) MGet(ctx context.Context, keys []string) ([][]byte, error) {
	pipe := self.rdb.Pipeline()
	blobs := make([][]byte, 0, len(keys))
	for _, key := range keys {
		_, err := self.getter(ctx, pipe, key)
		if err != nil {
			return nil, fmt.Errorf("getter get %q: %w", key, err)
		}
		if pipe.Len() == self.batchSize {
			if blobs, err = self.mgetPipeExec(ctx, pipe, blobs); err != nil {
				return nil, err
			}
		}
	}

	blobs, err := self.mgetPipeExec(ctx, pipe, blobs)
	if err != nil {
		return nil, err
	}

	return blobs, nil
}

func (self *StdRedis) mgetPipeExec(
	ctx context.Context, pipe redis.Pipeliner, blobs [][]byte,
) ([][]byte, error) {
	cmds, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("pipeline: %w", err)
	}

	for _, cmd := range cmds {
		if strCmd, ok := cmd.(interface{ Bytes() ([]byte, error) }); ok {
			if b, err := strCmd.Bytes(); err == nil {
				blobs = append(blobs, b)
			} else if errors.Is(err, redis.Nil) {
				blobs = append(blobs, nil)
			} else {
				return nil, fmt.Errorf("pipelined bytes %q: %w", cmd.Name(), err)
			}
		} else if cmd.Err() != nil {
			return nil, fmt.Errorf("pipelined cmd %q: %w", cmd.Name(), cmd.Err())
		} else {
			return nil, fmt.Errorf("pipelined: unexpected type=%T for Bytes()", cmd)
		}
	}

	return blobs, nil
}

// --------------------------------------------------

func (self *StdRedis) MSet(
	ctx context.Context,
	iter func() (key string, b []byte, ttl time.Duration, ok bool),
) error {
	pipe := self.rdb.Pipeline()
	for key, b, ttl, ok := iter(); ok; key, b, ttl, ok = iter() {
		if len(b) > 0 && ttl > 0 {
			if err := pipe.Set(ctx, key, b, ttl).Err(); err != nil {
				return fmt.Errorf("pipelined set: %w", err)
			} else if pipe.Len() == self.batchSize {
				if err := self.msetPipeExec(ctx, pipe); err != nil {
					return err
				}
			}
		}
	}
	return self.msetPipeExec(ctx, pipe)
}

func (self *StdRedis) msetPipeExec(ctx context.Context, pipe redis.Pipeliner) error {
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pipeline: %w", err)
	}
	return nil
}
