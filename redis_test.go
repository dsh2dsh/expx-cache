package cache

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/mocks/redis"
)

func TestNewRefreshRedis(t *testing.T) {
	const ttl = time.Minute
	rr := NewRefreshRedis(nil, ttl)
	require.IsType(t, new(StdRedis), rr)
	assert.Nil(t, rr.rdb)
}

func TestNewStdRedis(t *testing.T) {
	sr := NewStdRedis(nil)
	require.IsType(t, new(StdRedis), sr)
	assert.Nil(t, sr.rdb)
	assert.Implements(t, (*RedisClient)(nil), new(StdRedis))
}

func TestRedisClient_errors(t *testing.T) {
	ctx := context.Background()
	ttl := time.Minute
	expectErr := io.EOF

	clients := []struct {
		name       string
		makeClient func(rdb redis.Cmdable) RedisClient
	}{
		{
			name: "StdRedis",
			makeClient: func(rdb redis.Cmdable) RedisClient {
				return NewStdRedis(rdb)
			},
		},
		{
			name: "RefreshRedis",
			makeClient: func(rdb redis.Cmdable) RedisClient {
				return NewRefreshRedis(rdb, ttl)
			},
		},
	}

	tests := []struct {
		name      string
		configure func(t *testing.T, rdb *mocks.MockCmdable)
		do        func(t *testing.T, redisClient RedisClient) error
		assertErr func(t *testing.T, err error)
	}{
		{
			name: "Get",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				if strings.Contains(t.Name(), "/RefreshRedis/") {
					rdb.EXPECT().GetEx(ctx, mock.Anything, ttl).Return(
						redis.NewStringResult("", expectErr))
				} else {
					rdb.EXPECT().Get(ctx, testKey).Return(
						redis.NewStringResult("", expectErr))
				}
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				b, err := redisClient.Get(ctx, testKey)
				assert.Nil(t, b)
				return err //nolint:wrapcheck
			},
		},
		{
			name: "Del",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Del(ctx, testKey).Return(redis.NewIntResult(0, expectErr))
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				return redisClient.Del(ctx, testKey) //nolint:wrapcheck
			},
		},
		{
			name: "Set",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Set(ctx, testKey, mock.Anything, ttl).
					Return(redis.NewStatusResult("", expectErr))
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				//nolint:wrapcheck
				return redisClient.Set(ctx, testKey, []byte(""), ttl)
			},
		},
		{
			name: "MGet error from Pipelined",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
					func(ctx context.Context,
						fn func(redis.Pipeliner) error,
					) ([]redis.Cmder, error) {
						return nil, expectErr
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, "key1", "key2", "key3")
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet error from getter",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
					func(
						ctx context.Context, fn func(redis.Pipeliner) error,
					) ([]redis.Cmder, error) {
						pipe := mocks.NewMockPipeliner(t)
						if strings.Contains(t.Name(), "/RefreshRedis/") {
							pipe.EXPECT().GetEx(ctx, testKey, ttl).Return(
								redis.NewStringResult("", expectErr))
						} else {
							pipe.EXPECT().Get(ctx, testKey).Return(
								redis.NewStringResult("", expectErr))
						}
						return nil, fn(pipe)
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, testKey, "key2", "key3")
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet error from StringCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
					func(
						ctx context.Context, fn func(redis.Pipeliner) error,
					) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewStringResult("", io.EOF)}
						return cmds, nil
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, testKey, "key2", "key3")
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet error from BoolCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
					func(
						ctx context.Context, fn func(redis.Pipeliner) error,
					) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewBoolResult(false, io.EOF)}
						return cmds, nil
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, testKey, "key2", "key3")
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet unexpected type",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
					func(
						ctx context.Context, fn func(redis.Pipeliner) error,
					) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewBoolResult(false, nil)}
						return cmds, nil
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, testKey, "key2", "key3")
				return err //nolint:wrapcheck
			},
			assertErr: func(t *testing.T, err error) {
				assert.ErrorContains(t, err, "pipelined: unexpected type=")
			},
		},
		{
			name: "MSet error from Pipelined",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
					func(ctx context.Context,
						fn func(redis.Pipeliner) error,
					) ([]redis.Cmder, error) {
						return nil, expectErr
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				err := redisClient.MSet(ctx, []string{testKey, "key2", "key3"},
					make([][]byte, 3), []time.Duration{ttl, ttl, ttl})
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MSet error from SET",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
					func(
						ctx context.Context, fn func(redis.Pipeliner) error,
					) ([]redis.Cmder, error) {
						pipe := mocks.NewMockPipeliner(t)
						pipe.EXPECT().Set(ctx, testKey, []byte(nil), ttl).Return(
							redis.NewStatusResult("", expectErr))
						return nil, fn(pipe)
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				err := redisClient.MSet(ctx, []string{testKey, "key2", "key3"},
					make([][]byte, 3), []time.Duration{ttl, ttl, ttl})
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet error from StatusCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				rdb.EXPECT().Pipelined(ctx, mock.Anything).RunAndReturn(
					func(
						ctx context.Context, fn func(redis.Pipeliner) error,
					) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewStatusResult("", io.EOF)}
						return cmds, nil
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				err := redisClient.MSet(ctx, []string{testKey, "key2", "key3"},
					make([][]byte, 3), []time.Duration{ttl, ttl, ttl})
				return err //nolint:wrapcheck
			},
		},
	}

	for _, client := range clients {
		t.Run(client.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					rdb := mocks.NewMockCmdable(t)
					redisClient := client.makeClient(rdb)
					tt.configure(t, rdb)
					err := tt.do(t, redisClient)
					if tt.assertErr != nil {
						tt.assertErr(t, err)
					} else {
						assert.ErrorIs(t, err, expectErr)
					}
				})
			}
		})
	}
}

func TestStdRedis_WithBatchSize(t *testing.T) {
	redisCache := NewStdRedis(nil)
	require.NotNil(t, redisCache)

	assert.Equal(t, defaultBatchSize, redisCache.batchSize)

	batchSize := redisCache.batchSize * 2
	assert.Same(t, redisCache, redisCache.WithBatchSize(batchSize))
	assert.Equal(t, batchSize, redisCache.batchSize)
}
