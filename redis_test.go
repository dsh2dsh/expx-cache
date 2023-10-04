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
	ttl := time.Minute

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

	ctx := context.Background()
	ttls := []time.Duration{ttl}
	expectErr := io.EOF

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
				return redisClient.Set(ctx, testKey, []byte("abc"), ttl)
			},
		},
		{
			name: "MGet error from getter",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				if strings.Contains(t.Name(), "/RefreshRedis/") {
					pipe.EXPECT().GetEx(ctx, testKey, ttl).Return(
						redis.NewStringResult("", expectErr))
				} else {
					pipe.EXPECT().Get(ctx, testKey).Return(
						redis.NewStringResult("", expectErr))
				}
				rdb.EXPECT().Pipeline().Return(pipe)
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, []string{testKey, "key2", "key3"})
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet error from Exec",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2", "key3"}
				for _, expectedKey := range expectedKeys {
					if strings.Contains(t.Name(), "/RefreshRedis/") {
						pipe.EXPECT().GetEx(ctx, expectedKey, ttl).Return(
							redis.NewStringResult("", nil))
					} else {
						pipe.EXPECT().Get(ctx, expectedKey).Return(
							redis.NewStringResult("", nil))
					}
				}
				pipe.EXPECT().Len().Return(len(expectedKeys))
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						return nil, expectErr
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, []string{testKey, "key2", "key3"})
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet error from StringCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2", "key3"}
				for _, expectedKey := range expectedKeys {
					if strings.Contains(t.Name(), "/RefreshRedis/") {
						pipe.EXPECT().GetEx(ctx, expectedKey, ttl).Return(
							redis.NewStringResult("", nil))
					} else {
						pipe.EXPECT().Get(ctx, expectedKey).Return(
							redis.NewStringResult("", nil))
					}
				}
				pipe.EXPECT().Len().Return(len(expectedKeys))
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewStringResult("", io.EOF)}
						return cmds, nil
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, []string{testKey, "key2", "key3"})
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet error from BoolCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2", "key3"}
				for _, expectedKey := range expectedKeys {
					if strings.Contains(t.Name(), "/RefreshRedis/") {
						pipe.EXPECT().GetEx(ctx, expectedKey, ttl).Return(
							redis.NewStringResult("", nil))
					} else {
						pipe.EXPECT().Get(ctx, expectedKey).Return(
							redis.NewStringResult("", nil))
					}
				}
				pipe.EXPECT().Len().Return(len(expectedKeys))
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewBoolResult(false, io.EOF)}
						return cmds, nil
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, []string{testKey, "key2", "key3"})
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet unexpected type",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2", "key3"}
				for _, expectedKey := range expectedKeys {
					if strings.Contains(t.Name(), "/RefreshRedis/") {
						pipe.EXPECT().GetEx(ctx, expectedKey, ttl).Return(
							redis.NewStringResult("", nil))
					} else {
						pipe.EXPECT().Get(ctx, expectedKey).Return(
							redis.NewStringResult("", nil))
					}
				}
				pipe.EXPECT().Len().Return(len(expectedKeys))
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewBoolResult(false, nil)}
						return cmds, nil
					})
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				_, err := redisClient.MGet(ctx, []string{testKey, "key2", "key3"})
				return err //nolint:wrapcheck
			},
			assertErr: func(t *testing.T, err error) {
				assert.ErrorContains(t, err, "unexpected type=")
			},
		},
		{
			name: "MSet error from SET",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				pipe.EXPECT().Set(ctx, testKey, mock.Anything, ttl).Return(
					redis.NewStatusResult("", expectErr))
				rdb.EXPECT().Pipeline().Return(pipe)
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				err := redisClient.MSet(ctx,
					msetIter([]string{testKey}, [][]byte{[]byte("abc")}, ttls))
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MSet error from Exec",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				pipe.EXPECT().Set(ctx, testKey, mock.Anything, ttl).Return(
					redis.NewStatusResult("", nil))
				pipe.EXPECT().Len().Return(1)
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						return nil, expectErr
					})
				rdb.EXPECT().Pipeline().Return(pipe)
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				err := redisClient.MSet(ctx,
					msetIter([]string{testKey}, [][]byte{[]byte("abc")}, ttls))
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MSet error from StatusCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				pipe.EXPECT().Set(ctx, testKey, mock.Anything, ttl).Return(
					redis.NewStatusResult("", nil))
				pipe.EXPECT().Len().Return(1)
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						cmds := []redis.Cmder{redis.NewStatusResult("", io.EOF)}
						return cmds, nil
					})
				rdb.EXPECT().Pipeline().Return(pipe)
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				err := redisClient.MSet(ctx,
					msetIter([]string{testKey}, [][]byte{[]byte("abc")}, ttls))
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

func msetIter(
	keys []string, blobs [][]byte, times []time.Duration,
) func() (key string, b []byte, ttl time.Duration, ok bool) {
	i := 0
	return func() (key string, b []byte, ttl time.Duration, ok bool) {
		if i < len(keys) {
			key, b, ttl, ok = keys[i], blobs[i], times[i], true
			i++
		}
		return
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
