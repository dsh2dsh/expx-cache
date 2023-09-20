package cache

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	redisMocks "github.com/dsh2dsh/expx-cache/mocks/redis"
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
	expectErr := io.EOF

	clients := []struct {
		name              string
		makeClient        func(rdb redis.Cmdable) RedisClient
		overrideConfigure func(testName string, rdb *redisMocks.MockCmdable) bool
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
				return NewRefreshRedis(rdb, time.Minute)
			},
			overrideConfigure: func(testName string, rdb *redisMocks.MockCmdable) bool {
				if testName == "Get" {
					rdb.EXPECT().GetEx(mock.Anything, mock.Anything, time.Minute).
						Return(redis.NewStringResult("", expectErr))
					return true
				}
				return false
			},
		},
	}

	tests := []struct {
		name      string
		configure func(rdb *redisMocks.MockCmdable)
		do        func(t *testing.T, redisClient RedisClient) error
	}{
		{
			name: "Get",
			configure: func(rdb *redisMocks.MockCmdable) {
				rdb.EXPECT().Get(mock.Anything, mock.Anything).
					Return(redis.NewStringResult("", expectErr))
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				b, err := redisClient.Get(context.Background(), testKey)
				assert.Nil(t, b)
				return err //nolint:wrapcheck
			},
		},
		{
			name: "Del",
			configure: func(rdb *redisMocks.MockCmdable) {
				rdb.EXPECT().Del(mock.Anything, mock.Anything).
					Return(redis.NewIntResult(0, expectErr))
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				return redisClient.Del(context.Background(), testKey) //nolint:wrapcheck
			},
		},
		{
			name: "Set",
			configure: func(rdb *redisMocks.MockCmdable) {
				rdb.EXPECT().Set(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(redis.NewStatusResult("", expectErr))
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				//nolint:wrapcheck
				return redisClient.Set(context.Background(), testKey, "", time.Minute)
			},
		},
	}

	for _, client := range clients {
		t.Run(client.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					rdb := redisMocks.NewMockCmdable(t)
					if client.overrideConfigure == nil || !client.overrideConfigure(tt.name, rdb) {
						tt.configure(rdb)
					}
					redisClient := client.makeClient(rdb)
					assert.ErrorIs(t, tt.do(t, redisClient), expectErr)
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
