package cache

import (
	"context"
	"fmt"
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
	batchSize := 3
	ttl := time.Minute

	clients := []struct {
		name       string
		makeClient func(rdb redis.Cmdable) RedisClient
	}{
		{
			name: "StdRedis",
			makeClient: func(rdb redis.Cmdable) RedisClient {
				return NewStdRedis(rdb).WithBatchSize(batchSize)
			},
		},
		{
			name: "RefreshRedis",
			makeClient: func(rdb redis.Cmdable) RedisClient {
				return NewRefreshRedis(rdb, ttl).WithBatchSize(batchSize)
			},
		},
	}

	ctx := context.Background()
	ttls := []time.Duration{ttl, ttl, ttl}
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
				rdb.EXPECT().Del(ctx, []string{testKey}).
					Return(redis.NewIntResult(0, expectErr))
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
			name: "MGet error from batchSize",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2", "key3"}
				var pipeLen int
				getResult := func(ctx context.Context, key string) *redis.StringCmd {
					pipeLen++
					return redis.NewStringResult("", nil)
				}
				getResultEx := func(
					ctx context.Context, key string, ttl time.Duration,
				) *redis.StringCmd {
					pipeLen++
					return redis.NewStringResult("", nil)
				}
				for _, expectedKey := range expectedKeys {
					if strings.Contains(t.Name(), "/RefreshRedis/") {
						pipe.EXPECT().GetEx(ctx, expectedKey, ttl).RunAndReturn(getResultEx)
					} else {
						pipe.EXPECT().Get(ctx, expectedKey).RunAndReturn(getResult)
					}
				}
				pipe.EXPECT().Len().RunAndReturn(func() int { return pipeLen })
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
			name: "MGet error from Exec",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2"}
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
				_, err := redisClient.MGet(ctx, []string{testKey, "key2"})
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet error from StringCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2"}
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
				_, err := redisClient.MGet(ctx, []string{testKey, "key2"})
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet error from BoolCmd",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2"}
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
				_, err := redisClient.MGet(ctx, []string{testKey, "key2"})
				return err //nolint:wrapcheck
			},
		},
		{
			name: "MGet unexpected type",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)
				expectedKeys := []string{testKey, "key2"}
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
				_, err := redisClient.MGet(ctx, []string{testKey, "key2"})
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
			name: "MSet error from batchSize",
			configure: func(t *testing.T, rdb *mocks.MockCmdable) {
				pipe := mocks.NewMockPipeliner(t)
				wantKeys := []string{testKey, "key2", "key3"}
				var pipeLen int
				for _, wantKey := range wantKeys {
					pipe.EXPECT().Set(ctx, wantKey, mock.Anything, ttl).RunAndReturn(
						func(
							ctx context.Context, key string, v any, ttl time.Duration,
						) *redis.StatusCmd {
							pipeLen++
							return redis.NewStatusResult("", nil)
						})
				}
				pipe.EXPECT().Len().RunAndReturn(func() int { return pipeLen })
				pipe.EXPECT().Exec(ctx).Return(nil, expectErr)
				rdb.EXPECT().Pipeline().Return(pipe)
			},
			do: func(t *testing.T, redisClient RedisClient) error {
				err := redisClient.MSet(ctx, msetIter(
					[]string{testKey, "key2", "key3"},
					[][]byte{[]byte("abc"), []byte("abc"), []byte("abc")}, ttls))
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

func TestStdRedis_MGetSet_WithBatchSize(t *testing.T) {
	batchSize := 3
	maxKeys := 8

	keys := make([]string, maxKeys)
	blobs := make([][]byte, len(keys))
	times := make([]time.Duration, len(keys))

	blob := []byte("foobar")
	ttl := time.Minute
	ctx := context.Background()

	tests := []struct {
		name    string
		pipeOp  func(pipe *mocks.MockPipeliner, fn func())
		cacheOp func(t *testing.T, redisCache *StdRedis, nKeys int)
	}{
		{
			name: "MGet",
			pipeOp: func(pipe *mocks.MockPipeliner, fn func()) {
				pipe.EXPECT().Get(ctx, mock.Anything).RunAndReturn(
					func(ctx context.Context, key string) *redis.StringCmd {
						fn()
						return redis.NewStringResult("", nil)
					})
			},
			cacheOp: func(t *testing.T, redisCache *StdRedis, nKeys int) {
				blobs := valueNoError[[][]byte](t)(redisCache.MGet(ctx, keys[:nKeys]))
				assert.Equal(t, [][]byte{}, blobs)
			},
		},
		{
			name: "MSet",
			pipeOp: func(pipe *mocks.MockPipeliner, fn func()) {
				pipe.EXPECT().Set(ctx, mock.Anything, blob, ttl).RunAndReturn(
					func(
						ctx context.Context, key string, v any, ttl time.Duration,
					) *redis.StatusCmd {
						fn()
						return redis.NewStatusResult("", nil)
					},
				)
			},
			cacheOp: func(t *testing.T, redisCache *StdRedis, nKeys int) {
				iter := msetIter(keys[:nKeys], blobs[:nKeys], times[:nKeys])
				require.NoError(t, redisCache.MSet(ctx, iter))
			},
		},
	}

	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("key-%00d", i)
		blobs[i] = blob
		times[i] = ttl
	}

	for _, tt := range tests {
		for nKeys := 0; nKeys <= len(keys); nKeys++ {
			t.Run(fmt.Sprintf("%s with %d keys", tt.name, nKeys), func(t *testing.T) {
				rdb := mocks.NewMockCmdable(t)
				redisCache := NewStdRedis(rdb)
				require.NotNil(t, redisCache)
				redisCache.WithBatchSize(batchSize)

				var expect []int
				nExec := nKeys / redisCache.batchSize
				for i := 0; i < nExec; i++ {
					expect = append(expect, redisCache.batchSize)
				}
				expect = append(expect, nKeys%redisCache.batchSize)

				pipe := mocks.NewMockPipeliner(t)
				rdb.EXPECT().Pipeline().Return(pipe)

				opCnt := 0
				if nKeys > 0 {
					tt.pipeOp(pipe, func() {
						opCnt++
					})
					pipe.EXPECT().Len().RunAndReturn(func() int {
						return opCnt
					})
				}

				var got []int
				pipe.EXPECT().Exec(ctx).RunAndReturn(
					func(ctx context.Context) ([]redis.Cmder, error) {
						got = append(got, opCnt)
						opCnt = 0
						return nil, nil
					}).Times(len(expect))

				tt.cacheOp(t, redisCache, nKeys)
				assert.Equal(t, expect, got)
			})
		}
	}
}

func TestStdRedis_Del_WithBatchSize(t *testing.T) {
	batchSize := 3
	maxKeys := 8

	keys := make([]string, maxKeys)
	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("key-%00d", i)
	}
	ctx := context.Background()

	for nKeys := 0; nKeys <= len(keys); nKeys++ {
		t.Run(fmt.Sprintf("with %d keys", nKeys), func(t *testing.T) {
			rdb := mocks.NewMockCmdable(t)
			redisCache := NewStdRedis(rdb)
			require.NotNil(t, redisCache)
			redisCache.WithBatchSize(batchSize)

			var wantKeys [][]string
			nDel := nKeys / redisCache.batchSize
			for i := 0; i < nDel; i++ {
				low := i * redisCache.batchSize
				high := low + redisCache.batchSize
				wantKeys = append(wantKeys, keys[low:high])
			}
			if nKeys%redisCache.batchSize > 0 {
				low := nDel * redisCache.batchSize
				high := low + nKeys%redisCache.batchSize
				wantKeys = append(wantKeys, keys[low:high])
			}

			var gotKeys [][]string
			for i := range wantKeys {
				rdb.EXPECT().Del(ctx, wantKeys[i]).RunAndReturn(
					func(ctx context.Context, keys ...string) *redis.IntCmd {
						gotKeys = append(gotKeys, keys)
						return redis.NewIntResult(int64(len(keys)), nil)
					})
			}

			require.NoError(t, redisCache.Del(ctx, keys[:nKeys]...))
			assert.Equal(t, wantKeys, gotKeys)
		})
	}
}
