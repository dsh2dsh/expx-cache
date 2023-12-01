package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cacheMocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
	redisMocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func TestCache_Set_errorMarshal(t *testing.T) {
	localCache := cacheMocks.NewMockLocalCache(t)
	wantErr := errors.New("expected error")
	cache := New().WithLocalCache(localCache).WithMarshal(
		func(v any) ([]byte, error) { return nil, wantErr })

	ctx := context.Background()
	item := Item{Key: testKey, Value: "foobar"}

	require.ErrorIs(t, cache.Set(ctx, &item), wantErr)
	require.ErrorIs(t, cache.Set(ctx, &item, &item), wantErr)
}

func TestCache_Set_withoutCache(t *testing.T) {
	cache := New()

	ctx := context.Background()
	item := Item{Key: testKey, Value: "foobar"}

	require.NoError(t, cache.Set(ctx, &item))
	require.NoError(t, cache.Set(ctx, &item, &item))
}

func TestCache_Set_redisErr(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	rdb := redisMocks.NewMockCmdable(t)
	rdb.EXPECT().Set(ctx, testKey, mock.Anything, mock.Anything).Return(
		redis.NewStatusResult("", wantErr))

	cache := New().WithRedis(rdb)
	item := Item{Key: testKey, Value: "foobar"}
	require.ErrorIs(t, cache.Set(ctx, &item), wantErr)

	pipe := redisMocks.NewMockPipeliner(t)
	var cmds []redis.Cmder
	pipe.EXPECT().Set(ctx, testKey, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, key string, v any, ttl time.Duration) *redis.StatusCmd {
			cmd := redis.NewStatusResult("", nil)
			cmds = append(cmds, cmd)
			return cmd
		})
	pipe.EXPECT().Len().RunAndReturn(func() int { return len(cmds) })
	pipe.EXPECT().Exec(ctx).Return(
		[]redis.Cmder{
			redis.NewStatusResult("", wantErr),
			redis.NewStatusResult("", wantErr),
		}, wantErr,
	)
	rdb.EXPECT().Pipeline().Return(pipe)

	require.ErrorIs(t, cache.Set(ctx, &item, &item), wantErr)
}

func TestCache_Set_errorCanceled(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	item := Item{
		Key:   testKey,
		Value: CacheableObject{Str: "mystring", Num: 42},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, cache.Set(ctx, &item, &item), context.Canceled)
}

func TestCache_Set_errorWait(t *testing.T) {
	cache := New().WithTinyLFU(1000, time.Minute)
	assert.NotNil(t, cache)

	wantErr := errors.New("expected error")
	item := Item{
		Key: testKey,
		Do: func(ctx context.Context) (any, error) {
			return nil, wantErr
		},
	}

	require.ErrorIs(t, cache.Set(context.Background(), &item, &item), wantErr)
}
