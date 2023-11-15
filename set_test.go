package cache

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cacheMocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
	redisMocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func TestCache_Set_Marshall_error(t *testing.T) {
	localCache := cacheMocks.NewMockLocalCache(t)
	cache := New().WithLocalCache(localCache).
		WithMarshal(func(value any) ([]byte, error) {
			return nil, io.EOF
		})

	err := cache.Set(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: "foobar",
	})
	require.ErrorIs(t, err, io.EOF)
}

func TestCache_Set_withoutCache(t *testing.T) {
	cache := New()
	require.NoError(t, cache.Set(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: "foobar",
	}))
}

func TestCache_Set_redisErr(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("test error")

	pipe := redisMocks.NewMockPipeliner(t)
	pipe.EXPECT().Set(ctx, testKey, mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, string, any, time.Duration) *redis.StatusCmd {
			return redis.NewStatusResult("", wantErr)
		})

	rdb := redisMocks.NewMockCmdable(t)
	rdb.EXPECT().Pipeline().Return(pipe)
	cache := New().WithRedis(rdb)

	err := cache.Set(&Item{
		Ctx:   ctx,
		Key:   testKey,
		Value: "foobar",
	})
	require.ErrorIs(t, err, wantErr)
}
