package cache

import (
	"context"
	"io"
	"testing"

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
	rdb := redisMocks.NewMockCmdable(t)
	rdb.EXPECT().Set(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(redis.NewStatusResult("", io.EOF))

	cache := New().WithRedis(rdb)

	err := cache.Set(&Item{
		Ctx:   context.Background(),
		Key:   testKey,
		Value: "foobar",
	})
	require.ErrorIs(t, err, io.EOF)
}
