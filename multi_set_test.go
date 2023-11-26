package cache

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/redis"
)

func TestMultiCache_Set_errorCanceled(t *testing.T) {
	cache := New()
	assert.NotNil(t, cache)

	m := NewMultiCache(cache)
	assert.NotNil(t, m)

	item := Item{
		Key: testKey,
		Value: CacheableObject{
			Str: "mystring",
			Num: 42,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, m.Set(ctx, []*Item{&item}), context.Canceled)
}

func TestMultiCache_Set_errorWait(t *testing.T) {
	cache := New()
	assert.NotNil(t, cache)

	m := NewMultiCache(cache)
	assert.NotNil(t, m)

	item := Item{
		Key: testKey,
		Do: func(ctx context.Context) (any, error) {
			return nil, io.EOF
		},
	}
	require.ErrorIs(t, m.Set(context.Background(), []*Item{&item}), io.EOF)
}

func TestMultiCache_Set_errorRedis(t *testing.T) {
	ctx := context.Background()
	ttl := time.Minute
	wantErr := errors.New("test error")

	rdb := mocks.NewMockCmdable(t)
	rdb.EXPECT().Set(ctx, testKey, mock.Anything, ttl).Return(
		redis.NewStatusResult("", wantErr))

	cache := New().WithRedis(rdb)
	assert.NotNil(t, cache)
	m := NewMultiCache(cache)
	assert.NotNil(t, m)

	item := Item{
		Key:   testKey,
		Value: "foobar",
		TTL:   ttl,
	}
	require.ErrorIs(t, m.Set(ctx, []*Item{&item}), wantErr)
}
