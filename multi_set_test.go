package cache

import (
	"context"
	"io"
	"testing"
	"time"

	mocks "github.com/dsh2dsh/expx-cache/mocks/redis"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	assert.ErrorIs(t, m.Set(ctx, []*Item{&item}), context.Canceled)
}

func TestMultiCache_Set_errorWait(t *testing.T) {
	cache := New()
	assert.NotNil(t, cache)

	m := NewMultiCache(cache)
	assert.NotNil(t, m)

	item := Item{
		Key: testKey,
		Do: func(*Item) (any, error) {
			return nil, io.EOF
		},
	}
	assert.ErrorIs(t, m.Set(context.Background(), []*Item{&item}), io.EOF)
}

func TestMultiCache_Set_errorRedis(t *testing.T) {
	ctx := context.Background()
	ttl := time.Minute

	pipe := mocks.NewMockPipeliner(t)
	pipe.EXPECT().Set(ctx, testKey, mock.Anything, ttl).Return(
		redis.NewStatusResult("", io.EOF))

	rdb := mocks.NewMockCmdable(t)
	rdb.EXPECT().Pipeline().Return(pipe)

	cache := New().WithRedis(rdb)
	assert.NotNil(t, cache)

	m := NewMultiCache(cache)
	assert.NotNil(t, m)

	item := Item{
		Key:   testKey,
		Value: "foobar",
		TTL:   ttl,
	}
	assert.ErrorIs(t, m.Set(ctx, []*Item{&item}), io.EOF)
}
