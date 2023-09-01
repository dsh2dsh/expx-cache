package cache

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	mocks "github.com/dsh2dsh/expx-cache/mocks/cache"
)

func TestItem_ttlTooSmall(t *testing.T) {
	item := &Item{TTL: 500 * time.Millisecond}
	assert.Equal(t, time.Second, item.ttl())
}

func TestItem_redisSet(t *testing.T) {
	b := []byte{}

	tests := []struct {
		name     string
		expecter func(item *Item, redisCache *mocks.MockRedisClient)
		error    error
	}{
		{
			name: "Set",
			expecter: func(item *Item, redisCache *mocks.MockRedisClient) {
				redisCache.EXPECT().Set(item.Context(), item.Key, b, item.ttl()).
					Return(nil)
			},
		},
		{
			name: "SetNX",
			expecter: func(item *Item, redisCache *mocks.MockRedisClient) {
				item.SetNX = true
				redisCache.EXPECT().
					SetNX(item.Context(), item.Key, b, item.ttl()).Return(nil)
			},
		},
		{
			name: "SetXX",
			expecter: func(item *Item, redisCache *mocks.MockRedisClient) {
				item.SetXX = true
				redisCache.EXPECT().
					SetXX(item.Context(), item.Key, b, item.ttl()).Return(nil)
			},
		},
		{
			name: "error",
			expecter: func(item *Item, redisCache *mocks.MockRedisClient) {
				redisCache.EXPECT().
					Set(item.Context(), item.Key, b, item.ttl()).Return(io.EOF)
			},
			error: io.EOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := Item{Key: testKey}
			redisCache := mocks.NewMockRedisClient(t)
			tt.expecter(&item, redisCache)
			assert.ErrorIs(t, item.redisSet(redisCache, b), tt.error)
		})
	}
}
