package cache

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/mocks/cache"
)

func TestItem_redisSet(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)

	b := []byte{}

	tests := []struct {
		name     string
		expecter func(item *Item, redisCache *mocks.MockRedisClient)
		error    error
	}{
		{
			name: "Set",
			expecter: func(item *Item, redisCache *mocks.MockRedisClient) {
				redisCache.EXPECT().
					Set(item.Context(), item.Key, b, cache.ItemTTL(item)).Return(nil)
			},
		},
		{
			name: "SetNX",
			expecter: func(item *Item, redisCache *mocks.MockRedisClient) {
				item.SetNX = true
				redisCache.EXPECT().
					SetNX(item.Context(), item.Key, b, cache.ItemTTL(item)).Return(nil)
			},
		},
		{
			name: "SetXX",
			expecter: func(item *Item, redisCache *mocks.MockRedisClient) {
				item.SetXX = true
				redisCache.EXPECT().
					SetXX(item.Context(), item.Key, b, cache.ItemTTL(item)).Return(nil)
			},
		},
		{
			name: "error",
			expecter: func(item *Item, redisCache *mocks.MockRedisClient) {
				redisCache.EXPECT().
					Set(item.Context(), item.Key, b, cache.ItemTTL(item)).Return(io.EOF)
			},
			error: io.EOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := Item{Key: testKey}
			redisCache := mocks.NewMockRedisClient(t)
			tt.expecter(&item, redisCache)
			assert.ErrorIs(t, item.redisSet(redisCache, b, cache.ItemTTL(&item)),
				tt.error)
		})
	}
}
