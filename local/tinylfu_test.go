package local

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dsh2dsh/go-tinylfu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	mocks "github.com/dsh2dsh/expx-cache/internal/mocks/cache"
)

const testKey = "mykey"

func TestTinyLFU(t *testing.T) {
	localCache := NewTinyLFU(1000, time.Minute)
	require.NotNil(t, localCache)

	const key = "key1"
	value := []byte("value1")

	localCache.Set(key, value)
	b := localCache.Get(key)
	assert.Equal(t, value, b)

	localCache.Del(key)
	b = localCache.Get(key)
	assert.Nil(t, b)
}

func TestTinyLFU_Get_CorruptionOnExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	t.Parallel()

	strFor := func(i int) string {
		return fmt.Sprintf("a string %d", i)
	}
	keyName := func(i int) string {
		return fmt.Sprintf("key-%00000d", i)
	}

	mycache := NewTinyLFU(1000, time.Second)
	size := 50000
	// Put a bunch of stuff in the cache with a TTL of 1 second
	for i := 0; i < size; i++ {
		key := keyName(i)
		mycache.Set(key, []byte(strFor(i)))
	}

	// Read stuff for a bit longer than the TTL - that's when the corruption occurs
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := ctx.Done()
loop:
	for {
		select {
		case <-done:
			// this is expected
			break loop
		default:
			i := rand.Intn(size) //nolint:gosec // we don't need security rand here
			key := keyName(i)

			b := mycache.Get(key)
			if b == nil {
				continue loop
			}

			got := string(b)
			expected := strFor(i)
			require.Equal(t, expected, got, "expected=%q got=%q key=%q", expected,
				got, key)
		}
	}
}

func TestNewTinyLFU_offset(t *testing.T) {
	tests := []struct {
		ttl      time.Duration
		expected time.Duration
	}{
		{
			ttl:      10 * time.Second,
			expected: time.Second,
		},
		{
			ttl:      100 * time.Second,
			expected: 10 * time.Second,
		},
		{
			ttl:      1000 * time.Second,
			expected: 10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.ttl.String(), func(t *testing.T) {
			localCache := NewTinyLFU(1000, tt.ttl)
			require.NotNil(t, localCache)
			assert.Equal(t, tt.expected, localCache.offset)
		})
	}
}

func TestTinyLFU_UseRandomizedTTL(t *testing.T) {
	localCache := NewTinyLFU(1000, 1000*time.Second)
	require.NotNil(t, localCache)
	assert.Equal(t, 10*time.Second, localCache.offset)

	localCache.UseRandomizedTTL(10 * time.Hour)
	assert.Equal(t, 10*time.Hour, localCache.offset)
}

func TestTinyLFU_Set_offset(t *testing.T) {
	ttl := 10 * time.Second
	localCache := NewTinyLFU(1000, ttl)
	require.NotNil(t, localCache)

	lfu := mocks.NewMockLFU(t)
	localCache.lfu = lfu

	start := time.Now().Add(ttl)
	var expireAt time.Time
	lfu.EXPECT().Set(mock.Anything).Run(func(item *tinylfu.Item) {
		expireAt = item.ExpireAt
	})

	localCache.Set(testKey, []byte("a string"))
	assert.WithinRange(t, expireAt, start, time.Now().Add(ttl+localCache.offset))
}

func TestTinyLFU_Set_nil(t *testing.T) {
	localCache := NewTinyLFU(1000, 10*time.Second)
	require.NotNil(t, localCache)
	localCache.lfu = mocks.NewMockLFU(t)
	localCache.Set(testKey, nil)
}
