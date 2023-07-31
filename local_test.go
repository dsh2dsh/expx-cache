package cache

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTinyLFU_Get_CorruptionOnExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
	}

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
