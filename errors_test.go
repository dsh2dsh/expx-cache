package cache

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_Err(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	assert.False(t, cache.Failed())
	assert.False(t, cache.Unfail())

	wantErr1 := errors.New("test error")
	require.ErrorIs(t, cache.redisCacheError(wantErr1), wantErr1)
	assert.True(t, cache.Failed())

	wantErr2 := errors.New("test error 2")
	require.ErrorIs(t, cache.redisCacheError(wantErr2), wantErr2)
	assert.True(t, cache.Failed())

	assert.True(t, cache.Unfail())
	assert.False(t, cache.Failed())
	assert.False(t, cache.Unfail())
}

func TestCache_redisCacheError_canceled(t *testing.T) {
	cache := New()
	testErr := fmt.Errorf("test error: %w", context.Canceled)
	err := cache.redisCacheError(testErr)
	require.ErrorIs(t, err, testErr)
	assert.False(t, cache.Failed())

	testErr = fmt.Errorf("test error: %w", context.DeadlineExceeded)
	err = cache.redisCacheError(testErr)
	require.ErrorIs(t, err, testErr)
	assert.True(t, cache.Failed())
}
