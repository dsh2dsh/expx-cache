package cache

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_Err(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	require.NoError(t, cache.Err())
	require.NoError(t, cache.ResetErr())

	wantErr1 := errors.New("test error")
	require.ErrorIs(t, cache.redisCacheError(wantErr1), wantErr1)
	require.ErrorIs(t, cache.Err(), wantErr1)

	wantErr2 := errors.New("test error 2")
	require.ErrorIs(t, cache.redisCacheError(wantErr2), wantErr1)
	require.ErrorIs(t, cache.Err(), wantErr1)

	require.ErrorIs(t, cache.ResetErr(), wantErr1)
	require.NoError(t, cache.Err())
	require.NoError(t, cache.ResetErr())
}

func TestErrOnce(t *testing.T) {
	errOnce := newErrOnce()
	require.NoError(t, errOnce.Err())
	require.NoError(t, errOnce.Reset())

	wantErr1 := errors.New("test error")
	assert.Same(t, wantErr1, errOnce.Once(wantErr1))
	assert.Same(t, wantErr1, errOnce.Err())

	wantErr2 := errors.New("test error 2")
	assert.Same(t, wantErr1, errOnce.Once(wantErr2))
	assert.Same(t, wantErr1, errOnce.Err())

	assert.Same(t, wantErr1, errOnce.Reset())
	require.NoError(t, errOnce.Reset())
	assert.Same(t, wantErr2, errOnce.Once(wantErr2))
	assert.Same(t, wantErr2, errOnce.Err())
	assert.Same(t, wantErr2, errOnce.Reset())
}
