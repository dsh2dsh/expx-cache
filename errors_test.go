package cache

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
