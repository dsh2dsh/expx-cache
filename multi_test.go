package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMultiCache(t *testing.T) {
	cache := New()
	require.NotNil(t, cache)
	multi := NewMultiCache(cache)
	require.NotNil(t, multi)
	assert.Equal(t, &MultiCache{cache: cache}, multi)
}
