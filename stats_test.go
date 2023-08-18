package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_Stats_statsEnabled(t *testing.T) {
	cache := New()
	assert.Nil(t, cache.Stats())

	cache = cache.WithStats(true)
	require.NotNil(t, cache.Stats())
	assert.IsType(t, &Stats{}, cache.Stats())
}
