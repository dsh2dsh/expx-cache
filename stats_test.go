package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCache_Stats_statsEnabled(t *testing.T) {
	cache := New()
	cache.addHit()
	assert.Equal(t, Stats{}, cache.Stats())

	cache = cache.WithStats(true)
	cache.addHit()
	assert.NotEqual(t, Stats{}, cache.Stats())
}
