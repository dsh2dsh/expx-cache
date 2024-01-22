package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStats_Merge(t *testing.T) {
	s1 := Stats{
		Hits:        1,
		Misses:      2,
		LocalHits:   3,
		LocalMisses: 4,
	}
	s2 := Stats{
		Hits:        5,
		Misses:      6,
		LocalHits:   7,
		LocalMisses: 8,
	}
	s1.Merge(&s2)

	wantStats := Stats{
		Hits:        6,
		Misses:      8,
		LocalHits:   10,
		LocalMisses: 12,
	}
	assert.Equal(t, wantStats, s1)
}

func TestCache_Stats_statsEnabled(t *testing.T) {
	cache := New()
	cache.addHit()
	assert.Equal(t, Stats{}, cache.Stats())

	cache = cache.WithStats(true)
	cache.addHit()
	assert.NotEqual(t, Stats{}, cache.Stats())
}
