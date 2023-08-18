package cache

import "sync/atomic"

type Stats struct {
	Hits        uint64
	Misses      uint64
	LocalHits   uint64
	LocalMisses uint64
}

// Stats returns cache statistics.
func (self *Cache) Stats() *Stats {
	if !self.statsEnabled {
		return nil
	}
	return &Stats{
		Hits:        atomic.LoadUint64(&self.hits),
		Misses:      atomic.LoadUint64(&self.misses),
		LocalHits:   atomic.LoadUint64(&self.localHits),
		LocalMisses: atomic.LoadUint64(&self.localMisses),
	}
}

func (self *Cache) addLocalHit() {
	if self.statsEnabled {
		atomic.AddUint64(&self.localHits, 1)
	}
}

func (self *Cache) addLocalMiss() {
	if self.statsEnabled {
		atomic.AddUint64(&self.localMisses, 1)
	}
}

func (self *Cache) addHit() {
	if self.statsEnabled {
		atomic.AddUint64(&self.hits, 1)
	}
}

func (self *Cache) addMiss() {
	if self.statsEnabled {
		atomic.AddUint64(&self.misses, 1)
	}
}
