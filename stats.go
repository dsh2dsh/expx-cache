package cache

import "sync/atomic"

type Stats struct {
	Hits        uint64
	Misses      uint64
	LocalHits   uint64
	LocalMisses uint64
}

func (self *Stats) localHit() {
	atomic.AddUint64(&self.LocalHits, 1)
}

func (self *Stats) localMiss() {
	atomic.AddUint64(&self.LocalMisses, 1)
}

func (self *Stats) hit() {
	atomic.AddUint64(&self.Hits, 1)
}

func (self *Stats) miss() {
	atomic.AddUint64(&self.Misses, 1)
}

func (self *Stats) Stats() Stats {
	return Stats{
		Hits:        atomic.LoadUint64(&self.Hits),
		Misses:      atomic.LoadUint64(&self.Misses),
		LocalHits:   atomic.LoadUint64(&self.LocalHits),
		LocalMisses: atomic.LoadUint64(&self.LocalMisses),
	}
}

// Stats returns cache statistics.
func (self *Cache) Stats() Stats {
	return self.stats.Stats()
}

func (self *Cache) addLocalHit() {
	if self.statsEnabled {
		self.stats.localHit()
	}
}

func (self *Cache) addLocalMiss() {
	if self.statsEnabled {
		self.stats.localMiss()
	}
}

func (self *Cache) addHit() {
	if self.statsEnabled {
		self.stats.hit()
	}
}

func (self *Cache) addMiss() {
	if self.statsEnabled {
		self.stats.miss()
	}
}
