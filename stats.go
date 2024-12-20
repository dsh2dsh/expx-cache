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

func (self *Stats) Merge(s *Stats) {
	atomic.AddUint64(&self.Hits, s.Hits)
	atomic.AddUint64(&self.Misses, s.Misses)
	atomic.AddUint64(&self.LocalHits, s.LocalHits)
	atomic.AddUint64(&self.LocalMisses, s.LocalMisses)
}

// Stats returns cache statistics.
func (self *Cache) Stats() Stats { return self.stats.Stats() }

func (self *Cache) addLocalHit() { self.stats.localHit() }

func (self *Cache) addLocalMiss() { self.stats.localMiss() }

func (self *Cache) addHit() { self.stats.hit() }

func (self *Cache) addMiss() { self.stats.miss() }
