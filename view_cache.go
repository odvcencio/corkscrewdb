package corkscrewdb

import (
	"container/list"
	"sync"
)

const viewCacheSize = 8

type viewEntry struct {
	clock uint64
	index *index
	dim   int
}

// viewLRU memoizes recently built point-in-time view indices keyed by the view's
// maxLamport cutoff. Historical views (clock < the live head) are immutable and
// stay valid; the live-head key is invalidated when a write advances the clock.
type viewLRU struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List               // front = most recently used
	items map[uint64]*list.Element // clock -> element holding *viewEntry
	hits  int                      // test instrumentation
}

func newViewLRU(capacity int) *viewLRU {
	if capacity <= 0 {
		capacity = viewCacheSize
	}
	return &viewLRU{cap: capacity, ll: list.New(), items: make(map[uint64]*list.Element)}
}

func (c *viewLRU) get(clock uint64) (*viewEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[clock]; ok {
		c.ll.MoveToFront(el)
		c.hits++
		return el.Value.(*viewEntry), true
	}
	return nil, false
}

func (c *viewLRU) put(clock uint64, idx *index, dim int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[clock]; ok {
		c.ll.MoveToFront(el)
		ve := el.Value.(*viewEntry)
		ve.index = idx
		ve.dim = dim
		return
	}
	el := c.ll.PushFront(&viewEntry{clock: clock, index: idx, dim: dim})
	c.items[clock] = el
	for c.ll.Len() > c.cap {
		old := c.ll.Back()
		if old == nil {
			break
		}
		c.ll.Remove(old)
		delete(c.items, old.Value.(*viewEntry).clock)
	}
}

// invalidateHead drops any cached view whose clock is >= the incoming write's
// clock. Historical views (clock < write) are immutable and stay valid.
func (c *viewLRU) invalidateHead(writeClock uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for clock, el := range c.items {
		if clock >= writeClock {
			c.ll.Remove(el)
			delete(c.items, clock)
		}
	}
}
