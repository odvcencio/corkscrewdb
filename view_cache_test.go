package corkscrewdb

import "testing"

func TestViewLRU(t *testing.T) {
	c := newViewLRU(2)
	i1, i2, i3 := newIndex(4, 2, 1), newIndex(4, 2, 1), newIndex(4, 2, 1)

	c.put(10, i1, 4)
	c.put(20, i2, 4)
	// cap is 2; both present.
	if _, ok := c.get(10); !ok {
		t.Fatal("clock 10 should be present")
	}
	// Adding a third evicts the LRU. get(10) above moved 10 to front, so 20 is LRU.
	c.put(30, i3, 4)
	if _, ok := c.get(20); ok {
		t.Fatal("clock 20 should have been evicted (LRU)")
	}
	if _, ok := c.get(10); !ok {
		t.Fatal("clock 10 should still be present (recently used)")
	}
	if _, ok := c.get(30); !ok {
		t.Fatal("clock 30 should be present")
	}

	// get hit moves to front + counts.
	hitsBefore := c.hits
	if ve, ok := c.get(10); !ok || ve.index != i1 {
		t.Fatal("get(10) miss or wrong index")
	}
	if c.hits != hitsBefore+1 {
		t.Fatalf("hits = %d, want %d", c.hits, hitsBefore+1)
	}

	// invalidateHead drops entries at/above the write clock.
	c.put(40, newIndex(4, 2, 1), 4)
	c.invalidateHead(30) // drops 30 and 40, keeps 10
	if _, ok := c.get(30); ok {
		t.Fatal("clock 30 should be invalidated")
	}
	if _, ok := c.get(40); ok {
		t.Fatal("clock 40 should be invalidated")
	}
	if _, ok := c.get(10); !ok {
		t.Fatal("clock 10 (< 30) should survive invalidation")
	}
}
