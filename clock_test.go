package corkscrewdb

import (
	"testing"
)

func TestHLCPackUnpack(t *testing.T) {
	cases := []struct {
		physMs  uint64
		logical uint64
	}{
		{0, 0},
		{1, 0},
		{0, 1},
		{1234567890123, 999999},
		{(1 << 44) - 1, (1 << 20) - 1}, // max values
	}
	for _, tc := range cases {
		packed := packHLC(tc.physMs, tc.logical)
		gotPhys, gotLogical := unpackHLC(packed)
		if gotPhys != tc.physMs {
			t.Errorf("packHLC(%d, %d): unpack physical = %d, want %d", tc.physMs, tc.logical, gotPhys, tc.physMs)
		}
		if gotLogical != tc.logical {
			t.Errorf("packHLC(%d, %d): unpack logical = %d, want %d", tc.physMs, tc.logical, gotLogical, tc.logical)
		}
	}
}

func TestHLCOrdering(t *testing.T) {
	a := packHLC(100, 0)
	b := packHLC(100, 1)
	c := packHLC(101, 0)
	if !(a < b) {
		t.Fatalf("expected a < b: %d >= %d", a, b)
	}
	if !(b < c) {
		t.Fatalf("expected b < c: %d >= %d", b, c)
	}
}

func TestHLCNowAdvances(t *testing.T) {
	h := newHLC("test-actor")
	prev := h.Now()
	for i := 0; i < 1000; i++ {
		cur := h.Now()
		if cur <= prev {
			t.Fatalf("Now() did not advance at iteration %d: prev=%d cur=%d", i, prev, cur)
		}
		prev = cur
	}
}

func TestHLCWitness(t *testing.T) {
	h := newHLC("test-actor")
	_ = h.Now()
	_ = h.Now()

	// Create a remote clock far in the future.
	remote := packHLC(9999999999999, 42)
	h.Witness(remote)

	next := h.Now()
	if next <= remote {
		t.Fatalf("after Witness(%d), Now()=%d should be > remote", remote, next)
	}
}

func TestHLCLogicalOverflow(t *testing.T) {
	h := newHLC("test-actor")
	// Set the clock state so logical is at max (2^20 - 1).
	maxLogical := uint64((1 << 20) - 1)
	// Use a physical time beyond the real wall clock so wallNow doesn't dominate.
	physMs := uint64(1) << 43 // far future
	h.mu.Lock()
	h.clock = packHLC(physMs, maxLogical)
	h.mu.Unlock()

	// Override wall clock to return something smaller so the overflow path is taken.
	origWallNow := wallNow
	wallNow = func() uint64 { return 1000 }
	defer func() { wallNow = origWallNow }()

	next := h.Now()
	gotPhys, gotLogical := unpackHLC(next)
	// On overflow, physical should advance and logical should reset to 0.
	if gotPhys != physMs+1 {
		t.Fatalf("expected physical to advance to %d on overflow, got %d", physMs+1, gotPhys)
	}
	if gotLogical != 0 {
		t.Fatalf("expected logical to reset to 0 on overflow, got %d", gotLogical)
	}
}

func TestHLCActorTiebreak(t *testing.T) {
	// Two HLCs with the same packed value but different actor IDs.
	// The sort order should be deterministic via string comparison on actor.
	h1 := newHLC("aaa")
	h2 := newHLC("zzz")

	// Force both to the same clock value.
	target := packHLC(1000, 5)
	h1.mu.Lock()
	h1.clock = target
	h1.mu.Unlock()
	h2.mu.Lock()
	h2.clock = target
	h2.mu.Unlock()

	// "aaa" < "zzz", so given the same HLC, aaa should sort before zzz.
	if !(h1.ActorID() < h2.ActorID()) {
		t.Fatal("expected aaa < zzz for actor ID comparison")
	}
	if h1.Current() != h2.Current() {
		t.Fatal("expected same clock value for tiebreak test")
	}
}
