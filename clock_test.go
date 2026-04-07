package corkscrewdb

import "testing"

func TestLamportClockTick(t *testing.T) {
	c := newLamportClock("actor-1")
	v1 := c.Tick()
	v2 := c.Tick()
	if v1 >= v2 {
		t.Fatalf("clock did not advance: %d >= %d", v1, v2)
	}
}

func TestLamportClockWitness(t *testing.T) {
	c := newLamportClock("actor-1")
	c.Tick()
	c.Tick()
	c.Witness(10)
	v := c.Tick()
	if v != 11 {
		t.Fatalf("after witness(10), tick = %d, want 11", v)
	}
}

func TestLamportClockActorTiebreak(t *testing.T) {
	if !lamportBefore(5, "aaa", 5, "bbb") {
		t.Fatal("aaa should sort before bbb at same clock")
	}
	if lamportBefore(5, "bbb", 5, "aaa") {
		t.Fatal("bbb should not sort before aaa at same clock")
	}
}
