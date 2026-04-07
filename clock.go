package corkscrewdb

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
)

type lamportClock struct {
	mu      sync.Mutex
	counter uint64
	actorID string
}

func newLamportClock(actorID string) *lamportClock {
	return &lamportClock{actorID: actorID}
}

func generateActorID() string {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

func generateSeed() int64 {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return int64(uint64(buf[0]) |
		uint64(buf[1])<<8 |
		uint64(buf[2])<<16 |
		uint64(buf[3])<<24 |
		uint64(buf[4])<<32 |
		uint64(buf[5])<<40 |
		uint64(buf[6])<<48 |
		uint64(buf[7])<<56)
}

func (c *lamportClock) Tick() uint64 {
	c.mu.Lock()
	c.counter++
	v := c.counter
	c.mu.Unlock()
	return v
}

func (c *lamportClock) Witness(remote uint64) {
	c.mu.Lock()
	if remote > c.counter {
		c.counter = remote
	}
	c.mu.Unlock()
}

func (c *lamportClock) Current() uint64 {
	c.mu.Lock()
	v := c.counter
	c.mu.Unlock()
	return v
}

func (c *lamportClock) ActorID() string {
	return c.actorID
}

// lamportBefore returns true if (clockA, actorA) orders before (clockB, actorB).
func lamportBefore(clockA uint64, actorA string, clockB uint64, actorB string) bool {
	if clockA != clockB {
		return clockA < clockB
	}
	return actorA < actorB
}
