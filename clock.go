package corkscrewdb

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

const (
	hlcLogicalBits = 20
	hlcLogicalMask = (1 << hlcLogicalBits) - 1
)

// packHLC packs wall-clock milliseconds and a logical counter into a uint64.
// Upper 44 bits = physical millis, lower 20 bits = logical counter.
func packHLC(physMs, logical uint64) uint64 {
	return (physMs << hlcLogicalBits) | (logical & hlcLogicalMask)
}

// unpackHLC extracts the physical millis and logical counter from a packed HLC.
func unpackHLC(hlc uint64) (physMs, logical uint64) {
	return hlc >> hlcLogicalBits, hlc & hlcLogicalMask
}

// HLC is a Hybrid Logical Clock that packs wall-clock milliseconds and a
// logical counter into a single uint64, providing both monotonicity and
// physical-time correlation.
type HLC struct {
	mu      sync.Mutex
	clock   uint64
	actorID string
}

func newHLC(actorID string) *HLC {
	return &HLC{actorID: actorID}
}

// wallNow returns current wall-clock time in milliseconds.
// Extracted as a package-level var so tests could swap it if needed.
var wallNow = func() uint64 {
	return uint64(time.Now().UnixMilli())
}

// Now advances the HLC and returns a new, strictly-increasing timestamp.
// It picks max(wall_ms, last_physical) and increments the logical counter.
// If the logical counter overflows 20 bits, the physical component advances
// by 1 and the logical counter resets to 0.
func (h *HLC) Now() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	nowMs := wallNow()
	prevPhys, prevLogical := unpackHLC(h.clock)

	var physMs, logical uint64
	if nowMs > prevPhys {
		// Wall clock advanced: use it, reset logical.
		physMs = nowMs
		logical = 0
	} else {
		// Wall clock hasn't advanced (or went backward): stay on prevPhys.
		physMs = prevPhys
		logical = prevLogical + 1
		if logical > hlcLogicalMask {
			// Logical overflow: advance physical by 1, reset logical.
			physMs++
			logical = 0
		}
	}

	h.clock = packHLC(physMs, logical)
	return h.clock
}

// Witness merges a remote HLC value into this clock, ensuring subsequent
// Now() calls return values greater than the remote.
func (h *HLC) Witness(remote uint64) {
	h.mu.Lock()
	if remote > h.clock {
		h.clock = remote
	}
	h.mu.Unlock()
}

// Current returns the last HLC value without advancing the clock.
func (h *HLC) Current() uint64 {
	h.mu.Lock()
	v := h.clock
	h.mu.Unlock()
	return v
}

// ActorID returns the actor identifier for this clock.
func (h *HLC) ActorID() string {
	return h.actorID
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
