package rawstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func putF32(b []byte, v float32) { binary.LittleEndian.PutUint32(b, math.Float32bits(v)) }

func floatBytes(vals ...float32) []byte {
	out := make([]byte, len(vals)*4)
	for i, v := range vals {
		putF32(out[i*4:], v)
	}
	return out
}

func TestPutGetDedupReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	v := floatBytes(1, 2, 3)
	k1, err := s.Put(v)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := s.Put(v) // dedup: same key, no new blob
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k1, k2) {
		t.Fatal("dedup must return identical key")
	}
	got, err := s.Get(k1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, v) {
		t.Fatalf("get mismatch: %x", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen rebuilds the in-memory index from segment scan.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got2, err := s2.Get(k1)
	if err != nil || !bytes.Equal(got2, v) {
		t.Fatalf("reopen get mismatch: %x err=%v", got2, err)
	}
}

func TestGetDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	k, _ := s.Put(floatBytes(9, 8, 7))
	_ = s.Close()
	// Corrupt the only segment's value bytes (last 4 bytes of the frame).
	seg := filepath.Join(dir, "raw-000001.rvs")
	raw, _ := os.ReadFile(seg)
	raw[len(raw)-1] ^= 0xFF
	_ = os.WriteFile(seg, raw, 0o644)
	s2, _ := Open(dir)
	if _, err := s2.Get(k); err == nil {
		t.Fatal("expected corruption error from re-hash mismatch")
	}
}

func TestGC(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	kKeep, _ := s.Put(floatBytes(1, 1))
	kDrop, _ := s.Put(floatBytes(2, 2))
	reachable := map[string]struct{}{string(kKeep): {}}
	if err := s.GC(reachable); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(kKeep); err != nil {
		t.Fatalf("reachable blob was swept: %v", err)
	}
	if _, err := s.Get(kDrop); err == nil {
		t.Fatal("unreachable blob should be gone")
	}
}

func TestGetNotFoundIsTyped(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	defer s.Close()
	absent := make([]byte, 32)
	for i := range absent {
		absent[i] = byte(i)
	}
	_, err := s.Get(absent)
	if err == nil {
		t.Fatal("expected error for absent key")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want errors.Is ErrNotFound", err)
	}
	if errors.Is(err, ErrIntegrity) {
		t.Fatalf("not-found error wrongly classified as integrity: %v", err)
	}
}

func TestGetIntegrityIsTyped(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	k, _ := s.Put(floatBytes(9, 8, 7))
	_ = s.Close()
	// Corrupt the only segment's value bytes (last 4 bytes of the frame).
	seg := filepath.Join(dir, "raw-000001.rvs")
	raw, _ := os.ReadFile(seg)
	raw[len(raw)-1] ^= 0xFF
	_ = os.WriteFile(seg, raw, 0o644)
	s2, _ := Open(dir)
	defer s2.Close()
	_, err := s2.Get(k)
	if err == nil {
		t.Fatal("expected integrity error from re-hash mismatch")
	}
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("err = %v, want errors.Is ErrIntegrity", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("integrity error wrongly classified as not-found: %v", err)
	}
}
