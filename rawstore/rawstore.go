package rawstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"lukechampine.com/blake3"
)

func putF32(b []byte, v float32) { binary.LittleEndian.PutUint32(b, math.Float32bits(v)) }

type location struct {
	segment int
	offset  int64 // byte offset of the frame start within the segment
}

// Store is a per-collection content-addressed blob store for raw float32 vectors.
type Store struct {
	mu        sync.Mutex
	dir       string
	index     map[string]location // raw key string -> location
	active    *os.File
	activeNum int
	activeSz  int64
}

const maxSegmentBytes = 64 << 20

const headerLen = 4 + 1 // magic(u32) + version(u8)

// Open opens or creates a raw store rooted at dir, rebuilding its index by
// scanning all segment headers + frames.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, index: make(map[string]location)}
	segs, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	for _, num := range segs {
		if err := s.scanSegment(num); err != nil {
			return nil, err
		}
	}
	if err := s.openActive(segs); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) openActive(segs []int) error {
	num := 1
	if len(segs) > 0 {
		num = segs[len(segs)-1]
	}
	path := segmentPath(s.dir, num)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if info.Size() == 0 {
		if err := writeSegmentHeader(f); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		info, _ = f.Stat()
	}
	s.active = f
	s.activeNum = num
	s.activeSz = info.Size()
	return nil
}

func (s *Store) scanSegment(num int) error {
	f, err := os.Open(segmentPath(s.dir, num))
	if err != nil {
		return err
	}
	defer f.Close()
	if err := readSegmentHeader(f); err != nil {
		if errors.Is(err, ErrRVSFormat) {
			return err
		}
		return err
	}
	offset := int64(headerLen)
	for {
		key, _, n, err := readFrame(f)
		if err != nil {
			break // tail EOF or truncated trailing frame: stop scanning
		}
		s.index[string(key)] = location{segment: num, offset: offset}
		offset += int64(n)
	}
	return nil
}

// Put writes the raw bytes content-addressed by blake3-256, fsyncs, and
// returns the key. A duplicate key is a no-op (dedup).
func (s *Store) Put(raw []byte) ([]byte, error) {
	sum := blake3.Sum256(raw)
	key := sum[:]
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.index[string(key)]; ok {
		return append([]byte(nil), key...), nil // dedup
	}
	if s.activeSz > headerLen && s.activeSz+int64(keyLen+4+len(raw)) > maxSegmentBytes {
		if err := s.rotateLocked(); err != nil {
			return nil, err
		}
	}
	offset := s.activeSz
	var frame bytes.Buffer
	if err := writeFrame(&frame, key, raw); err != nil {
		return nil, err
	}
	if _, err := s.active.Write(frame.Bytes()); err != nil {
		return nil, err
	}
	if err := s.active.Sync(); err != nil { // fsync blob BEFORE caller appends WAL
		return nil, err
	}
	s.activeSz += int64(frame.Len())
	s.index[string(key)] = location{segment: s.activeNum, offset: offset}
	return append([]byte(nil), key...), nil
}

// Get returns the raw bytes for key, verifying integrity by re-hashing.
func (s *Store) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	loc, ok := s.index[string(key)]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("rawstore: key %x not found", key)
	}
	f, err := os.Open(segmentPath(s.dir, loc.segment))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(loc.offset, 0); err != nil {
		return nil, err
	}
	gotKey, val, _, err := readFrame(f)
	if err != nil {
		return nil, err
	}
	sum := blake3.Sum256(val)
	if !bytes.Equal(sum[:], gotKey) || !bytes.Equal(gotKey, key) {
		return nil, fmt.Errorf("rawstore: integrity check failed for key %x", key)
	}
	return val, nil
}

// GC reclaims every blob whose key is not in reachable. Callers MUST invoke GC
// only after the new snapshot is durably renamed. It rewrites live blobs into a
// fresh segment, then removes the old ones.
func (s *Store) GC(reachable map[string]struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldSegs, err := listSegments(s.dir)
	if err != nil {
		return err
	}
	// Collect live (key, value) pairs.
	type kv struct {
		key, val []byte
	}
	var live []kv
	for k, loc := range s.index {
		if _, ok := reachable[k]; !ok {
			continue
		}
		f, err := os.Open(segmentPath(s.dir, loc.segment))
		if err != nil {
			return err
		}
		if _, err := f.Seek(loc.offset, 0); err != nil {
			_ = f.Close()
			return err
		}
		gk, val, _, err := readFrame(f)
		_ = f.Close()
		if err != nil {
			return err
		}
		live = append(live, kv{key: gk, val: val})
	}
	// Close active, write a single compacted segment numbered after the old max.
	if s.active != nil {
		_ = s.active.Close()
		s.active = nil
	}
	newNum := 1
	if len(oldSegs) > 0 {
		newNum = oldSegs[len(oldSegs)-1] + 1
	}
	newPath := segmentPath(s.dir, newNum)
	f, err := os.OpenFile(newPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if err := writeSegmentHeader(f); err != nil {
		_ = f.Close()
		return err
	}
	newIndex := make(map[string]location, len(live))
	offset := int64(headerLen)
	for _, e := range live {
		var frame bytes.Buffer
		if err := writeFrame(&frame, e.key, e.val); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(frame.Bytes()); err != nil {
			_ = f.Close()
			return err
		}
		newIndex[string(e.key)] = location{segment: newNum, offset: offset}
		offset += int64(frame.Len())
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	s.active = f
	s.activeNum = newNum
	s.activeSz = offset
	s.index = newIndex
	// Remove old segments only after the compacted one is durable.
	for _, num := range oldSegs {
		if err := os.Remove(segmentPath(s.dir, num)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (s *Store) rotateLocked() error {
	if s.active != nil {
		if err := s.active.Sync(); err != nil {
			return err
		}
		_ = s.active.Close()
	}
	s.activeNum++
	f, err := os.OpenFile(segmentPath(s.dir, s.activeNum), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if err := writeSegmentHeader(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	s.active = f
	s.activeSz = headerLen
	return nil
}

// Close fsyncs and closes the active segment.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == nil {
		return nil
	}
	if err := s.active.Sync(); err != nil {
		_ = s.active.Close()
		return err
	}
	err := s.active.Close()
	s.active = nil
	return err
}

func segmentPath(dir string, num int) string {
	return filepath.Join(dir, fmt.Sprintf("raw-%06d.rvs", num))
}

func listSegments(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var nums []int
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "raw-") || !strings.HasSuffix(e.Name(), ".rvs") {
			continue
		}
		stem := strings.TrimSuffix(strings.TrimPrefix(e.Name(), "raw-"), ".rvs")
		n, err := strconv.Atoi(stem)
		if err != nil {
			continue
		}
		nums = append(nums, n)
	}
	sort.Ints(nums)
	return nums, nil
}
