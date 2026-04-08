package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const defaultSegmentBytes int64 = 8 << 20

// Manager owns the active WAL segment and rotates when it grows too large.
type Manager struct {
	mu              sync.Mutex
	dir             string
	maxSegmentBytes int64
	syncMode        SyncMode
	activeNumber    int
	activePath      string
	activeSize      int64
	writer          *Writer
}

// ManagerConfig configures WAL segment management.
type ManagerConfig struct {
	Dir             string
	MaxSegmentBytes int64
	SyncMode        SyncMode
}

func NewManager(dir string, maxSegmentBytes int64) (*Manager, error) {
	return NewManagerWithConfig(ManagerConfig{
		Dir:             dir,
		MaxSegmentBytes: maxSegmentBytes,
	})
}

func NewManagerWithConfig(cfg ManagerConfig) (*Manager, error) {
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = defaultSegmentBytes
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	segments, err := ListSegments(cfg.Dir)
	if err != nil {
		return nil, err
	}
	next := nextSegmentNumber(segments)
	path := segmentPath(cfg.Dir, next)
	writer, err := NewWriterWithSync(path, cfg.SyncMode)
	if err != nil {
		return nil, err
	}
	return &Manager{
		dir:             cfg.Dir,
		maxSegmentBytes: cfg.MaxSegmentBytes,
		syncMode:        cfg.SyncMode,
		activeNumber:    next,
		activePath:      path,
		writer:          writer,
	}, nil
}

func (m *Manager) Append(entry Entry) error {
	data, err := entry.MarshalBinary()
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return os.ErrClosed
	}
	if m.activeSize > 0 && m.activeSize+int64(len(data)) > m.maxSegmentBytes {
		if err := m.rotateLocked(); err != nil {
			return err
		}
	}
	if err := m.writer.appendEncoded(data); err != nil {
		return err
	}
	m.activeSize += int64(len(data))
	return nil
}

func (m *Manager) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return nil
	}
	return m.writer.Sync()
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer == nil {
		return nil
	}
	err := m.writer.Close()
	m.writer = nil
	return err
}

func (m *Manager) ActivePath() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activePath
}

func ListSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	segments := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		segments = append(segments, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(segments)
	return segments, nil
}

func (m *Manager) rotateLocked() error {
	if err := m.writer.Close(); err != nil {
		return err
	}
	m.activeNumber++
	m.activePath = segmentPath(m.dir, m.activeNumber)
	writer, err := NewWriterWithSync(m.activePath, m.syncMode)
	if err != nil {
		return err
	}
	m.writer = writer
	m.activeSize = 0
	return nil
}

func nextSegmentNumber(paths []string) int {
	if len(paths) == 0 {
		return 1
	}
	base := filepath.Base(paths[len(paths)-1])
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	n, err := strconv.Atoi(stem)
	if err != nil {
		return len(paths) + 1
	}
	return n + 1
}

func segmentPath(dir string, number int) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.wal", number))
}
