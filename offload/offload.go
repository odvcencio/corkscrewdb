// Package offload provides cold storage push for sealed WAL segments and snapshots.
//
// The default build includes a filesystem backend for testing. S3 and GCS
// backends are planned behind build tags (csdb_offload_s3, csdb_offload_gcs).
package offload

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Backend is the interface for pushing files to cold storage.
type Backend interface {
	// Push copies a local file to cold storage under the given key.
	Push(key string, reader io.Reader) error
	// List returns keys under a prefix, sorted lexicographically.
	List(prefix string) ([]string, error)
	// Pull copies a cold storage object to a local writer.
	Pull(key string, writer io.Writer) error
	// Delete removes a cold storage object.
	Delete(key string) error
}

// Config configures the offload manager.
type Config struct {
	Backend  Backend
	DBPath   string
	Interval time.Duration
}

// Manager periodically pushes sealed WAL segments and snapshots to cold storage.
type Manager struct {
	backend  Backend
	dbPath   string
	interval time.Duration

	mu      sync.Mutex
	stopCh  chan struct{}
	doneCh  chan struct{}
	running bool
}

// NewManager creates an offload manager.
func NewManager(cfg Config) (*Manager, error) {
	if cfg.Backend == nil {
		return nil, errors.New("offload: backend is required")
	}
	if cfg.DBPath == "" {
		return nil, errors.New("offload: db path is required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Minute
	}
	return &Manager{
		backend:  cfg.Backend,
		dbPath:   cfg.DBPath,
		interval: cfg.Interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}, nil
}

// Start begins the periodic offload loop.
func (m *Manager) Start() {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.mu.Unlock()
	go m.loop()
}

// Stop halts the offload loop.
func (m *Manager) Stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	close(m.stopCh)
	<-m.doneCh
}

// PushNow performs a single offload pass immediately.
func (m *Manager) PushNow() error {
	return m.pushAll()
}

func (m *Manager) loop() {
	defer close(m.doneCh)
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			_ = m.pushAll()
		}
	}
}

func (m *Manager) pushAll() error {
	collectionsDir := filepath.Join(m.dbPath, "collections")
	entries, err := os.ReadDir(collectionsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var errs []error
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if err := m.pushCollection(name); err != nil {
			errs = append(errs, fmt.Errorf("offload %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) pushCollection(name string) error {
	var errs []error

	// Push sealed WAL segments (all except the last/active one).
	walDir := filepath.Join(m.dbPath, "collections", name, "wal")
	walFiles, err := listFiles(walDir, ".wal")
	if err != nil {
		return err
	}
	if len(walFiles) > 1 {
		sealed := walFiles[:len(walFiles)-1]
		for _, path := range sealed {
			key := fmt.Sprintf("collections/%s/wal/%s", name, filepath.Base(path))
			if err := m.pushFile(key, path); err != nil {
				errs = append(errs, err)
			}
		}
	}

	// Push snapshots.
	collDir := filepath.Join(m.dbPath, "collections", name)
	snapFiles, err := listFiles(collDir, ".csdb")
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for _, path := range snapFiles {
		key := fmt.Sprintf("collections/%s/%s", name, filepath.Base(path))
		if err := m.pushFile(key, path); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (m *Manager) pushFile(key, localPath string) error {
	existing, err := m.backend.List(key)
	if err != nil {
		return err
	}
	for _, k := range existing {
		if k == key {
			return nil // already pushed
		}
	}

	file, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer file.Close()
	return m.backend.Push(key, file)
}

func listFiles(dir, suffix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), suffix) {
			continue
		}
		paths = append(paths, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

// FSBackend implements Backend using a local directory (for testing/dev).
type FSBackend struct {
	root string
}

// NewFSBackend creates a filesystem-backed cold storage backend.
func NewFSBackend(root string) *FSBackend {
	return &FSBackend{root: root}
}

func (b *FSBackend) Push(key string, reader io.Reader) error {
	path := filepath.Join(b.root, key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, reader)
	return err
}

func (b *FSBackend) List(prefix string) ([]string, error) {
	dir := filepath.Join(b.root, filepath.Dir(prefix))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var keys []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		key := filepath.Join(filepath.Dir(prefix), entry.Name())
		if strings.HasPrefix(key, prefix) || key == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (b *FSBackend) Pull(key string, writer io.Writer) error {
	file, err := os.Open(filepath.Join(b.root, key))
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(writer, file)
	return err
}

func (b *FSBackend) Delete(key string) error {
	return os.Remove(filepath.Join(b.root, key))
}
