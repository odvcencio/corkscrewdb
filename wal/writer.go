package wal

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// SyncMode controls when the WAL writer calls fsync.
type SyncMode int

const (
	// SyncEvery fsyncs after every append. Safe but slower.
	SyncEvery SyncMode = iota
	// SyncOnRotate fsyncs only on segment rotation and close. Faster but
	// the last few entries can be lost on crash.
	SyncOnRotate
)

// Writer appends WAL entries to a single segment.
type Writer struct {
	mu       sync.Mutex
	file     *os.File
	syncMode SyncMode
	closed   bool
}

func NewWriter(path string) (*Writer, error) {
	return NewWriterWithSync(path, SyncEvery)
}

func NewWriterWithSync(path string, mode SyncMode) (*Writer, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Detect whether we are creating a brand-new segment so we can fsync the
	// parent directory once (on create), making the new dirent durable. We do
	// not dir-fsync when reopening an existing segment.
	created := false
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			created = true
		} else {
			return nil, err
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if created {
		if err := fsyncDir(dir); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return &Writer{file: file, syncMode: mode}, nil
}

// fsyncDir fsyncs a directory so that a newly created segment's directory entry
// (filename) is durable, mirroring the snapshot/manifest writers' dir-fsync.
// On filesystems lacking directory fsync support the syscall may return
// EINVAL/ENOTSUP; that is treated as a non-fatal no-op.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil
		}
		return err
	}
	return d.Close()
}

func (w *Writer) Append(entry Entry) error {
	data, err := entry.MarshalBinary()
	if err != nil {
		return err
	}
	return w.appendEncoded(data)
}

func (w *Writer) appendEncoded(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return os.ErrClosed
	}
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	if w.syncMode == SyncEvery {
		return w.file.Sync()
	}
	return nil
}

func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	return w.file.Sync()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}
