package wal

import (
	"os"
	"path/filepath"
	"sync"
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
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{file: file, syncMode: mode}, nil
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
