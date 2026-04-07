package wal

import (
	"os"
	"path/filepath"
	"sync"
)

// Writer appends WAL entries to a single segment.
type Writer struct {
	mu     sync.Mutex
	file   *os.File
	closed bool
}

func NewWriter(path string) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{file: file}, nil
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
	_, err := w.file.Write(data)
	return err
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
