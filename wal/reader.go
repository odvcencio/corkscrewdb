package wal

import (
	"errors"
	"io"
	"os"
)

// Reader replays entries from one WAL segment.
type Reader struct {
	file  *os.File
	entry Entry
	err   error
}

func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{file: file}, nil
}

func (r *Reader) Next() bool {
	if r.err != nil {
		return false
	}
	entry, err := ReadEntry(r.file)
	if err == nil {
		r.entry = entry
		return true
	}
	if errors.Is(err, io.EOF) {
		return false
	}
	r.err = err
	return false
}

func (r *Reader) Entry() Entry {
	return r.entry
}

func (r *Reader) Err() error {
	return r.err
}

func (r *Reader) Close() error {
	if r.file == nil {
		return nil
	}
	return r.file.Close()
}
