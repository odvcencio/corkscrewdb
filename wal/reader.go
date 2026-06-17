package wal

import (
	"errors"
	"io"
	"os"
)

// Reader replays entries from one WAL segment.
type Reader struct {
	file   *os.File
	path   string
	offset int64 // start offset of the next entry to read
	entry  Entry
	err    error
}

func NewReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{file: file, path: path}, nil
}

func (r *Reader) Next() bool {
	if r.err != nil {
		return false
	}
	start := r.offset
	entry, n, err := readEntryCounting(r.file)
	if err == nil {
		r.entry = entry
		r.offset += n
		return true
	}
	// Classify the failure at `start`.
	if errors.Is(err, io.EOF) {
		// Clean tail: frame began exactly at EOF (nothing consumed).
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		// Truncated trailing frame: partial bytes, file ends mid-entry.
		return false
	}
	// Interior corruption: CRC/format failure with a fully-present frame.
	r.err = &ErrWALCorrupt{Segment: r.path, Offset: start, Err: err}
	return false
}

func (r *Reader) Entry() Entry { return r.entry }
func (r *Reader) Err() error   { return r.err }

func (r *Reader) Close() error {
	if r.file == nil {
		return nil
	}
	return r.file.Close()
}
