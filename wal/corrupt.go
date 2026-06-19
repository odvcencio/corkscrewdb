package wal

import "fmt"

// ErrWALCorrupt reports a CRC/format failure in the interior of a WAL segment
// (i.e. valid-looking bytes follow the failed entry), distinguishing real
// corruption from a clean or truncated tail.
type ErrWALCorrupt struct {
	Segment string
	Offset  int64
	Err     error
}

func (e *ErrWALCorrupt) Error() string {
	return fmt.Sprintf("wal: corruption in %s at offset %d: %v", e.Segment, e.Offset, e.Err)
}

func (e *ErrWALCorrupt) Unwrap() error { return e.Err }
