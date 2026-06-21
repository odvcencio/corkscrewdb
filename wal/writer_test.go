package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriterAppendAndReaderReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.wal")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{Kind: EntryPut, CollectionID: "docs", VectorID: "a", Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{1}}, Dim: 3, LamportClock: 1, ActorID: "x", WallClock: time.Now().UTC()},
		{Kind: EntryPut, CollectionID: "docs", VectorID: "b", Quantized: &QuantizedVector{MSE: []byte{2}, Signs: []byte{2}}, Dim: 3, LamportClock: 2, ActorID: "x", WallClock: time.Now().UTC()},
		{Kind: EntryTombstone, CollectionID: "docs", VectorID: "a", LamportClock: 3, ActorID: "x", WallClock: time.Now().UTC()},
	}
	for _, entry := range entries {
		if err := w.Append(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var replayed []Entry
	for r.Next() {
		replayed = append(replayed, r.Entry())
	}
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if len(replayed) != 3 {
		t.Fatalf("replayed %d entries, want 3", len(replayed))
	}
	if replayed[2].Kind != EntryTombstone {
		t.Fatalf("entry 2 kind = %d, want tombstone", replayed[2].Kind)
	}
}

func TestWriterCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("wal file not created: %v", err)
	}
}

// TestWriterDirFsyncOnCreate verifies the create path (which now fsyncs the
// parent directory so the new segment's dirent is durable) succeeds and that
// reopening an existing segment does not error. We cannot observe the fsync
// syscall directly, but exercising both branches guards against regressions.
func TestWriterDirFsyncOnCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.wal")
	w, err := NewWriterWithSync(path, SyncEvery) // create branch: dir-fsynced
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := NewWriterWithSync(path, SyncEvery) // reopen branch: no dir-fsync
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestFsyncDir exercises the directory-fsync helper directly; it must succeed
// on a real directory and error on a missing one. EINVAL/ENOTSUP from
// filesystems lacking dir-fsync are swallowed as a non-fatal no-op.
func TestFsyncDir(t *testing.T) {
	dir := t.TempDir()
	if err := fsyncDir(dir); err != nil {
		t.Fatalf("fsyncDir on a real directory failed: %v", err)
	}
	if err := fsyncDir(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("fsyncDir on a missing directory should error")
	}
}
