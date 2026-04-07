package offload

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFSBackendPushListPullDelete(t *testing.T) {
	root := t.TempDir()
	b := NewFSBackend(root)

	data := []byte("hello cold storage")
	if err := b.Push("wal/000001.wal", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	keys, err := b.List("wal/000001.wal")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "wal/000001.wal" {
		t.Fatalf("keys = %v, want [wal/000001.wal]", keys)
	}

	var buf bytes.Buffer
	if err := b.Pull("wal/000001.wal", &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "hello cold storage" {
		t.Fatalf("pulled = %q", buf.String())
	}

	if err := b.Delete("wal/000001.wal"); err != nil {
		t.Fatal(err)
	}
	keys, err = b.List("wal/000001.wal")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("keys after delete = %v", keys)
	}
}

func TestManagerPushNow(t *testing.T) {
	dbDir := t.TempDir()
	coldDir := t.TempDir()

	// Create a fake collection with WAL segments and a snapshot.
	walDir := filepath.Join(dbDir, "collections", "docs", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Sealed segment.
	if err := os.WriteFile(filepath.Join(walDir, "000001.wal"), []byte("sealed"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Active segment (should NOT be pushed).
	if err := os.WriteFile(filepath.Join(walDir, "000002.wal"), []byte("active"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Snapshot.
	collDir := filepath.Join(dbDir, "collections", "docs")
	if err := os.WriteFile(filepath.Join(collDir, "snapshot-00042.csdb"), []byte("snap"), 0o644); err != nil {
		t.Fatal(err)
	}

	mgr, err := NewManager(Config{
		Backend: NewFSBackend(coldDir),
		DBPath:  dbDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := mgr.PushNow(); err != nil {
		t.Fatal(err)
	}

	// Check sealed WAL was pushed.
	data, err := os.ReadFile(filepath.Join(coldDir, "collections", "docs", "wal", "000001.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "sealed" {
		t.Fatalf("sealed content = %q", string(data))
	}

	// Check active WAL was NOT pushed.
	if _, err := os.Stat(filepath.Join(coldDir, "collections", "docs", "wal", "000002.wal")); !os.IsNotExist(err) {
		t.Fatal("active WAL segment should not be pushed")
	}

	// Check snapshot was pushed.
	data, err = os.ReadFile(filepath.Join(coldDir, "collections", "docs", "snapshot-00042.csdb"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "snap" {
		t.Fatalf("snapshot content = %q", string(data))
	}
}

func TestManagerIdempotent(t *testing.T) {
	dbDir := t.TempDir()
	coldDir := t.TempDir()

	walDir := filepath.Join(dbDir, "collections", "docs", "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "000001.wal"), []byte("sealed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "000002.wal"), []byte("active"), 0o644); err != nil {
		t.Fatal(err)
	}

	mgr, err := NewManager(Config{
		Backend: NewFSBackend(coldDir),
		DBPath:  dbDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := mgr.PushNow(); err != nil {
		t.Fatal(err)
	}
	// Second push should be a no-op.
	if err := mgr.PushNow(); err != nil {
		t.Fatal(err)
	}
}

func TestManagerRequiresConfig(t *testing.T) {
	if _, err := NewManager(Config{}); err == nil {
		t.Fatal("expected error for empty config")
	}
	if _, err := NewManager(Config{Backend: NewFSBackend(".")}); err == nil {
		t.Fatal("expected error for missing db path")
	}
}
