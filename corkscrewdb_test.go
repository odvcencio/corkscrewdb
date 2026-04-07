package corkscrewdb

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenCloseRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csdb")
	provider := &mockProvider{dim: 32}

	db, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs")
	if err := coll.Put("doc-1", Entry{Text: "hello world", Metadata: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	coll2 := db2.Collection("docs")
	results, err := coll2.Search("hello", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("after reopen: results = %v, want doc-1", results)
	}
}

func TestOpenCreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "test.csdb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}

func TestOpenDefaultProviderSupportsTextSearch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "default.csdb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("docs")
	if err := coll.Put("doc-1", Entry{Text: "the auth module uses passkeys", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	results, err := coll.Search("passkeys", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("results = %v, want doc-1", results)
	}
}

func TestOpenRejectsEmbeddingConfigMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mismatch.csdb")

	db, err := Open(path, WithProvider(&mockProvider{dim: 32}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, WithProvider(&mockProvider{dim: 16})); err == nil {
		t.Fatal("expected embedding config mismatch error")
	}
}

func TestOpenPersistsPeerConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.csdb")
	db, err := Open(path, WithPeers("node-a:4040", "node-b:4040"), WithToken("secret-token"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(path, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) == "" || !containsAll(string(data), "node-a:4040", "node-b:4040") {
		t.Fatalf("manifest missing peers: %s", string(data))
	}
	if containsAll(string(data), "secret-token") {
		t.Fatalf("token should not be persisted in manifest: %s", string(data))
	}
}

func TestRecoveryFromSnapshotAndWALTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tail.csdb")
	provider := &mockProvider{dim: 16}

	db, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll := db.Collection("docs")
	if err := coll.Put("doc-1", Entry{Text: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := coll.persistSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := coll.sync(); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("doc-2", Entry{Text: "bravo"}); err != nil {
		t.Fatal(err)
	}
	if err := coll.sync(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	results, err := db2.Collection("docs").Search("bravo", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 || results[0].ID != "doc-2" {
		t.Fatalf("results = %v, want doc-2 from WAL tail replay", results)
	}
}

func TestCloseWritesQuantizedIndexFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index-file.csdb")
	db, err := Open(path, WithProvider(&mockProvider{dim: 8}))
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs")
	if err := coll.Put("doc-1", Entry{Text: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	indexPath := filepath.Join(path, "collections", "docs", "index", "quantized.tqi")
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("expected quantized index file: %v", err)
	}
}

func TestEmbeddedLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lifecycle.csdb")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	coll := db.Collection("docs")

	if err := coll.Put("doc-1", Entry{Text: "webauthn passkeys are enabled", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	if err := coll.Put("doc-2", Entry{Text: "database migrations are append only", Metadata: map[string]string{"source": "design"}}); err != nil {
		t.Fatal(err)
	}
	filtered, err := coll.Search("passkeys", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 1 || filtered[0].ID != "doc-1" {
		t.Fatalf("filtered = %v, want doc-1", filtered)
	}
	history, err := coll.History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("history len = %d, want 1", len(history))
	}
	if err := coll.Delete("doc-1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	results, err := db2.Collection("docs").Search("passkeys", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("deleted review entry survived reopen: %v", results)
	}
	history, err = db2.Collection("docs").History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || !history[1].Tombstone {
		t.Fatalf("history after reopen = %+v", history)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

func TestConnectRemoteLifecycle(t *testing.T) {
	serverDB, addr := startRemoteTestServer(t, WithProvider(&mockProvider{dim: 16}), WithToken("secret"))
	_ = serverDB

	client, err := Connect(addr, WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	coll := client.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "alpha remote", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	history, err := coll.History("doc-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 {
		t.Fatalf("history len = %d, want 1", len(history))
	}
	firstClock := history[0].LamportClock

	if err := coll.Put("doc-1", Entry{Text: "beta remote", Metadata: map[string]string{"source": "review"}}); err != nil {
		t.Fatal(err)
	}
	results, err := coll.Search("beta", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID != "doc-1" {
		t.Fatalf("results = %v, want doc-1", results)
	}

	view := coll.At(firstClock)
	viewResults, err := view.Search("alpha", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(viewResults) != 1 || viewResults[0].Text != "alpha remote" {
		t.Fatalf("viewResults = %v, want alpha version", viewResults)
	}

	if err := coll.Delete("doc-1"); err != nil {
		t.Fatal(err)
	}
	results, err = coll.Search("beta", 5, Filter("source", "review"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("deleted remote result survived: %v", results)
	}
}

func TestConnectRequiresToken(t *testing.T) {
	_, addr := startRemoteTestServer(t, WithProvider(&mockProvider{dim: 8}), WithToken("secret"))
	client, err := Connect(addr, WithToken("wrong"))
	if err == nil || err.Error() != ErrUnauthorized.Error() {
		if client != nil {
			_ = client.Close()
		}
		t.Fatalf("err = %v, want %v", err, ErrUnauthorized)
	}
}

func startRemoteTestServer(t *testing.T, opts ...Option) (*DB, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "remote.csdb")
	db, err := Open(path, opts...)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- db.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		_ = db.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("serve did not exit")
		}
	})
	return db, listener.Addr().String()
}
