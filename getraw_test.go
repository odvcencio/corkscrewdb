package corkscrewdb

import (
	"bytes"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"lukechampine.com/blake3"
)

// startServedPrimary opens a DB, serves it over a loopback listener, and returns
// the DB and its serve address. Cleanup is registered on t.
func startServedPrimary(t *testing.T, dim int) (*DB, string) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "primary.csdb"), WithProvider(&mockProvider{dim: dim}), WithToken("secret"))
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = db.Serve(listener) }()
	t.Cleanup(func() {
		_ = listener.Close()
		_ = db.Close()
	})
	return db, listener.Addr().String()
}

// newPeerFollower opens a local follower DB that knows the primary as a peer so
// fetchRawByHash can dial it, while keeping its own local raw store.
func newPeerFollower(t *testing.T, dim int, peerAddr string) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "follower.csdb"),
		WithProvider(&mockProvider{dim: dim}), WithToken("secret"), WithPeers(peerAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestGetRawRoundTrip(t *testing.T) {
	primary, addr := startServedPrimary(t, 8)
	coll := primary.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "raw round trip"}); err != nil {
		t.Fatal(err)
	}
	pr := primary.streamer.Pull("docs", 0, 100)
	if len(pr.Entries) != 1 || len(pr.Entries[0].RawHash) != 32 {
		t.Fatalf("expected one entry with a raw hash, got %+v", pr.Entries)
	}
	hash := pr.Entries[0].RawHash

	follower := newPeerFollower(t, 8, addr)
	follower.Collection("docs", WithBitWidth(2))

	raw, err := follower.fetchRawByHash("docs", hash)
	if err != nil {
		t.Fatalf("fetchRawByHash: %v", err)
	}
	sum := blake3.Sum256(raw)
	if !bytes.Equal(sum[:], hash) {
		t.Fatalf("returned bytes do not re-hash to the requested key")
	}

	// Second call must be served from the local store (idempotent dedup).
	stored, err := follower.Collection("docs").getRaw(hash)
	if err != nil {
		t.Fatalf("local getRaw after fetch: %v", err)
	}
	if !bytes.Equal(stored, raw) {
		t.Fatalf("local store did not retain fetched raw bytes")
	}
}

func TestGetRawMissingIsNotFound(t *testing.T) {
	_, addr := startServedPrimary(t, 8)
	follower := newPeerFollower(t, 8, addr)
	follower.Collection("docs", WithBitWidth(2))

	absent := make([]byte, 32)
	for i := range absent {
		absent[i] = byte(i)
	}
	_, err := follower.fetchRawByHash("docs", absent)
	if err == nil {
		t.Fatal("expected error for absent hash")
	}
	if !errors.Is(err, ErrRawUnavailable) {
		t.Fatalf("err = %v, want errors.Is ErrRawUnavailable (terminal)", err)
	}
}

func TestGetRawIntegrityIsRetriable(t *testing.T) {
	primary, addr := startServedPrimary(t, 8)
	coll := primary.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "integrity test"}); err != nil {
		t.Fatal(err)
	}
	pr := primary.streamer.Pull("docs", 0, 100)
	hash := pr.Entries[0].RawHash

	// Corrupt the primary's stored raw bytes on disk so Get returns ErrIntegrity.
	rawDir := primary.rawStoreDir("docs")
	seg := filepath.Join(rawDir, "raw-000001.rvs")
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatalf("read raw segment: %v", err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(seg, raw, 0o644); err != nil {
		t.Fatalf("write corrupt segment: %v", err)
	}

	follower := newPeerFollower(t, 8, addr)
	follower.Collection("docs", WithBitWidth(2))

	_, err = follower.fetchRawByHash("docs", hash)
	if err == nil {
		t.Fatal("expected integrity error")
	}
	// Integrity is retriable: it must NOT be classified as the terminal
	// ErrRawUnavailable (not-found).
	if errors.Is(err, ErrRawUnavailable) {
		t.Fatalf("integrity error wrongly classified as terminal not-found: %v", err)
	}
}

func TestGetRawReturnedBytesMismatchRejected(t *testing.T) {
	primary, addr := startServedPrimary(t, 8)
	coll := primary.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "mismatch test"}); err != nil {
		t.Fatal(err)
	}
	pr := primary.streamer.Pull("docs", 0, 100)
	realHash := pr.Entries[0].RawHash

	// A fabricated hash the server does not have: server returns NotFound, the
	// follower never stores bytes under it.
	wrong := append([]byte(nil), realHash...)
	wrong[0] ^= 0xFF

	follower := newPeerFollower(t, 8, addr)
	follower.Collection("docs", WithBitWidth(2))

	if _, err := follower.fetchRawByHash("docs", wrong); err == nil {
		t.Fatal("expected error fetching a hash the primary lacks")
	}
	// The follower must not have stored anything under the wrong key: getRaw
	// returns a not-found error rather than bytes.
	if _, err := follower.Collection("docs").getRaw(wrong); err == nil {
		t.Fatalf("follower wrongly stored bytes under a mismatched key")
	}
}

// TestGetRawServerErrorCodes proves the server maps a missing key to a terminal
// (not-found) client error via the gRPC code round-trip.
func TestGetRawServerErrorCodes(t *testing.T) {
	primary, addr := startServedPrimary(t, 8)
	coll := primary.Collection("docs", WithBitWidth(2))
	if err := coll.Put("doc-1", Entry{Text: "code mapping"}); err != nil {
		t.Fatal(err)
	}

	client, err := newGRPCClient(addr, "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	absent := make([]byte, 32)
	if _, err := client.GetRaw("docs", absent); !errors.Is(err, ErrRawUnavailable) {
		t.Fatalf("absent hash err = %v, want errors.Is ErrRawUnavailable", err)
	}
}
