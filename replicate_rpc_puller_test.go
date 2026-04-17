package corkscrewdb

import (
	"errors"
	"net"
	"path/filepath"
	"testing"

	"github.com/odvcencio/corkscrewdb/replica"
)

// TestRPCPullerForwardsToken asserts RPCPuller.PullEntries and
// RPCPuller.PullSnapshot forward req.Token through to the primary. The
// puller is built over a real Connect'd client so this exercises the full
// RPC path: if req.Token is dropped anywhere between the Puller and the
// server's authorize(), an explicit wrong token on the PullRequest would
// be silently replaced by the connection's default and the call would
// succeed — the test asserts the opposite.
func TestRPCPullerForwardsToken(t *testing.T) {
	const primaryToken = "puller-token"

	primaryPath := filepath.Join(t.TempDir(), "primary.csdb")
	primaryDB, err := Open(primaryPath, WithProvider(&mockProvider{dim: 8}), WithToken(primaryToken))
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}
	defer primaryDB.Close()

	// Seed so PullSnapshot has something real to return.
	if err := primaryDB.Collection("docs", WithBitWidth(2)).Put("doc-1", Entry{Text: "hello"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	serveDone := make(chan error, 1)
	go func() { serveDone <- primaryDB.Serve(listener) }()
	defer func() {
		_ = listener.Close()
		<-serveDone
	}()

	// Connect with the primary's token so the initial Info handshake passes.
	client, err := Connect(listener.Addr().String(), WithToken(primaryToken))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	puller, err := NewRPCPuller(client)
	if err != nil {
		t.Fatalf("new puller: %v", err)
	}

	// Correct token on the request: both calls succeed.
	if _, err := puller.PullEntries(replica.PullRequest{
		Token:      primaryToken,
		Collection: "docs",
		MaxEntries: 10,
	}); err != nil {
		t.Fatalf("PullEntries with correct token: %v", err)
	}
	if _, err := puller.PullSnapshot(replica.SnapshotRequest{
		Token:      primaryToken,
		Collection: "docs",
	}); err != nil {
		t.Fatalf("PullSnapshot with correct token: %v", err)
	}

	// Wrong token on the request: since req.Token is actually forwarded,
	// it overrides the connection's default and the server rejects the call.
	// Before the fix, req.Token was dropped and the connection token
	// silently authenticated these calls.
	if _, err := puller.PullEntries(replica.PullRequest{
		Token:      "very-wrong",
		Collection: "docs",
		MaxEntries: 10,
	}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("PullEntries wrong req token: got %v, want ErrUnauthorized", err)
	}
	if _, err := puller.PullSnapshot(replica.SnapshotRequest{
		Token:      "very-wrong",
		Collection: "docs",
	}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("PullSnapshot wrong req token: got %v, want ErrUnauthorized", err)
	}
}
