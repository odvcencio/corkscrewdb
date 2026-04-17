package main

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/odvcencio/corkscrewdb"
)

// newTestPrimary spins an in-process CorkScrewDB primary on a random
// local port and returns (address, token). Cleanup is registered via
// t.Cleanup so the listener and DB are torn down on test exit.
func newTestPrimary(t *testing.T) (string, string) {
	t.Helper()
	const token = "test-token"

	dir := filepath.Join(t.TempDir(), "primary.csdb")
	db, err := corkscrewdb.Open(dir, corkscrewdb.WithToken(token))
	if err != nil {
		t.Fatalf("open primary: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		db.Close()
		t.Fatalf("listen: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- db.Serve(listener) }()

	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("primary.Serve did not exit within 2s")
		}
		_ = db.Close()
	})

	return listener.Addr().String(), token
}

func TestDBClientsHappyPath(t *testing.T) {
	addr, token := newTestPrimary(t)

	cfg := Config{
		AddrRW:             addr,
		AddrRO:             addr,
		CorkscrewDBToken:   token,
		ExpectedProviderID: "manta-embed-v0",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clients, err := NewDBClients(ctx, cfg)
	if err != nil {
		t.Fatalf("NewDBClients: %v", err)
	}
	if clients == nil {
		t.Fatal("clients is nil")
	}
	if clients.WriteClient() == nil {
		t.Fatal("WriteClient nil")
	}
	if clients.ReadClient(false) == nil {
		t.Fatal("ReadClient(false) nil")
	}
	if clients.ReadClient(true) == nil {
		t.Fatal("ReadClient(true) nil")
	}
	if err := clients.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestDBClientsWrongProviderID(t *testing.T) {
	addr, token := newTestPrimary(t)

	cfg := Config{
		AddrRW:             addr,
		AddrRO:             addr,
		CorkscrewDBToken:   token,
		ExpectedProviderID: "wrong",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clients, err := NewDBClients(ctx, cfg)
	if err == nil {
		if clients != nil {
			clients.Close()
		}
		t.Fatal("expected provider ID mismatch error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "wrong") {
		t.Errorf("error must contain expected id %q: %v", "wrong", err)
	}
	if !strings.Contains(msg, "manta-embed-v0") {
		t.Errorf("error must contain actual id %q: %v", "manta-embed-v0", err)
	}
}

func TestDBClientsUnreachableRW(t *testing.T) {
	cfg := Config{
		AddrRW:             "127.0.0.1:1",
		AddrRO:             "127.0.0.1:1",
		CorkscrewDBToken:   "t",
		ExpectedProviderID: "manta-embed-v0",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	start := time.Now()
	clients, err := NewDBClients(ctx, cfg)
	elapsed := time.Since(start)

	if err == nil {
		if clients != nil {
			clients.Close()
		}
		t.Fatal("expected connect error, got nil")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("NewDBClients exceeded 2s budget: %v", elapsed)
	}
}

func TestDBClientsStalenessRouting(t *testing.T) {
	addrRW, token := newTestPrimary(t)
	addrRO, _ := newTestPrimary(t)

	cfg := Config{
		AddrRW:             addrRW,
		AddrRO:             addrRO,
		CorkscrewDBToken:   token,
		ExpectedProviderID: "manta-embed-v0",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clients, err := NewDBClients(ctx, cfg)
	if err != nil {
		t.Fatalf("NewDBClients: %v", err)
	}
	t.Cleanup(func() { _ = clients.Close() })

	rw := clients.WriteClient()
	roFresh := clients.ReadClient(false)
	roStale := clients.ReadClient(true)

	if rw == nil || roFresh == nil || roStale == nil {
		t.Fatal("nil client returned")
	}
	if roFresh != rw {
		t.Error("ReadClient(false) must route to rw")
	}
	if roStale == rw {
		t.Error("ReadClient(true) must not be the rw client when AddrRW != AddrRO")
	}
}
