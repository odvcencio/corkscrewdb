package corkscrewdb

import (
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestImportOwnedReconstructsCodes drives a real peer snapshot+entries handoff
// through importOwnedSnapshot/importOwnedEntries and asserts only keys owned
// under L1 land on the gainer, with codes intact and searchable. Raw vectors
// are fetched by hash for raw_store collections (never inlined).
func TestImportOwnedReconstructsCodes(t *testing.T) {
	// Peer (source) node: serves a collection with two keys.
	peerDB, peerAddr := startRemoteTestServer(t, WithProvider(&mockProvider{dim: 16}))
	peerDB.registerServeAddr(peerAddr)

	peerColl := peerDB.Collection("docs", WithBitWidth(2))
	// Pick two ids: one we will hand to the local node (L1 owner = local), one
	// that stays with the peer.
	maxKey := ^uint64(0)
	mid := maxKey / 2
	l0 := []ShardAssignment{
		{ID: "s-local", Owner: "local-node", Start: 0, End: mid},
		{ID: "s-peer", Owner: peerAddr, Start: mid + 1, End: maxKey},
	}
	l1 := []ShardAssignment{
		{ID: "s-local", Owner: "local-node", Start: 0, End: maxKey}, // local gains everything
	}

	// Find one id owned by the peer under L0 (so the peer holds it) that moves
	// to local under L1, and one that ... under L1 local owns everything, so
	// every peer key moves. Use two distinct peer-owned ids.
	var k1, k2 string
	for i := 0; i < 100000 && (k1 == "" || k2 == ""); i++ {
		id := "doc-imp-" + strconv.Itoa(i)
		key := shardKey("docs", id)
		if key > mid {
			if k1 == "" {
				k1 = id
			} else if k2 == "" {
				k2 = id
			}
		}
	}
	if k1 == "" || k2 == "" {
		t.Fatal("could not find two peer-owned ids")
	}
	if err := peerColl.PutVector(k1, []float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := peerColl.PutVector(k2, []float32{0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}

	// Local (gainer) node, registered with a stable id and a peer client.
	localDB, err := Open(filepath.Join(t.TempDir(), "gainer.csdb"),
		WithProvider(&mockProvider{dim: 16}), WithPeers(peerAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer localDB.Close()
	localDB.serveAddr = "local-node"

	client, err := localDB.peerClient(peerAddr)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.PullSnapshot(RPCPullSnapshotRequest{Collection: "docs"})
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.RawStore {
		t.Fatal("peer snapshot RawStore = false, want true (default raw-store collection)")
	}

	oldMembers := []string{"local-node", peerAddr}
	if err := localDB.importOwnedSnapshot(peerAddr, RPCCollectionInfo{Name: "docs", BitWidth: snapshot.BitWidth}, snapshot, l0, oldMembers, l1); err != nil {
		t.Fatalf("importOwnedSnapshot err = %v", err)
	}

	// Both keys are now owned by local under L1 and must have landed with codes.
	for _, id := range []string{k1, k2} {
		hist, err := localDB.Collection("docs").historyFor(id, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(hist) != 1 {
			t.Fatalf("imported key %q history len = %d, want 1", id, len(hist))
		}
		if hist[0].Quantized == nil {
			t.Fatalf("imported key %q lost its codes", id)
		}
		if len(hist[0].RawHash) != 32 {
			t.Fatalf("imported key %q lost its raw hash", id)
		}
		// Raw fetched by hash and stored locally.
		raw, err := localDB.Collection("docs").getRaw(hist[0].RawHash)
		if err != nil {
			t.Fatalf("imported raw for %q not local: %v", id, err)
		}
		if len(raw) == 0 {
			t.Fatalf("imported raw for %q is empty", id)
		}
	}
}

// TestLegacyMemberFencedRebalanceRejected asserts that OrchestrateRebalance
// rejects a fenced cluster rebalance whose L0/L1 diff would fall through to
// legacy peer-hash-mod for a moving key (no explicit shard ranges).
func TestLegacyMemberFencedRebalanceRejected(t *testing.T) {
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listenerA.Close()
	addrA := listenerA.Addr().String()

	// No explicit shards: routing is entirely legacy peer-hash-mod.
	dbA, err := Open(filepath.Join(t.TempDir(), "legacy.csdb"),
		WithProvider(&mockProvider{dim: 16}), WithPeers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	dbA.registerServeAddr(addrA)

	done := make(chan error, 1)
	go func() { done <- dbA.Serve(listenerA) }()
	t.Cleanup(func() {
		_ = listenerA.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("serve A did not exit")
		}
	})

	// The CURRENT layout is empty (legacy peer-hash-mod), so any moving key's
	// L0 owner is resolved via legacy members — the symmetric diff cannot be
	// expressed as explicit ranges. A full-range explicit L1 is still rejected.
	maxKey := ^uint64(0)
	mid := maxKey / 2
	newLayout := []ShardAssignment{
		{ID: "s-a", Owner: addrA, Start: 0, End: mid},
		{ID: "s-b", Owner: "127.0.0.1:1", Start: mid + 1, End: maxKey},
	}
	err = dbA.OrchestrateRebalance(newLayout...)
	if !errors.Is(err, ErrLegacyRebalanceUnsafe) {
		t.Fatalf("OrchestrateRebalance with legacy-fallthrough diff err = %v, want ErrLegacyRebalanceUnsafe", err)
	}
}
