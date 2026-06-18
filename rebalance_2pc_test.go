package corkscrewdb

import (
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"sync"
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

// servedNode is a served test DB plus its address and a cleanup-registered
// listener.
type servedNode struct {
	db   *DB
	addr string
}

// startServedNode opens a served DB at addr-bound listener with the given
// options and registers cleanup.
func startServedNode(t *testing.T, name string, opts ...Option) *servedNode {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	db, err := Open(filepath.Join(t.TempDir(), name+".csdb"), opts...)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	db.registerServeAddr(addr)
	done := make(chan error, 1)
	go func() { done <- db.Serve(listener) }()
	t.Cleanup(func() {
		_ = listener.Close()
		_ = db.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("serve %s did not exit", name)
		}
	})
	return &servedNode{db: db, addr: addr}
}

// recordingClient wraps a remoteClient and appends a label to *log on each 2PC
// control-plane call, so a test can assert call ordering (e.g. all Freeze ACKs
// precede any Pull).
type recordingClient struct {
	remoteClient
	mu  *sync.Mutex
	log *[]string
}

func (r *recordingClient) record(label string) {
	r.mu.Lock()
	*r.log = append(*r.log, label)
	r.mu.Unlock()
}

func (r *recordingClient) FreezeRebalance(shards []ShardAssignment, epoch uint64, coordinator string) error {
	r.record("freeze")
	return r.remoteClient.FreezeRebalance(shards, epoch, coordinator)
}

func (r *recordingClient) PullSnapshot(req RPCPullSnapshotRequest) (RPCPullSnapshotResponse, error) {
	r.record("pull-snapshot")
	return r.remoteClient.PullSnapshot(req)
}

func (r *recordingClient) PullEntries(req RPCPullEntriesRequest) (RPCPullEntriesResponse, error) {
	r.record("pull-entries")
	return r.remoteClient.PullEntries(req)
}

func (r *recordingClient) CommitRebalance(shards []ShardAssignment, epoch uint64) error {
	r.record("commit")
	return r.remoteClient.CommitRebalance(shards, epoch)
}

func (r *recordingClient) AbortRebalance(epoch uint64) error {
	r.record("abort")
	return r.remoteClient.AbortRebalance(epoch)
}

// wrapPeerClient replaces the cached peer client for addr with a recording
// wrapper that logs into *log. The peer must already be dialable.
func wrapPeerClient(t *testing.T, db *DB, addr string, mu *sync.Mutex, log *[]string) {
	t.Helper()
	inner, err := db.peerClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	db.peerClients[addr] = &recordingClient{remoteClient: inner, mu: mu, log: log}
	db.mu.Unlock()
}

// orchestrateLayouts builds a 2-node old/new layout that moves the cut so a
// band of keys hands off from B to A (A gains, B loses).
func orchestrateLayouts(addrA, addrB string) (oldA, newAll []ShardAssignment) {
	maxKey := ^uint64(0)
	oldCut := maxKey / 2
	newCut := (maxKey / 4) * 3 // A's range grows from [0,oldCut] to [0,newCut]
	oldA = []ShardAssignment{
		{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: addrB, Start: oldCut + 1, End: maxKey},
	}
	newAll = []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: newCut},
		{ID: "shard-b", Owner: addrB, Start: newCut + 1, End: maxKey},
	}
	return oldA, newAll
}

func TestFreezeBeforePullOrdersBarrier(t *testing.T) {
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addrBPre := listenerB.Addr().String()
	_ = listenerB.Close()

	nodeA := startServedNode(t, "barrier-a", WithProvider(&mockProvider{dim: 16}), WithToken("s"), WithPeers(addrBPre))
	addrA := nodeA.addr
	oldA, newAll := orchestrateLayouts(addrA, addrBPre)
	// Reconfigure A's manifest shards to oldA now that we know addrA.
	if err := nodeA.db.applyShardLayout(oldA, 0); err != nil {
		t.Fatal(err)
	}

	// Node B bound to the pre-reserved port.
	lB, err := net.Listen("tcp", addrBPre)
	if err != nil {
		t.Fatal(err)
	}
	addrB := lB.Addr().String()
	dbB, err := Open(filepath.Join(t.TempDir(), "barrier-b.csdb"), WithProvider(&mockProvider{dim: 16}), WithToken("s"), WithPeers(addrA))
	if err != nil {
		t.Fatal(err)
	}
	dbB.registerServeAddr(addrB)
	oldB := []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: oldA[0].Start, End: oldA[0].End},
		{ID: "shard-b", Owner: LocalShardOwner, Start: oldA[1].Start, End: oldA[1].End},
	}
	if err := dbB.applyShardLayout(oldB, 0); err != nil {
		t.Fatal(err)
	}
	doneB := make(chan error, 1)
	go func() { doneB <- dbB.Serve(lB) }()
	t.Cleanup(func() {
		_ = lB.Close()
		_ = dbB.Close()
		select {
		case <-doneB:
		case <-time.After(2 * time.Second):
			t.Errorf("serve B did not exit")
		}
	})

	// Seed a moving key on B (B owns it under L0, A gains it under L1).
	var movingID string
	for i := 0; i < 100000; i++ {
		id := "doc-bar-" + strconv.Itoa(i)
		key := shardKey("docs", id)
		if key > oldA[0].End && key <= newAll[0].End {
			movingID = id
			break
		}
	}
	if movingID == "" {
		t.Fatal("no moving key found")
	}
	if err := dbB.Collection("docs", WithBitWidth(2)).PutVector(movingID, unitVec(16, 0)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var log []string
	wrapPeerClient(t, nodeA.db, addrB, &mu, &log)

	if err := nodeA.db.OrchestrateRebalance(newAll...); err != nil {
		t.Fatalf("OrchestrateRebalance err = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// Every freeze ACK must precede any pull.
	firstPull := -1
	lastFreeze := -1
	for i, ev := range log {
		switch ev {
		case "freeze":
			lastFreeze = i
		case "pull-snapshot", "pull-entries":
			if firstPull == -1 {
				firstPull = i
			}
		}
	}
	if firstPull == -1 {
		t.Fatalf("no pull recorded; log = %v", log)
	}
	if lastFreeze == -1 {
		t.Fatalf("no freeze recorded; log = %v", log)
	}
	if lastFreeze > firstPull {
		t.Fatalf("freeze (idx %d) ordered after first pull (idx %d); log = %v", lastFreeze, firstPull, log)
	}
}

// unitVec returns a dim-length vector with 1 at index hot.
func unitVec(dim, hot int) []float32 {
	v := make([]float32, dim)
	if hot < dim {
		v[hot] = 1
	}
	return v
}

// twoNode2PC stands up two served nodes A and B with an explicit L0 layout
// (A owns [0,oldCut], B owns the rest) and returns them plus the L1 layout that
// hands a band of keys from B to A, and a moving key already written on B.
func twoNode2PC(t *testing.T, optsExtra ...Option) (a, b *servedNode, l1 []ShardAssignment, movingID string) {
	t.Helper()
	// Reserve two ports first.
	la, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addrA := la.Addr().String()
	lb, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = la.Close()
		t.Fatal(err)
	}
	addrB := lb.Addr().String()

	maxKey := ^uint64(0)
	oldCut := maxKey / 2
	newCut := (maxKey / 4) * 3
	l0A := []ShardAssignment{
		{ID: "shard-a", Owner: LocalShardOwner, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: addrB, Start: oldCut + 1, End: maxKey},
	}
	l0B := []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: oldCut},
		{ID: "shard-b", Owner: LocalShardOwner, Start: oldCut + 1, End: maxKey},
	}
	l1 = []ShardAssignment{
		{ID: "shard-a", Owner: addrA, Start: 0, End: newCut},
		{ID: "shard-b", Owner: addrB, Start: newCut + 1, End: maxKey},
	}

	baseOpts := append([]Option{WithProvider(&mockProvider{dim: 16}), WithToken("s")}, optsExtra...)
	dbA, err := Open(filepath.Join(t.TempDir(), "2pc-a.csdb"), append(append([]Option{}, baseOpts...), WithPeers(addrB), WithShards(l0A...))...)
	if err != nil {
		t.Fatal(err)
	}
	dbB, err := Open(filepath.Join(t.TempDir(), "2pc-b.csdb"), append(append([]Option{}, baseOpts...), WithPeers(addrA), WithShards(l0B...))...)
	if err != nil {
		t.Fatal(err)
	}
	dbA.registerServeAddr(addrA)
	dbB.registerServeAddr(addrB)

	doneA := make(chan error, 1)
	go func() { doneA <- dbA.Serve(la) }()
	doneB := make(chan error, 1)
	go func() { doneB <- dbB.Serve(lb) }()
	t.Cleanup(func() {
		_ = la.Close()
		_ = lb.Close()
		_ = dbA.Close()
		_ = dbB.Close()
		select {
		case <-doneA:
		case <-time.After(2 * time.Second):
			t.Errorf("serve A did not exit")
		}
		select {
		case <-doneB:
		case <-time.After(2 * time.Second):
			t.Errorf("serve B did not exit")
		}
	})

	// A moving key: owned by B under L0 (key > oldCut), gained by A under L1
	// (key <= newCut).
	for i := 0; i < 200000; i++ {
		id := "doc-2pc-" + strconv.Itoa(i)
		key := shardKey("docs", id)
		if key > oldCut && key <= newCut {
			movingID = id
			break
		}
	}
	if movingID == "" {
		t.Fatal("no moving key found")
	}
	if err := dbB.Collection("docs", WithBitWidth(2)).PutVector(movingID, unitVec(16, 1)); err != nil {
		t.Fatal(err)
	}
	return &servedNode{db: dbA, addr: addrA}, &servedNode{db: dbB, addr: addrB}, l1, movingID
}

func TestOrchestrate2PCHappyPath(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	if err := a.db.OrchestrateRebalance(l1...); err != nil {
		t.Fatalf("OrchestrateRebalance err = %v", err)
	}

	// The moving key handed off: present on A (gainer), pruned on B (loser).
	gained, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(gained) != 1 {
		t.Fatalf("gainer history = %+v, want 1", gained)
	}
	lost, err := b.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 0 {
		t.Fatalf("loser retained handed-off key: %+v", lost)
	}

	// A post-cutover routed write to the moving key lands on the new owner (A).
	if err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 2)}); err != nil {
		t.Fatalf("post-cutover routed write err = %v", err)
	}
	final, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(final) != 2 {
		t.Fatalf("new owner history after routed write = %+v, want 2", final)
	}
}

func TestPrepareRaceNoLostWrite(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	// Fire a client write to the moving key during the gainer's Pull sub-phase
	// (the seam between the freeze barrier and the pull). The write goes through
	// B's federation, which now routes the moving key to its L0 owner (B) — but
	// B is frozen, so the write must be rejected with ErrRebalanceInProgress,
	// never silently dropped.
	a.db.rebalanceHooks = &rebalanceHooks{
		beforePull: func(target string) {
			err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 3)})
			if err == nil {
				t.Errorf("racing write during freeze was accepted; want ErrRebalanceInProgress")
			} else if !errors.Is(err, ErrRebalanceInProgress) {
				t.Errorf("racing write err = %v, want ErrRebalanceInProgress", err)
			}
		},
	}

	if err := a.db.OrchestrateRebalance(l1...); err != nil {
		t.Fatalf("OrchestrateRebalance err = %v", err)
	}
	a.db.rebalanceHooks = nil

	// After cutover the moving key is present on A with exactly the pre-handoff
	// version (the racing write was rejected, so no extra version, no loss).
	gained, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(gained) != 1 {
		t.Fatalf("gainer history after race = %+v, want 1 (racing write rejected, original present)", gained)
	}
}

// failingClient wraps a remoteClient and injects a failure on a named method.
// failOn is the method label; after failAfter successful passthroughs the next
// call returns injErr. A nil injErr means "succeed" (used to flip behavior).
type failingClient struct {
	remoteClient
	mu        sync.Mutex
	failOn    string
	failAfter int
	calls     map[string]int
	injErr    error
}

func newFailingClient(inner remoteClient, failOn string, failAfter int, injErr error) *failingClient {
	return &failingClient{remoteClient: inner, failOn: failOn, failAfter: failAfter, calls: map[string]int{}, injErr: injErr}
}

func (f *failingClient) shouldFail(method string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if method != f.failOn {
		return nil
	}
	n := f.calls[method]
	f.calls[method] = n + 1
	if n >= f.failAfter {
		return f.injErr
	}
	return nil
}

func (f *failingClient) FreezeRebalance(shards []ShardAssignment, epoch uint64, coordinator string) error {
	if err := f.shouldFail("freeze"); err != nil {
		return err
	}
	return f.remoteClient.FreezeRebalance(shards, epoch, coordinator)
}

func (f *failingClient) PrepareRebalance(shards []ShardAssignment, epoch uint64) error {
	if err := f.shouldFail("prepare"); err != nil {
		return err
	}
	return f.remoteClient.PrepareRebalance(shards, epoch)
}

func (f *failingClient) CommitRebalance(shards []ShardAssignment, epoch uint64) error {
	if err := f.shouldFail("commit"); err != nil {
		return err
	}
	return f.remoteClient.CommitRebalance(shards, epoch)
}

func injectFailingPeer(t *testing.T, db *DB, addr, failOn string, failAfter int, injErr error) *failingClient {
	t.Helper()
	inner, err := db.peerClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	fc := newFailingClient(inner, failOn, failAfter, injErr)
	db.mu.Lock()
	db.peerClients[addr] = fc
	db.mu.Unlock()
	return fc
}

func TestFreezeFailAborts(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	// B (the loser) fails its Freeze immediately.
	injectFailingPeer(t, a.db, b.addr, "freeze", 0, errors.New("injected freeze failure"))

	err := a.db.OrchestrateRebalance(l1...)
	if err == nil {
		t.Fatal("OrchestrateRebalance returned nil despite injected freeze failure")
	}

	// B never froze (its freeze failed), so the moving key is writable on B,
	// which still owns it under the unchanged L0.
	if err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 4)}); err != nil {
		t.Fatalf("moving-key write on old owner after abort err = %v, want nil", err)
	}

	// The coordinator A did not commit: it still owns its L0 range only and is IDLE.
	if a.db.rebalanceSnapshot().phase != phaseIdle {
		t.Fatalf("coordinator not IDLE after freeze-fail abort: %v", a.db.rebalanceSnapshot().phase)
	}
}

func TestPullFailAbortsReachesFrozenLoser(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	// B (loser) freezes fine, but its Prepare (pull request handling) ... no:
	// the GAINER is A (local). To make the gainer's pull fail, fail B's
	// PullSnapshot. A pulls from B; injecting a failure on B's PullSnapshot makes
	// A's local pullRebalanceShards fail after B has frozen.
	injectPullFailure(t, a.db, b.addr)

	err := a.db.OrchestrateRebalance(l1...)
	if err == nil {
		t.Fatal("OrchestrateRebalance returned nil despite injected pull failure")
	}

	// The frozen loser B must have received Abort and unfrozen: the moving key
	// is writable again on B (§5.4 row 2).
	if b.db.rebalanceSnapshot().phase != phaseIdle {
		t.Fatalf("frozen loser not unfrozen after pull-fail abort: %v", b.db.rebalanceSnapshot().phase)
	}
	if err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 5)}); err != nil {
		t.Fatalf("moving-key write on frozen loser after abort err = %v, want nil", err)
	}
}

// pullFailClient fails PullSnapshot to simulate a gainer pull failure after the
// loser has frozen.
type pullFailClient struct {
	remoteClient
}

func (p *pullFailClient) PullSnapshot(req RPCPullSnapshotRequest) (RPCPullSnapshotResponse, error) {
	return RPCPullSnapshotResponse{}, errors.New("injected pull-snapshot failure")
}

func injectPullFailure(t *testing.T, db *DB, addr string) {
	t.Helper()
	inner, err := db.peerClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	db.peerClients[addr] = &pullFailClient{remoteClient: inner}
	db.mu.Unlock()
}

func TestCommitPartialFailureNoSplitBrain(t *testing.T) {
	a, b, l1, movingID := twoNode2PC(t)

	// B's FIRST CommitRebalance fails; the second (retry) succeeds.
	fc := injectFailingPeer(t, a.db, b.addr, "commit", 0, errors.New("injected commit failure"))

	err := a.db.OrchestrateRebalance(l1...)
	if err == nil {
		t.Fatal("OrchestrateRebalance returned nil despite injected commit failure")
	}

	// The coordinator A already durably decided commit (it bumped its epoch and
	// applied L1 locally before broadcasting Commit). The laggard B is still on
	// L0 and its moving keys are fenced (frozen) — no conflicting write.
	if cerr := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 6)}); !errors.Is(cerr, ErrRebalanceInProgress) {
		t.Fatalf("laggard moving-key write err = %v, want ErrRebalanceInProgress (fenced)", cerr)
	}

	// Stop failing; retry the commit/prune to converge to L1.
	fc.mu.Lock()
	fc.failOn = ""
	fc.mu.Unlock()
	if err := a.db.OrchestrateRebalance(l1...); err != nil {
		// A is already committed (idempotent); B commits on retry.
		t.Fatalf("retry OrchestrateRebalance err = %v", err)
	}

	// Cluster converged: A owns the moving key, B pruned it.
	gained, err := a.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(gained) != 1 {
		t.Fatalf("gainer history after converge = %+v, want 1", gained)
	}
	lost, err := b.db.Collection("docs").historyFor(movingID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 0 {
		t.Fatalf("loser retained moving key after converge: %+v", lost)
	}
}

// driveToPrepared puts a participant DB into a persisted PREPARED state for
// epoch with the given coordinator address and pending layout, then closes it
// and returns the path so the caller can reopen and exercise recovery.
func driveToPrepared(t *testing.T, path, coordAddr string, epoch uint64, pending []ShardAssignment) {
	t.Helper()
	p, err := Open(path, WithProvider(nil), WithShards(twoNodeShardLayout("other", LocalShardOwner)...))
	if err != nil {
		t.Fatal(err)
	}
	p.installFreeze(epoch, coordAddr, pending)
	p.markPrepared()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedParticipantQueriesCoordinatorOnRestart(t *testing.T) {
	maxKey := ^uint64(0)
	pending := []ShardAssignment{{ID: "s", Owner: "other", Start: 0, End: maxKey}}

	t.Run("coordinator committed -> commit-forward", func(t *testing.T) {
		// Coordinator with a durable commit record for epoch 5.
		coord := startServedNode(t, "coord-commit", WithProvider(nil), WithToken("s"))
		coord.db.mu.Lock()
		coord.db.manifest.RebalanceEpoch = 5
		coord.db.mu.Unlock()
		if err := coord.db.saveManifest(); err != nil {
			t.Fatal(err)
		}

		path := filepath.Join(t.TempDir(), "p-commit.csdb")
		driveToPrepared(t, path, coord.addr, 5, pending)

		p, err := Open(path, WithProvider(nil), WithToken("s"))
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		// Recovery committed-forward: phase IDLE and the layout is L1 (pending).
		if p.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("participant not IDLE after commit-forward: %v", p.rebalanceSnapshot().phase)
		}
		if len(p.shardAssignments()) != 1 {
			t.Fatalf("participant did not apply L1: shards = %+v", p.shardAssignments())
		}
	})

	t.Run("coordinator aborted -> revert to L0", func(t *testing.T) {
		// Coordinator with NO commit record for epoch 5 (floor at 0 => abort).
		coord := startServedNode(t, "coord-abort", WithProvider(nil), WithToken("s"))

		path := filepath.Join(t.TempDir(), "p-abort.csdb")
		driveToPrepared(t, path, coord.addr, 5, pending)

		p, err := Open(path, WithProvider(nil), WithToken("s"))
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if p.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("participant not IDLE after abort: %v", p.rebalanceSnapshot().phase)
		}
		// Reverted to L0 (the original two-shard layout, not the single-shard L1).
		if len(p.shardAssignments()) != 2 {
			t.Fatalf("participant did not revert to L0: shards = %+v", p.shardAssignments())
		}
	})

	t.Run("coordinator gave away all shards but is still reachable", func(t *testing.T) {
		// The coordinator owns NO shards (it gave them all away), so it is in no
		// participant's shard-derived peer set. Recovery must STILL resolve by
		// dialing the persisted coordinator address directly.
		coord := startServedNode(t, "coord-empty", WithProvider(nil), WithToken("s"))
		coord.db.mu.Lock()
		coord.db.manifest.RebalanceEpoch = 9 // committed => commit
		// Coordinator owns nothing under the current layout.
		coord.db.mu.Unlock()
		if err := coord.db.saveManifest(); err != nil {
			t.Fatal(err)
		}

		path := filepath.Join(t.TempDir(), "p-empty.csdb")
		// The participant's shard layout does NOT mention the coordinator at all.
		p0, err := Open(path, WithProvider(nil), WithToken("s"))
		if err != nil {
			t.Fatal(err)
		}
		p0.installFreeze(9, coord.addr, pending)
		p0.markPrepared()
		if err := p0.Close(); err != nil {
			t.Fatal(err)
		}

		p, err := Open(path, WithProvider(nil), WithToken("s"))
		if err != nil {
			t.Fatal(err)
		}
		defer p.Close()
		if p.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("participant not IDLE after dialing coordinator-with-no-shards: %v", p.rebalanceSnapshot().phase)
		}
	})
}

func TestCoordinatorCrashRecovery(t *testing.T) {
	t.Run("after durable decide -> commit-forward", func(t *testing.T) {
		a, b, l1, movingID := twoNode2PC(t)
		// Stop right after the durable decision write (commit record present).
		a.db.rebalanceHooks = &rebalanceHooks{
			afterDurableDecide: func(epoch uint64) {
				panic("stop-after-durable-decide")
			},
		}
		func() {
			defer func() { _ = recover() }()
			_ = a.db.OrchestrateRebalance(l1...)
		}()
		a.db.rebalanceHooks = nil
		// The coordinator A holds a durable commit record (COMMITTING, floor>=epoch).
		st := a.db.rebalanceSnapshot()
		if st.phase != phaseCommitting {
			t.Fatalf("coordinator phase after durable decide = %v, want COMMITTING", st.phase)
		}
		// Recovery (re-running the recover step) commits-forward locally.
		if err := a.db.recoverPendingRebalance(); err != nil {
			t.Fatalf("recoverPendingRebalance err = %v", err)
		}
		if a.db.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("coordinator not IDLE after commit-forward recovery")
		}
		gained, err := a.db.Collection("docs").historyFor(movingID, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(gained) != 1 {
			t.Fatalf("gainer missing moving key after commit-forward: %+v", gained)
		}
		_ = b
	})

	t.Run("before durable decide -> abort", func(t *testing.T) {
		a, b, l1, movingID := twoNode2PC(t)
		// Stop right before the durable decision (no commit record): panic in the
		// freeze barrier seam.
		a.db.rebalanceHooks = &rebalanceHooks{
			afterFreezeBarrier: func(epoch uint64) {
				panic("stop-before-durable-decide")
			},
		}
		func() {
			defer func() { _ = recover() }()
			_ = a.db.OrchestrateRebalance(l1...)
		}()
		a.db.rebalanceHooks = nil
		// No commit record: floor still below the in-flight epoch.
		if a.db.committedEpoch() >= a.db.rebalanceSnapshot().epoch {
			t.Fatalf("unexpected commit record before durable decide")
		}
		// Recovery aborts: coordinator self-aborts and broadcasts Abort.
		if err := a.db.recoverPendingRebalance(); err != nil {
			t.Fatalf("recoverPendingRebalance err = %v", err)
		}
		if a.db.rebalanceSnapshot().phase != phaseIdle {
			t.Fatalf("coordinator not IDLE after abort recovery")
		}
		// The frozen loser B unfroze; the moving key is writable on B again.
		if err := b.db.Collection("docs").Put(movingID, Entry{Vector: unitVec(16, 7)}); err != nil {
			t.Fatalf("moving-key write on loser after coordinator-abort recovery err = %v", err)
		}
	})
}

func TestForceAbortHighestEpochAndStaleCommitAborts(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "forceabort.csdb"), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// ForceAbortRebalance bumps the committed floor strictly above the stuck epoch.
	before := db.committedEpoch()
	if err := db.ForceAbortRebalance(3); err != nil {
		t.Fatal(err)
	}
	after := db.committedEpoch()
	if after <= 3 || after <= before {
		t.Fatalf("force-abort floor = %d, want strictly > max(committed=%d, stuck=3)", after, before)
	}
}

func TestForceAbortPersistsAndStaleCommitRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stale.csdb")
	db, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}

	// Force-abort epoch 4 => floor becomes 5.
	if err := db.ForceAbortRebalance(4); err != nil {
		t.Fatal(err)
	}
	floor := db.committedEpoch()
	if floor <= 4 {
		t.Fatalf("force-abort floor = %d, want > 4", floor)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.committedEpoch() != floor {
		t.Fatalf("floor after reopen = %d, want %d (persisted)", reopened.committedEpoch(), floor)
	}

	// A stale-epoch commit (epoch <= floor) is rejected with ErrStaleEpoch.
	if err := reopened.applyShardLayout(twoNodeShardLayout("a", "b"), 4); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale commit err = %v, want ErrStaleEpoch", err)
	}
}

func TestStaleEpochFreezeRejected(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stalefreeze.csdb"), WithProvider(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.mu.Lock()
	db.manifest.RebalanceEpoch = 5
	db.mu.Unlock()
	if err := db.saveManifest(); err != nil {
		t.Fatal(err)
	}

	// A Freeze/Prepare with epoch <= committed floor is rejected.
	if err := db.freezeRebalanceShards(5, "coord", twoNodeShardLayout("a", "b")); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale freeze err = %v, want ErrStaleEpoch", err)
	}
	if err := db.pullRebalanceShards(3, "coord", twoNodeShardLayout("a", "b")); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale prepare err = %v, want ErrStaleEpoch", err)
	}
}

// TestCoordinatorStaleCommitTreatsAsAbort verifies that when a participant
// rejects the coordinator's CommitRebalance with ErrStaleEpoch (a force-abort
// bumped the floor past this epoch), the coordinator treats the whole rebalance
// as aborted: it does NOT proceed to Prune and reverts its own pending state.
func TestCoordinatorStaleCommitTreatsAsAbort(t *testing.T) {
	a, b, l1, _ := twoNode2PC(t)

	// Inject a peer that rejects Commit with ErrStaleEpoch (simulating a peer
	// whose floor was bumped past this epoch by a concurrent force-abort).
	injectFailingPeer(t, a.db, b.addr, "commit", 0, ErrStaleEpoch)

	err := a.db.OrchestrateRebalance(l1...)
	if !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("OrchestrateRebalance err = %v, want ErrStaleEpoch", err)
	}
	// The coordinator reverted its own pending state (it does not stay COMMITTING).
	if a.db.rebalanceSnapshot().phase != phaseIdle {
		t.Fatalf("coordinator phase after stale-commit = %v, want IDLE", a.db.rebalanceSnapshot().phase)
	}
}
