package corkscrewdb

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"m31labs.dev/corkscrewdb/replica"
	walpkg "m31labs.dev/corkscrewdb/wal"
)

// replicaPullResponse aliases replica.PullResponse so the pull handlers do not
// have to qualify it everywhere.
type replicaPullResponse = replica.PullResponse

var ErrUnauthorized = errors.New("corkscrewdb: unauthorized")

type RPCInfoRequest struct {
	Token string
}

type RPCInfoResponse struct {
	PackageVersion string
	Embedding      embeddingConfig
	Peers          []string
	Collections    []RPCCollectionInfo
	Shards         []ShardAssignment
}

type RPCCollectionInfo struct {
	Name     string
	BitWidth int
}

type RPCEmpty struct{}

type RPCEnsureCollectionRequest struct {
	Token    string
	Name     string
	BitWidth int
	Seed     int64
}

type RPCPutRequest struct {
	Token      string
	Internal   bool
	Collection string
	ID         string
	Entry      Entry
}

type RPCPutVectorRequest struct {
	Token      string
	Internal   bool
	Collection string
	ID         string
	Vector     []float32
	Text       string
	Metadata   map[string]string
}

type RPCDeleteRequest struct {
	Token      string
	Internal   bool
	Collection string
	ID         string
}

type RPCFilter struct {
	Key   string
	Value string
}

type RPCSearchRequest struct {
	Token      string
	Internal   bool
	Collection string
	Query      string
	K          int
	Filters    []RPCFilter
	UseAt      bool
	AtLamport  uint64
}

type RPCSearchVectorRequest struct {
	Token      string
	Internal   bool
	Collection string
	Query      []float32
	K          int
	Filters    []RPCFilter
	UseAt      bool
	AtLamport  uint64
}

type RPCSearchResponse struct {
	Results []SearchResult
}

type RPCHistoryRequest struct {
	Token      string
	Internal   bool
	Collection string
	ID         string
	UseAt      bool
	AtLamport  uint64
}

type RPCHistoryResponse struct {
	Versions []Version
}

// Connect opens a remote CorkScrewDB client over gRPC.
func Connect(addr string, opts ...Option) (*DB, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, errors.New("corkscrewdb: address is required")
	}
	cfg := dbConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt.applyDB(&cfg)
		}
	}
	if cfg.providerSet {
		return nil, errors.New("corkscrewdb: Connect does not accept WithProvider")
	}

	client, err := newGRPCClient(addr, cfg.token)
	if err != nil {
		return nil, err
	}
	info, err := client.Info()
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	return &DB{
		path:        addr,
		remote:      client,
		token:       cfg.token,
		peers:       append([]string(nil), info.Peers...),
		manifest:    manifest{ModuleVersion: info.PackageVersion, Embedding: info.Embedding, Shards: cloneShardAssignments(info.Shards)},
		collections: make(map[string]*Collection),
	}, nil
}

// Serve exposes the DB over gRPC.
func (db *DB) Serve(listener net.Listener) error {
	if db == nil {
		return errors.New("corkscrewdb: nil database")
	}
	if db.remote != nil {
		return errors.New("corkscrewdb: remote clients cannot serve")
	}
	db.registerServeAddr(listener.Addr().String())
	return serveGRPC(db, listener)
}

// ListenAndServe binds a TCP listener and serves requests until the listener closes.
func (db *DB) ListenAndServe(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return db.Serve(listener)
}

func (db *DB) remoteCollection(name string, opts ...CollectionOption) *Collection {
	db.mu.Lock()
	defer db.mu.Unlock()

	if existing, ok := db.collections[name]; ok {
		var cfg collectionConfig
		for _, opt := range opts {
			if opt != nil {
				opt.applyCollection(&cfg)
			}
		}
		if cfg.bitWidth != 0 && existing.bitWidth != 0 && cfg.bitWidth != existing.bitWidth {
			return &Collection{db: db, name: name, remote: db.remote, err: fmt.Errorf("corkscrewdb: collection %q already exists with bit width %d", name, existing.bitWidth)}
		}
		if cfg.bitWidth != 0 && existing.bitWidth == 0 {
			if err := db.remote.EnsureCollection(name, cfg.bitWidth); err != nil {
				return &Collection{db: db, name: name, remote: db.remote, err: err}
			}
			existing.bitWidth = cfg.bitWidth
		}
		return existing
	}

	var cfg collectionConfig
	for _, opt := range opts {
		if opt != nil {
			opt.applyCollection(&cfg)
		}
	}
	if err := db.remote.EnsureCollection(name, cfg.bitWidth); err != nil {
		return &Collection{db: db, name: name, remote: db.remote, err: err}
	}
	coll := &Collection{
		db:       db,
		name:     name,
		bitWidth: cfg.bitWidth,
		remote:   db.remote,
	}
	db.collections[name] = coll
	return coll
}

type transportServer struct {
	db *DB
}

func (s *transportServer) Info(req RPCInfoRequest, resp *RPCInfoResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	*resp = RPCInfoResponse{
		PackageVersion: PackageVersion,
		Embedding:      s.db.manifest.Embedding,
		Peers:          append([]string(nil), s.db.peers...),
		Collections:    s.db.rpcCollectionInfo(),
		Shards:         cloneShardAssignments(s.db.manifest.Shards),
	}
	return nil
}

func (s *transportServer) DropCollection(req RPCEnsureCollectionRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	return s.db.DropCollection(req.Name)
}

func (s *transportServer) EnsureCollection(req RPCEnsureCollectionRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	opts := make([]CollectionOption, 0, 2)
	if req.BitWidth != 0 {
		opts = append(opts, WithBitWidth(req.BitWidth))
	}
	if req.Seed != 0 {
		// Pin the producer's quantizer seed so a replication-bootstrapped
		// collection agrees on codes (§2.3).
		opts = append(opts, WithQuantizerSeed(req.Seed))
	}
	coll := s.db.Collection(req.Name, opts...)
	return coll.err
}

func (s *transportServer) Put(req RPCPutRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	if req.Internal {
		// Trust the client's routing: apply locally with no owner re-check and no
		// re-fan-out (the federating client already resolved the owner). This
		// breaks the fan-out loop (§6.1, risk 6).
		return s.db.Collection(req.Collection).put(req.ID, req.Entry, false)
	}
	if err := s.requireLocalOwner(req.Collection, req.ID); err != nil {
		return err
	}
	return s.db.Collection(req.Collection).put(req.ID, req.Entry, false)
}

func (s *transportServer) PutVector(req RPCPutVectorRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	cfg := putVectorConfig{text: req.Text, metadata: req.Metadata}
	if req.Internal {
		return s.db.Collection(req.Collection).putVectorRequest(req.ID, req.Vector, cfg, false)
	}
	if err := s.requireLocalOwner(req.Collection, req.ID); err != nil {
		return err
	}
	return s.db.Collection(req.Collection).putVectorRequest(req.ID, req.Vector, cfg, false)
}

// requireLocalOwner rejects a non-internal direct-client write to a key the
// local node does not own (the server does NOT fan out; only a federating
// client fans out, §6.1). When the node is not federating (single node /
// no shards) every key is locally owned.
func (s *transportServer) requireLocalOwner(collection, id string) error {
	if !s.db.shouldFederate() {
		return nil
	}
	if s.db.isLocalOwner(collection, id) {
		return nil
	}
	return ErrWrongOwner
}

func (s *transportServer) Search(req RPCSearchRequest, resp *RPCSearchResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	coll := s.db.Collection(req.Collection)
	if req.UseAt {
		results, err := coll.At(req.AtLamport).Search(req.Query, req.K, fromRPCFilters(req.Filters)...)
		resp.Results = results
		return err
	}
	results, err := coll.search(req.Query, req.K, fromRPCFilters(req.Filters), !req.Internal)
	resp.Results = results
	return err
}

func (s *transportServer) SearchVector(req RPCSearchVectorRequest, resp *RPCSearchResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	coll := s.db.Collection(req.Collection)
	if req.UseAt {
		results, err := coll.At(req.AtLamport).SearchVector(req.Query, req.K, fromRPCFilters(req.Filters)...)
		resp.Results = results
		return err
	}
	results, err := coll.searchVector(req.Query, req.K, fromRPCFilters(req.Filters), !req.Internal)
	resp.Results = results
	return err
}

func (s *transportServer) History(req RPCHistoryRequest, resp *RPCHistoryResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	coll := s.db.Collection(req.Collection)
	if req.UseAt {
		versions, err := coll.At(req.AtLamport).History(req.ID)
		resp.Versions = versions
		return err
	}
	versions, err := coll.historyFor(req.ID, !req.Internal)
	resp.Versions = versions
	return err
}

func (s *transportServer) Delete(req RPCDeleteRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	if req.Internal {
		return s.db.Collection(req.Collection).delete(req.ID, false)
	}
	if err := s.requireLocalOwner(req.Collection, req.ID); err != nil {
		return err
	}
	return s.db.Collection(req.Collection).delete(req.ID, false)
}

func (s *transportServer) authorize(token string) error {
	if s.db.token == "" {
		return nil
	}
	if token != s.db.token {
		return ErrUnauthorized
	}
	return nil
}

func toRPCFilters(filters []FilterOption) []RPCFilter {
	if len(filters) == 0 {
		return nil
	}
	out := make([]RPCFilter, len(filters))
	for i, filter := range filters {
		out[i] = RPCFilter{Key: filter.key, Value: filter.value}
	}
	return out
}

func fromRPCFilters(filters []RPCFilter) []FilterOption {
	if len(filters) == 0 {
		return nil
	}
	out := make([]FilterOption, len(filters))
	for i, filter := range filters {
		out[i] = Filter(filter.Key, filter.Value)
	}
	return out
}

// RPC methods for replication pull.

type RPCPullEntriesRequest struct {
	Token      string
	Collection string
	SinceClock uint64
	MaxEntries int
}

type RPCPullEntriesResponse struct {
	Entries     []RPCReplicaEntry
	LatestClock uint64
	HasMore     bool
}

type RPCReplicaEntry struct {
	Kind         uint8
	CollectionID string
	VectorID     string
	Quantized    *walpkg.QuantizedVector
	Dim          int
	RawHash      []byte
	Sparse       *walpkg.SparseBlock
	Children     []walpkg.ChildVector
	Text         string
	Metadata     map[string]string
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
}

type RPCPullSnapshotRequest struct {
	Token      string
	Collection string
}

type RPCPullSnapshotResponse struct {
	Collection    string
	BitWidth      int
	Seed          int64
	Dim           int
	MaxLamport    uint64
	Records       []RPCSnapshotRecord
	RawStore      bool
	SparseEnabled bool
}

type RPCSnapshotRecord struct {
	ID       string
	Versions []RPCSnapshotVersion
}

type RPCSnapshotVersion struct {
	Quantized    *walpkg.QuantizedVector
	Dim          int
	RawHash      []byte
	Sparse       *walpkg.SparseBlock
	Children     []walpkg.ChildVector
	Text         string
	Metadata     map[string]string
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
	Tombstone    bool
}

type RPCRebalanceRequest struct {
	Token  string
	Shards []ShardAssignment
	Epoch  uint64
	// Coordinator is the coordinator's serve address. PrepareRebalance carries it
	// so a pure gainer (gains keys, loses none, so it never received Freeze)
	// persists the real coordinator on PREPARED and crash recovery can dial it
	// directly. Empty for Commit/Prune.
	Coordinator string
}

// rebalanceDecision is the durable outcome a coordinator reports for a
// rebalance epoch when a recovered participant queries it via ResolveRebalance.
type rebalanceDecision int

const (
	decisionUnknown rebalanceDecision = iota
	decisionCommit
	decisionAbort
)

type RPCGetRawRequest struct {
	Token      string
	Collection string
	Hash       []byte
}

type RPCGetRawResponse struct {
	Raw []byte
}

type RPCFreezeRebalanceRequest struct {
	Token       string
	Shards      []ShardAssignment
	Epoch       uint64
	Coordinator string
}

type RPCAbortRebalanceRequest struct {
	Token string
	Epoch uint64
}

type RPCResolveRebalanceRequest struct {
	Token string
	Epoch uint64
}

type RPCResolveRebalanceResponse struct {
	Decision       rebalanceDecision
	CommittedEpoch uint64
}

func (s *transportServer) PullEntries(req RPCPullEntriesRequest, resp *RPCPullEntriesResponse) error {
	pulled, err := s.pullEntries(req)
	if err != nil {
		return err
	}
	*resp = pulled
	return nil
}

func (s *transportServer) pullEntries(req RPCPullEntriesRequest) (RPCPullEntriesResponse, error) {
	if err := s.authorize(req.Token); err != nil {
		return RPCPullEntriesResponse{}, err
	}
	if s.db.streamer == nil {
		return RPCPullEntriesResponse{}, errStreamerUnavailable
	}
	pr := s.db.streamer.Pull(req.Collection, req.SinceClock, req.MaxEntries)
	return toRPCPullEntriesResponse(pr), nil
}

func (s *transportServer) pullEntriesBlocking(req RPCPullEntriesRequest, wait time.Duration) (RPCPullEntriesResponse, error) {
	if err := s.authorize(req.Token); err != nil {
		return RPCPullEntriesResponse{}, err
	}
	if s.db.streamer == nil {
		return RPCPullEntriesResponse{}, errStreamerUnavailable
	}
	pr := s.db.streamer.PullBlocking(req.Collection, req.SinceClock, req.MaxEntries, wait)
	return toRPCPullEntriesResponse(pr), nil
}

func (s *transportServer) PullSnapshot(req RPCPullSnapshotRequest, resp *RPCPullSnapshotResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	coll := s.db.Collection(req.Collection)
	if coll.err != nil {
		return coll.err
	}
	*resp = coll.snapshotForPull()
	return nil
}

// errStreamerUnavailable is returned when a pull is requested but the server has
// no replication streamer (e.g. a Connect client acting as server).
var errStreamerUnavailable = errors.New("corkscrewdb: replication streamer unavailable")

// toRPCPullEntriesResponse maps a streamer PullResponse into the wire response,
// carrying each entry's WAL v5 codes verbatim (no re-quantization).
func toRPCPullEntriesResponse(pr replicaPullResponse) RPCPullEntriesResponse {
	entries := make([]RPCReplicaEntry, len(pr.Entries))
	for i, e := range pr.Entries {
		entries[i] = RPCReplicaEntry{
			Kind:         e.Kind,
			CollectionID: e.CollectionID,
			VectorID:     e.VectorID,
			Quantized:    cloneWALQuantized(e.Quantized),
			Dim:          e.Dim,
			RawHash:      append([]byte(nil), e.RawHash...),
			Sparse:       cloneWALSparse(e.Sparse),
			Children:     cloneWALChildren(e.Children),
			Text:         e.Text,
			Metadata:     cloneMetadata(e.Metadata),
			LamportClock: e.LamportClock,
			ActorID:      e.ActorID,
			WallClock:    e.WallClock,
		}
	}
	return RPCPullEntriesResponse{
		Entries:     entries,
		LatestClock: pr.LatestClock,
		HasMore:     pr.HasMore,
	}
}

// PrepareRebalance is the gainer's PULL sub-phase: the participant pulls every
// moving key it gains under L1 from the old owners. The epoch (when set) fences
// out a stale coordinator. The coordinator address recorded at Freeze is kept.
func (s *transportServer) PrepareRebalance(req RPCRebalanceRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	if req.Epoch == 0 {
		// Legacy unfenced prepare (single-node RebalanceShards over the wire).
		return s.db.prepareRebalanceShards(req.Shards)
	}
	// Prefer the coordinator address carried in the request: a pure gainer that
	// never froze has no recorded coordinator, so the request is the only source
	// of the address it must persist on PREPARED. Fall back to any already-
	// recorded coordinator (a freeze+pull participant) for backward compatibility.
	coord := strings.TrimSpace(req.Coordinator)
	if coord == "" {
		coord = s.db.rebalanceSnapshot().coordinator
	}
	return s.db.pullRebalanceShards(req.Epoch, coord, req.Shards)
}

// CommitRebalance applies L1 locally, bumping the durable RebalanceEpoch floor
// and unfreezing. Epoch-fenced: a stale epoch is rejected with ErrStaleEpoch.
func (s *transportServer) CommitRebalance(req RPCRebalanceRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	normalized, err := normalizeShardAssignments(req.Shards)
	if err != nil {
		return err
	}
	return s.db.applyShardLayout(normalized, req.Epoch)
}

func (s *transportServer) PruneRebalance(req RPCRebalanceRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	normalized, err := normalizeShardAssignments(req.Shards)
	if err != nil {
		return err
	}
	return s.db.pruneUnownedData(normalized)
}

// GetRaw serves a raw blob by hash from the collection's raw store. The two
// distinct failure modes (not-found, integrity) are surfaced via the rawstore
// sentinels so grpcStatusError maps them to codes.NotFound / codes.DataLoss and
// the follower can apply the right retry policy.
func (s *transportServer) GetRaw(req RPCGetRawRequest, resp *RPCGetRawResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	raw, err := s.db.Collection(req.Collection).getRaw(req.Hash)
	if err != nil {
		return err
	}
	resp.Raw = raw
	return nil
}

// FreezeRebalance is the 2PC freeze sub-phase: the loser installs a range
// freeze for epoch over L1, persisting the coordinator address durably so a
// recovering participant can reach it, and ACKs. Epoch-fenced.
func (s *transportServer) FreezeRebalance(req RPCFreezeRebalanceRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	return s.db.freezeRebalanceShards(req.Epoch, req.Coordinator, req.Shards)
}

// AbortRebalance reverts an in-flight (FROZEN/PREPARED) rebalance to L0,
// unfreezing the moving keys. A committed participant is never rolled back.
func (s *transportServer) AbortRebalance(req RPCAbortRebalanceRequest, _ *RPCEmpty) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	return s.db.abortRebalanceLocal(req.Epoch)
}

// ResolveRebalance returns the coordinator's durable decision for an epoch so a
// recovered PREPARED participant can finish (commit-forward or abort). Wired in
// Task 15.
func (s *transportServer) ResolveRebalance(req RPCResolveRebalanceRequest, resp *RPCResolveRebalanceResponse) error {
	if err := s.authorize(req.Token); err != nil {
		return err
	}
	decision, committed := s.db.resolveRebalanceDecision(req.Epoch)
	*resp = RPCResolveRebalanceResponse{Decision: decision, CommittedEpoch: committed}
	return nil
}
