package corkscrewdb

// remoteClient abstracts the transport layer for remote DB operations.
// Implemented by *grpcClient over gRPC.
type remoteClient interface {
	Info() (RPCInfoResponse, error)
	EnsureCollection(name string, bitWidth int) error
	DropCollection(name string) error
	Put(collection, id string, entry Entry, internal bool) error
	PutVector(collection, id string, vector []float32, text string, metadata map[string]string, internal bool) error
	Delete(collection, id string, internal bool) error
	Search(collection, query string, k int, filters []FilterOption, useAt bool, atLamport uint64, internal bool) ([]SearchResult, error)
	SearchVector(collection string, query []float32, k int, filters []FilterOption, useAt bool, atLamport uint64, internal bool) ([]SearchResult, error)
	History(collection, id string, useAt bool, atLamport uint64, internal bool) ([]Version, error)
	PullEntries(req RPCPullEntriesRequest) (RPCPullEntriesResponse, error)
	PullSnapshot(req RPCPullSnapshotRequest) (RPCPullSnapshotResponse, error)
	PrepareRebalance(shards []ShardAssignment) error
	CommitRebalance(shards []ShardAssignment) error
	PruneRebalance(shards []ShardAssignment) error
	Close() error
}
