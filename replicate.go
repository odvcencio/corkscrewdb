package corkscrewdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"lukechampine.com/blake3"
	"m31labs.dev/corkscrewdb/replica"
	walpkg "m31labs.dev/corkscrewdb/wal"
)

// RPCPuller adapts a remote DB connection (from Connect) to the
// replica.Puller interface for setting up replication followers.
type RPCPuller struct {
	remote remoteClient
}

// NewRPCPuller creates a Puller from a DB opened via Connect.
func NewRPCPuller(db *DB) (*RPCPuller, error) {
	if db == nil {
		return nil, errors.New("corkscrewdb: nil database")
	}
	if db.remote == nil {
		return nil, errors.New("corkscrewdb: RPCPuller requires a remote DB from Connect")
	}
	return &RPCPuller{remote: db.remote}, nil
}

// replicaEntryFromRPC maps a wire RPCReplicaEntry into a verbatim, deep-cloned
// replica.Entry (the WAL v5 payload, codes intact — never re-quantized).
func replicaEntryFromRPC(e RPCReplicaEntry) replica.Entry {
	return replica.Entry{Entry: walpkg.Entry{
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
	}}
}

func (p *RPCPuller) PullEntries(req replica.PullRequest) (replica.PullResponse, error) {
	resp, err := p.remote.PullEntries(RPCPullEntriesRequest{
		Token:      req.Token,
		Collection: req.Collection,
		SinceClock: req.SinceClock,
		MaxEntries: req.MaxEntries,
	})
	if err != nil {
		return replica.PullResponse{}, err
	}
	entries := make([]replica.Entry, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = replicaEntryFromRPC(e)
	}
	return replica.PullResponse{
		Entries:     entries,
		LatestClock: resp.LatestClock,
		HasMore:     resp.HasMore,
	}, nil
}

func (p *RPCPuller) PullSnapshot(req replica.SnapshotRequest) (replica.SnapshotResponse, error) {
	resp, err := p.remote.PullSnapshot(RPCPullSnapshotRequest{
		Token:      req.Token,
		Collection: req.Collection,
	})
	if err != nil {
		return replica.SnapshotResponse{}, err
	}
	records := make([]replica.VersionRecord, len(resp.Records))
	for i, r := range resp.Records {
		versions := make([]replica.VersionEntry, len(r.Versions))
		for j, v := range r.Versions {
			versions[j] = replica.VersionEntry{
				Quantized:    cloneWALQuantized(v.Quantized),
				Dim:          v.Dim,
				RawHash:      append([]byte(nil), v.RawHash...),
				Sparse:       cloneWALSparse(v.Sparse),
				Children:     cloneWALChildren(v.Children),
				Text:         v.Text,
				Metadata:     cloneMetadata(v.Metadata),
				LamportClock: v.LamportClock,
				ActorID:      v.ActorID,
				WallClock:    v.WallClock,
				Tombstone:    v.Tombstone,
			}
		}
		records[i] = replica.VersionRecord{ID: r.ID, Versions: versions}
	}
	return replica.SnapshotResponse{
		Data: replica.SnapshotData{
			Collection:    resp.Collection,
			BitWidth:      resp.BitWidth,
			Seed:          resp.Seed,
			Dim:           resp.Dim,
			MaxLamport:    resp.MaxLamport,
			RawStore:      resp.RawStore,
			SparseEnabled: resp.SparseEnabled,
			Entries:       records,
		},
	}, nil
}

func (p *RPCPuller) StreamEntries(ctx context.Context, req replica.PullRequest, handle func(replica.PullResponse) error) error {
	return p.remote.StreamEntries(ctx, RPCPullEntriesRequest{
		Token:      req.Token,
		Collection: req.Collection,
		SinceClock: req.SinceClock,
		MaxEntries: req.MaxEntries,
	}, func(resp RPCPullEntriesResponse) error {
		entries := make([]replica.Entry, len(resp.Entries))
		for i, e := range resp.Entries {
			entries[i] = replicaEntryFromRPC(e)
		}
		return handle(replica.PullResponse{
			Entries:     entries,
			LatestClock: resp.LatestClock,
			HasMore:     resp.HasMore,
		})
	})
}

// DBApplier adapts a local DB to the replica.Applier interface for receiving
// replicated entries.
type DBApplier struct {
	db *DB
}

// NewDBApplier creates an Applier that writes replicated data into a local DB.
func NewDBApplier(db *DB) (*DBApplier, error) {
	if db == nil {
		return nil, errors.New("corkscrewdb: nil database")
	}
	return &DBApplier{db: db}, nil
}

func (a *DBApplier) ApplyReplicatedEntry(collection string, entry replica.Entry) error {
	// Reconstruct a Version directly from the transmitted codes and apply it via
	// the same loadVersion -> applyVersionLocked path WAL replay uses. NO
	// re-quantization: the codes are inserted verbatim. Idempotent: applyVersionLocked
	// dedups on (LamportClock, ActorID).
	//
	// Entry-stream-only replication (no prior CatchUp/ApplySnapshot) creates the
	// follower collection lazily. When the very first entry carries a sparse
	// payload the collection must be created WithSparse(), or the follower's
	// SearchMulti later rejects sparse queries even though the sparse channel is
	// indexed. The preferred path is CatchUp first (which carries the flag
	// explicitly via ApplySnapshot); this handles the snapshot-less stream. We
	// only request WithSparse() when the collection does not yet exist as
	// non-sparse, so a mixed stream into an existing non-sparse collection does
	// not trip the WithSparse mismatch error.
	var opts []CollectionOption
	if fromWALSparse(entry.Sparse) != nil && a.db.collectionSparseCompatible(collection) {
		opts = append(opts, WithSparse())
	}
	coll := a.db.Collection(collection, opts...)
	if coll.err != nil {
		return coll.err
	}
	return coll.loadVersion(entry.VectorID, Version{
		Quantized:    fromWALQuantized(entry.Quantized),
		dim:          entry.Dim,
		RawHash:      append([]byte(nil), entry.RawHash...),
		Sparse:       fromWALSparse(entry.Sparse),
		Children:     fromWALChildren(entry.Children),
		Text:         entry.Text,
		Metadata:     cloneMetadata(entry.Metadata),
		LamportClock: entry.LamportClock,
		ActorID:      entry.ActorID,
		WallClock:    entry.WallClock,
		Tombstone:    entry.Kind == walpkg.EntryTombstone,
	})
}

// collectionSparseCompatible reports whether requesting WithSparse() for the
// named collection is safe: true when the collection does not exist yet (so the
// follower can create it sparse), or it already exists with the sparse channel
// enabled. It returns false for an existing non-sparse collection so the
// entry-stream applier does not trip the WithSparse mismatch error (Bug 5).
func (db *DB) collectionSparseCompatible(name string) bool {
	if db == nil {
		return false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	existing, ok := db.collections[name]
	if !ok {
		return true
	}
	return existing.sparseEnabled
}

func (a *DBApplier) ApplySnapshot(data replica.SnapshotData) error {
	// Pin bitWidth + seed so the codes are meaningful on the follower (§2.1).
	// The quantizer seed is set directly rather than via WithQuantizerSeed
	// because generateSeed() yields full-range int64 values (often negative),
	// which the WithQuantizerSeed option rejects; the snapshot-load path
	// (corkscrewdb.go) assigns coll.seed the same way.
	//
	// Mirror the primary's sparse/raw-store config so a hybrid (WithSparse)
	// collection replicates its sparse channel: without WithSparse here the
	// follower's SearchMulti rejects sparse queries (multi.go), and a raw-store
	// mismatch would diverge from the primary's storage form.
	opts := []CollectionOption{WithBitWidth(data.BitWidth)}
	if !data.RawStore {
		opts = append(opts, WithoutRawStore())
	}
	if data.SparseEnabled {
		opts = append(opts, WithSparse())
	}
	coll := a.db.Collection(data.Collection, opts...)
	if coll.err != nil {
		return coll.err
	}
	if data.Seed != 0 {
		coll.pinQuantizerSeed(data.Seed)
	}
	for _, record := range data.Entries {
		for _, v := range record.Versions {
			if err := coll.loadVersion(record.ID, Version{
				Quantized:    fromWALQuantized(v.Quantized),
				dim:          v.Dim,
				RawHash:      append([]byte(nil), v.RawHash...),
				Sparse:       fromWALSparse(v.Sparse),
				Children:     fromWALChildren(v.Children),
				Text:         v.Text,
				Metadata:     cloneMetadata(v.Metadata),
				LamportClock: v.LamportClock,
				ActorID:      v.ActorID,
				WallClock:    v.WallClock,
				Tombstone:    v.Tombstone,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// rawSources returns the remote clients fetchRawByHash will try, in order: the
// connected remote (if this DB is a Connect client), then each known peer.
func (db *DB) rawSources() []remoteClient {
	if db.remote != nil {
		return []remoteClient{db.remote}
	}
	var out []remoteClient
	for _, addr := range db.remoteShardTargets() {
		if client, err := db.peerClient(addr); err == nil && client != nil {
			out = append(out, client)
		}
	}
	if len(out) > 0 {
		return out
	}
	db.mu.RLock()
	peers := append([]string(nil), db.peers...)
	db.mu.RUnlock()
	for _, addr := range peers {
		if client, err := db.peerClient(addr); err == nil && client != nil {
			out = append(out, client)
		}
	}
	return out
}

// fetchRawByHash pulls a raw blob for hash from a remote source, verifies the
// returned bytes re-hash to the requested key (rejecting + not storing on
// mismatch), then writes it into the local raw store (idempotent dedup) and
// returns it. A terminal not-found from every source surfaces as
// ErrRawUnavailable; transient/integrity failures are returned as-is so the
// caller may retry.
func (db *DB) fetchRawByHash(collection string, hash []byte) ([]byte, error) {
	if len(hash) != 32 {
		return nil, fmt.Errorf("corkscrewdb: raw hash must be 32 bytes, got %d", len(hash))
	}
	coll := db.Collection(collection)
	if coll.err != nil {
		return nil, coll.err
	}
	// Already local? Serve from the local store.
	if local, err := coll.getRaw(hash); err == nil {
		return local, nil
	}

	sources := db.rawSources()
	if len(sources) == 0 {
		return nil, fmt.Errorf("%w: no remote source for collection %q", ErrRawUnavailable, collection)
	}

	var lastErr error
	terminalOnly := true
	for _, src := range sources {
		raw, err := src.GetRaw(collection, hash)
		if err != nil {
			lastErr = err
			if !errors.Is(err, ErrRawUnavailable) {
				// Retriable (integrity / unavailable): try the next source but
				// do not collapse to terminal.
				terminalOnly = false
			}
			continue
		}
		// Verify the returned bytes re-hash to the requested key; reject + do
		// not store on mismatch (treated as a transient/retriable error).
		sum := blake3.Sum256(raw)
		if !bytes.Equal(sum[:], hash) {
			lastErr = fmt.Errorf("corkscrewdb: fetched raw bytes for %x re-hash to %x (mismatch)", hash, sum[:])
			terminalOnly = false
			continue
		}
		if _, err := coll.putRaw(raw); err != nil {
			return nil, err
		}
		return raw, nil
	}

	if lastErr == nil {
		return nil, fmt.Errorf("%w: %x", ErrRawUnavailable, hash)
	}
	if terminalOnly && errors.Is(lastErr, ErrRawUnavailable) {
		return nil, lastErr
	}
	return nil, lastErr
}
