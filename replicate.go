package corkscrewdb

import (
	"context"
	"errors"

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
			Collection: resp.Collection,
			BitWidth:   resp.BitWidth,
			Seed:       resp.Seed,
			Dim:        resp.Dim,
			MaxLamport: resp.MaxLamport,
			Entries:    records,
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
	coll := a.db.Collection(collection)
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

func (a *DBApplier) ApplySnapshot(data replica.SnapshotData) error {
	// Pin bitWidth + seed so the codes are meaningful on the follower (§2.1).
	// The quantizer seed is set directly rather than via WithQuantizerSeed
	// because generateSeed() yields full-range int64 values (often negative),
	// which the WithQuantizerSeed option rejects; the snapshot-load path
	// (corkscrewdb.go) assigns coll.seed the same way.
	coll := a.db.Collection(data.Collection, WithBitWidth(data.BitWidth))
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
