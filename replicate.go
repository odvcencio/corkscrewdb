package corkscrewdb

import (
	"context"
	"errors"

	"github.com/odvcencio/corkscrewdb/replica"
	walpkg "github.com/odvcencio/corkscrewdb/wal"
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

func (p *RPCPuller) PullEntries(req replica.PullRequest) (replica.PullResponse, error) {
	resp, err := p.remote.PullEntries(RPCPullEntriesRequest{
		Collection: req.Collection,
		SinceClock: req.SinceClock,
		MaxEntries: req.MaxEntries,
	})
	if err != nil {
		return replica.PullResponse{}, err
	}
	entries := make([]replica.Entry, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = replica.Entry{Entry: walpkg.Entry{
			Kind:         e.Kind,
			CollectionID: e.CollectionID,
			VectorID:     e.VectorID,
			Embedding:    cloneVector(e.Embedding),
			Text:         e.Text,
			Metadata:     cloneMetadata(e.Metadata),
			LamportClock: e.LamportClock,
			ActorID:      e.ActorID,
			WallClock:    e.WallClock,
		}}
	}
	return replica.PullResponse{
		Entries:     entries,
		LatestClock: resp.LatestClock,
		HasMore:     resp.HasMore,
	}, nil
}

func (p *RPCPuller) PullSnapshot(req replica.SnapshotRequest) (replica.SnapshotResponse, error) {
	resp, err := p.remote.PullSnapshot(RPCPullSnapshotRequest{
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
				Embedding:    cloneVector(v.Embedding),
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
		Collection: req.Collection,
		SinceClock: req.SinceClock,
		MaxEntries: req.MaxEntries,
	}, func(resp RPCPullEntriesResponse) error {
		entries := make([]replica.Entry, len(resp.Entries))
		for i, e := range resp.Entries {
			entries[i] = replica.Entry{Entry: walpkg.Entry{
				Kind:         e.Kind,
				CollectionID: e.CollectionID,
				VectorID:     e.VectorID,
				Embedding:    cloneVector(e.Embedding),
				Text:         e.Text,
				Metadata:     cloneMetadata(e.Metadata),
				LamportClock: e.LamportClock,
				ActorID:      e.ActorID,
				WallClock:    e.WallClock,
			}}
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
	coll := a.db.Collection(collection)
	return coll.loadVersion(entry.VectorID, Version{
		Embedding:    cloneVector(entry.Embedding),
		Text:         entry.Text,
		Metadata:     cloneMetadata(entry.Metadata),
		LamportClock: entry.LamportClock,
		ActorID:      entry.ActorID,
		WallClock:    entry.WallClock,
		Tombstone:    entry.Kind == walpkg.EntryTombstone,
	})
}

func (a *DBApplier) ApplySnapshot(data replica.SnapshotData) error {
	coll := a.db.Collection(data.Collection, WithBitWidth(data.BitWidth))
	for _, record := range data.Entries {
		for _, v := range record.Versions {
			if err := coll.loadVersion(record.ID, Version{
				Embedding:    cloneVector(v.Embedding),
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
