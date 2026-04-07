package replica

import (
	"sync"
	"testing"
	"time"

	walpkg "github.com/odvcencio/corkscrewdb/wal"
)

type mockApplier struct {
	mu      sync.Mutex
	entries []Entry
	snaps   []SnapshotData
}

func (m *mockApplier) ApplyReplicatedEntry(_ string, entry Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entry)
	return nil
}

func (m *mockApplier) ApplySnapshot(data SnapshotData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snaps = append(m.snaps, data)
	return nil
}

func (m *mockApplier) entryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

type mockPuller struct {
	streamer *Streamer
	snap     SnapshotData
}

func (p *mockPuller) PullEntries(req PullRequest) (PullResponse, error) {
	return p.streamer.Pull(req.Collection, req.SinceClock, req.MaxEntries), nil
}

func (p *mockPuller) PullSnapshot(req SnapshotRequest) (SnapshotResponse, error) {
	return SnapshotResponse{Data: p.snap}, nil
}

func TestStreamerRecordAndPull(t *testing.T) {
	s := NewStreamer()
	for i := uint64(1); i <= 5; i++ {
		s.Record("docs", walpkg.Entry{
			Kind:         walpkg.EntryPut,
			CollectionID: "docs",
			VectorID:     "v" + string(rune('0'+i)),
			LamportClock: i,
			ActorID:      "a",
			WallClock:    time.Now().UTC(),
		})
	}

	resp := s.Pull("docs", 0, 3)
	if len(resp.Entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(resp.Entries))
	}
	if !resp.HasMore {
		t.Fatal("expected HasMore=true")
	}
	if resp.LatestClock != 3 {
		t.Fatalf("LatestClock = %d, want 3", resp.LatestClock)
	}

	resp2 := s.Pull("docs", 3, 10)
	if len(resp2.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(resp2.Entries))
	}
	if resp2.HasMore {
		t.Fatal("expected HasMore=false")
	}
}

func TestStreamerPullEmpty(t *testing.T) {
	s := NewStreamer()
	resp := s.Pull("docs", 0, 10)
	if len(resp.Entries) != 0 {
		t.Fatalf("got %d entries from empty streamer", len(resp.Entries))
	}
}

func TestStreamerLatestClock(t *testing.T) {
	s := NewStreamer()
	if s.LatestClock("docs") != 0 {
		t.Fatal("expected 0 for empty collection")
	}
	s.Record("docs", walpkg.Entry{LamportClock: 42})
	if s.LatestClock("docs") != 42 {
		t.Fatalf("LatestClock = %d, want 42", s.LatestClock("docs"))
	}
}

func TestFollowerPullsAndApplies(t *testing.T) {
	s := NewStreamer()
	for i := uint64(1); i <= 3; i++ {
		s.Record("docs", walpkg.Entry{
			Kind:         walpkg.EntryPut,
			CollectionID: "docs",
			VectorID:     "v",
			LamportClock: i,
			ActorID:      "a",
			WallClock:    time.Now().UTC(),
		})
	}

	applier := &mockApplier{}
	follower, err := NewFollower(FollowerConfig{
		Collection: "docs",
		Applier:    applier,
		Puller:     &mockPuller{streamer: s},
		Interval:   50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	follower.Start()
	time.Sleep(200 * time.Millisecond)
	follower.Stop()

	if applier.entryCount() != 3 {
		t.Fatalf("applied %d entries, want 3", applier.entryCount())
	}
	if follower.LastClock() != 3 {
		t.Fatalf("LastClock = %d, want 3", follower.LastClock())
	}
}

func TestFollowerCatchUp(t *testing.T) {
	s := NewStreamer()
	s.Record("docs", walpkg.Entry{
		Kind:         walpkg.EntryPut,
		CollectionID: "docs",
		VectorID:     "tail-entry",
		LamportClock: 11,
		ActorID:      "a",
		WallClock:    time.Now().UTC(),
	})

	applier := &mockApplier{}
	snap := SnapshotData{
		Collection: "docs",
		BitWidth:   2,
		Seed:       42,
		Dim:        8,
		MaxLamport: 10,
		Entries: []VersionRecord{
			{ID: "snap-entry", Versions: []VersionEntry{{LamportClock: 10, ActorID: "a"}}},
		},
	}

	follower, err := NewFollower(FollowerConfig{
		Collection: "docs",
		Applier:    applier,
		Puller:     &mockPuller{streamer: s, snap: snap},
		Interval:   time.Hour, // won't tick
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := follower.CatchUp("token"); err != nil {
		t.Fatal(err)
	}

	if len(applier.snaps) != 1 {
		t.Fatalf("snapshots applied = %d, want 1", len(applier.snaps))
	}
	if applier.snaps[0].MaxLamport != 10 {
		t.Fatalf("snapshot MaxLamport = %d, want 10", applier.snaps[0].MaxLamport)
	}
	if applier.entryCount() != 1 {
		t.Fatalf("tail entries applied = %d, want 1", applier.entryCount())
	}
	if follower.LastClock() != 11 {
		t.Fatalf("LastClock = %d, want 11", follower.LastClock())
	}
}

func TestFollowerRequiresFields(t *testing.T) {
	if _, err := NewFollower(FollowerConfig{}); err == nil {
		t.Fatal("expected error for empty config")
	}
	if _, err := NewFollower(FollowerConfig{Collection: "docs"}); err == nil {
		t.Fatal("expected error for missing applier")
	}
	if _, err := NewFollower(FollowerConfig{Collection: "docs", Applier: &mockApplier{}}); err == nil {
		t.Fatal("expected error for missing puller")
	}
}
