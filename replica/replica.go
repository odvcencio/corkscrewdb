package replica

import (
	"errors"
	"sync"
	"time"

	walpkg "github.com/odvcencio/corkscrewdb/wal"
)

// Entry is a WAL entry enriched with replication metadata.
type Entry struct {
	walpkg.Entry
}

// SnapshotData is the full state of one collection for catch-up replication.
type SnapshotData struct {
	Collection string
	BitWidth   int
	Seed       int64
	Dim        int
	MaxLamport uint64
	Entries    []VersionRecord
}

// VersionRecord is one vector ID with its full version history.
type VersionRecord struct {
	ID       string
	Versions []VersionEntry
}

// VersionEntry is a single version in a record's history.
type VersionEntry struct {
	Embedding    []float32
	Text         string
	Metadata     map[string]string
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
	Tombstone    bool
}

// PullRequest asks for WAL entries after a given Lamport clock.
type PullRequest struct {
	Token      string
	Collection string
	SinceClock uint64
	MaxEntries int
}

// PullResponse carries WAL entries from a primary to a follower.
type PullResponse struct {
	Entries     []Entry
	LatestClock uint64
	HasMore     bool
}

// SnapshotRequest asks for a full collection snapshot.
type SnapshotRequest struct {
	Token      string
	Collection string
}

// SnapshotResponse carries a full collection state for catch-up.
type SnapshotResponse struct {
	Data SnapshotData
}

// Applier is the interface a DB/Collection must implement to receive replicated entries.
type Applier interface {
	ApplyReplicatedEntry(collection string, entry Entry) error
	ApplySnapshot(data SnapshotData) error
}

// Streamer reads WAL entries from a collection's history for replication.
type Streamer struct {
	mu      sync.RWMutex
	entries map[string][]Entry // collection -> sorted entries
}

// NewStreamer creates a new replication streamer.
func NewStreamer() *Streamer {
	return &Streamer{
		entries: make(map[string][]Entry),
	}
}

// Record adds a WAL entry to the streamer's buffer for a collection.
func (s *Streamer) Record(collection string, walEntry walpkg.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[collection] = append(s.entries[collection], Entry{Entry: walEntry})
}

// Pull returns entries for a collection after sinceClock, up to maxEntries.
func (s *Streamer) Pull(collection string, sinceClock uint64, maxEntries int) PullResponse {
	if maxEntries <= 0 {
		maxEntries = 1000
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	all := s.entries[collection]
	if len(all) == 0 {
		return PullResponse{}
	}

	// Binary search for the first entry after sinceClock.
	start := 0
	for start < len(all) && all[start].LamportClock <= sinceClock {
		start++
	}
	if start >= len(all) {
		return PullResponse{LatestClock: all[len(all)-1].LamportClock}
	}

	end := start + maxEntries
	hasMore := false
	if end > len(all) {
		end = len(all)
	} else if end < len(all) {
		hasMore = true
	}

	result := make([]Entry, end-start)
	copy(result, all[start:end])
	return PullResponse{
		Entries:     result,
		LatestClock: result[len(result)-1].LamportClock,
		HasMore:     hasMore,
	}
}

// LatestClock returns the highest Lamport clock seen for a collection.
func (s *Streamer) LatestClock(collection string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := s.entries[collection]
	if len(all) == 0 {
		return 0
	}
	return all[len(all)-1].LamportClock
}

// Follower polls a remote primary for new WAL entries and applies them locally.
type Follower struct {
	mu         sync.Mutex
	collection string
	lastClock  uint64
	applier    Applier
	puller     Puller
	interval   time.Duration
	stopCh     chan struct{}
	doneCh     chan struct{}
	running    bool
}

// Puller is the interface for pulling entries from a remote primary.
type Puller interface {
	PullEntries(req PullRequest) (PullResponse, error)
	PullSnapshot(req SnapshotRequest) (SnapshotResponse, error)
}

// FollowerConfig configures a replication follower.
type FollowerConfig struct {
	Collection string
	LastClock  uint64
	Applier    Applier
	Puller     Puller
	Interval   time.Duration
}

// NewFollower creates a replication follower that polls for new entries.
func NewFollower(cfg FollowerConfig) (*Follower, error) {
	if cfg.Collection == "" {
		return nil, errors.New("replica: collection is required")
	}
	if cfg.Applier == nil {
		return nil, errors.New("replica: applier is required")
	}
	if cfg.Puller == nil {
		return nil, errors.New("replica: puller is required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	return &Follower{
		collection: cfg.Collection,
		lastClock:  cfg.LastClock,
		applier:    cfg.Applier,
		puller:     cfg.Puller,
		interval:   cfg.Interval,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
	}, nil
}

// Start begins the polling loop in a goroutine.
func (f *Follower) Start() {
	f.mu.Lock()
	if f.running {
		f.mu.Unlock()
		return
	}
	f.running = true
	f.mu.Unlock()
	go f.loop()
}

// Stop halts the polling loop and waits for it to finish.
func (f *Follower) Stop() {
	f.mu.Lock()
	if !f.running {
		f.mu.Unlock()
		return
	}
	f.mu.Unlock()
	close(f.stopCh)
	<-f.doneCh
}

// CatchUp performs a one-time snapshot + WAL tail catch-up.
func (f *Follower) CatchUp(token string) error {
	resp, err := f.puller.PullSnapshot(SnapshotRequest{
		Token:      token,
		Collection: f.collection,
	})
	if err != nil {
		return err
	}
	if err := f.applier.ApplySnapshot(resp.Data); err != nil {
		return err
	}
	f.mu.Lock()
	f.lastClock = resp.Data.MaxLamport
	f.mu.Unlock()
	return f.pullOnce(token)
}

// LastClock returns the follower's last applied Lamport clock.
func (f *Follower) LastClock() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastClock
}

func (f *Follower) loop() {
	defer close(f.doneCh)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		select {
		case <-f.stopCh:
			return
		case <-ticker.C:
			_ = f.pullOnce("")
		}
	}
}

func (f *Follower) pullOnce(token string) error {
	f.mu.Lock()
	clock := f.lastClock
	f.mu.Unlock()

	for {
		resp, err := f.puller.PullEntries(PullRequest{
			Token:      token,
			Collection: f.collection,
			SinceClock: clock,
			MaxEntries: 1000,
		})
		if err != nil {
			return err
		}
		for _, entry := range resp.Entries {
			if err := f.applier.ApplyReplicatedEntry(f.collection, entry); err != nil {
				return err
			}
		}
		if len(resp.Entries) > 0 {
			clock = resp.LatestClock
			f.mu.Lock()
			f.lastClock = clock
			f.mu.Unlock()
		}
		if !resp.HasMore {
			break
		}
	}
	return nil
}
