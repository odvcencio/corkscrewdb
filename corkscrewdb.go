package corkscrewdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/odvcencio/corkscrewdb/replica"
	snap "github.com/odvcencio/corkscrewdb/snapshot"
	walpkg "github.com/odvcencio/corkscrewdb/wal"
)

const (
	defaultBitWidth   = 2
	defaultWALMaxSize = 8 << 20
)

type dbConfig struct {
	provider       EmbeddingProvider
	providerSet    bool
	defaultBits    int
	walSegmentSize int64
	peers          []string
	token          string
}

// Option configures Open.
type Option interface {
	applyDB(*dbConfig)
}

type dbOptionFunc func(*dbConfig)

func (f dbOptionFunc) applyDB(cfg *dbConfig) {
	f(cfg)
}

// WithProvider overrides the default built-in text embedding provider.
func WithProvider(provider EmbeddingProvider) Option {
	return dbOptionFunc(func(cfg *dbConfig) {
		cfg.provider = provider
		cfg.providerSet = true
	})
}

// WithWALSegmentSize configures WAL segment rotation.
func WithWALSegmentSize(size int64) Option {
	return dbOptionFunc(func(cfg *dbConfig) {
		cfg.walSegmentSize = size
	})
}

// WithPeers records remote peer seed addresses for future federation/cluster use.
func WithPeers(peers ...string) Option {
	return dbOptionFunc(func(cfg *dbConfig) {
		cfg.peers = sanitizePeers(peers)
	})
}

// WithToken configures the in-memory auth token used for future remote access.
func WithToken(token string) Option {
	return dbOptionFunc(func(cfg *dbConfig) {
		cfg.token = token
	})
}

// DB is an embedded CorkScrewDB instance.
type DB struct {
	path           string
	provider       EmbeddingProvider
	encoder        *encoder
	walSegmentSize int64
	peers          []string
	token          string
	remote         *rpcClient
	serveAddr      string

	mu          sync.RWMutex
	manifest    manifest
	collections map[string]*Collection
	peerClients map[string]*rpcClient
	streamer    *replica.Streamer
	closed      bool
}

type manifest struct {
	FormatVersion   int                       `json:"format_version"`
	ModuleVersion   string                    `json:"module_version"`
	ActorID         string                    `json:"actor_id"`
	DefaultBitWidth int                       `json:"default_bit_width"`
	Peers           []string                  `json:"peers,omitempty"`
	Embedding       embeddingConfig           `json:"embedding"`
	Collections     map[string]collectionMeta `json:"collections"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

type embeddingConfig struct {
	ID  string `json:"id"`
	Dim int    `json:"dim"`
}

type collectionMeta struct {
	BitWidth int   `json:"bit_width"`
	Seed     int64 `json:"seed"`
	Dim      int   `json:"dim"`
}

type providerIdentifier interface {
	ProviderID() string
}

// Open creates or opens an embedded CorkScrewDB at path.
func Open(path string, opts ...Option) (*DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("corkscrewdb: path is required")
	}
	cfg := dbConfig{
		defaultBits:    defaultBitWidth,
		walSegmentSize: defaultWALMaxSize,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyDB(&cfg)
		}
	}
	if !cfg.providerSet {
		cfg.provider = newBuiltinProvider()
	}
	cleanPath := filepath.Clean(path)
	if err := ensureDBDir(cleanPath); err != nil {
		return nil, err
	}
	manifestData, err := loadOrCreateManifest(cleanPath, cfg.defaultBits)
	if err != nil {
		return nil, err
	}
	if err := applyRuntimeConfig(&manifestData, cfg.provider, cfg.peers); err != nil {
		return nil, err
	}

	db := &DB{
		path:           cleanPath,
		provider:       cfg.provider,
		encoder:        newEncoder(cfg.provider),
		walSegmentSize: cfg.walSegmentSize,
		peers:          append([]string(nil), manifestData.Peers...),
		token:          cfg.token,
		manifest:       manifestData,
		collections:    make(map[string]*Collection),
		peerClients:    make(map[string]*rpcClient),
		streamer:       replica.NewStreamer(),
	}
	if err := db.saveManifest(); err != nil {
		return nil, err
	}

	for name, meta := range manifestData.Collections {
		coll, err := db.loadCollection(name, meta)
		if err != nil {
			return nil, err
		}
		db.collections[name] = coll
	}
	return db, nil
}

// Collection opens or creates a named collection.
func (db *DB) Collection(name string, opts ...CollectionOption) *Collection {
	if db == nil {
		return &Collection{err: errors.New("corkscrewdb: nil database")}
	}
	if db.isClosed() {
		return &Collection{err: errors.New("corkscrewdb: database is closed"), db: db, encoder: db.encoder}
	}
	if !validCollectionName(name) {
		return &Collection{err: fmt.Errorf("corkscrewdb: invalid collection name %q", name), db: db, encoder: db.encoder}
	}
	if db.remote != nil {
		return db.remoteCollection(name, opts...)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if existing, ok := db.collections[name]; ok {
		var cfg collectionConfig
		for _, opt := range opts {
			if opt != nil {
				opt.applyCollection(&cfg)
			}
		}
		if cfg.bitWidth != 0 && cfg.bitWidth != existing.bitWidth {
			return &Collection{
				db:       db,
				name:     name,
				bitWidth: existing.bitWidth,
				seed:     existing.seed,
				encoder:  db.encoder,
				err:      fmt.Errorf("corkscrewdb: collection %q already exists with bit width %d", name, existing.bitWidth),
			}
		}
		return existing
	}

	cfg := collectionConfig{bitWidth: db.manifest.DefaultBitWidth}
	for _, opt := range opts {
		if opt != nil {
			opt.applyCollection(&cfg)
		}
	}
	if cfg.bitWidth < 2 {
		return &Collection{db: db, name: name, encoder: db.encoder, err: errors.New("corkscrewdb: bit width must be >= 2")}
	}

	meta := collectionMeta{
		BitWidth: cfg.bitWidth,
		Seed:     generateSeed(),
	}
	coll, err := db.newCollection(name, meta)
	if err != nil {
		return &Collection{db: db, name: name, encoder: db.encoder, err: err}
	}
	db.collections[name] = coll
	db.manifest.Collections[name] = meta
	if err := db.saveManifestLocked(); err != nil {
		coll.err = err
	}
	return coll
}

// Close flushes snapshots and closes all active WAL writers.
func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil
	}
	collections := make([]*Collection, 0, len(db.collections))
	for _, coll := range db.collections {
		collections = append(collections, coll)
	}
	remote := db.remote
	db.mu.RUnlock()

	var errs []error
	if remote != nil {
		if err := remote.Close(); err != nil {
			errs = append(errs, err)
		}
		db.mu.Lock()
		db.closed = true
		db.mu.Unlock()
		return errors.Join(errs...)
	}
	for _, coll := range collections {
		if err := coll.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := db.closePeerClients(); err != nil {
		errs = append(errs, err)
	}
	if err := db.encoder.Close(); err != nil {
		errs = append(errs, err)
	}
	db.mu.Lock()
	db.closed = true
	db.mu.Unlock()
	return errors.Join(errs...)
}

func (db *DB) saveManifest() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.saveManifestLocked()
}

func (db *DB) isClosed() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.closed
}

func ensureDBDir(path string) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("corkscrewdb: path %q exists and is not a directory", path)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Join(path, "collections"), 0o755); err != nil {
		return err
	}
	return nil
}

func loadOrCreateManifest(path string, defaultBits int) (manifest, error) {
	manifestPath := filepath.Join(path, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return manifest{}, err
		}
		now := time.Now().UTC()
		m := manifest{
			FormatVersion:   1,
			ModuleVersion:   PackageVersion,
			ActorID:         generateActorID(),
			DefaultBitWidth: defaultBits,
			Collections:     make(map[string]collectionMeta),
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		if err := writeManifest(manifestPath, m); err != nil {
			return manifest{}, err
		}
		return m, nil
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest{}, err
	}
	if m.Collections == nil {
		m.Collections = make(map[string]collectionMeta)
	}
	if m.DefaultBitWidth == 0 {
		m.DefaultBitWidth = defaultBits
	}
	return m, nil
}

func writeManifest(path string, data manifest) error {
	data.ModuleVersion = PackageVersion
	data.UpdatedAt = time.Now().UTC()
	buf, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(buf, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (db *DB) saveManifestLocked() error {
	return writeManifest(filepath.Join(db.path, "manifest.json"), db.manifest)
}

func (db *DB) persistCollectionMeta(name string, bitWidth, dim int, seed int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	meta := db.manifest.Collections[name]
	meta.BitWidth = bitWidth
	meta.Dim = dim
	meta.Seed = seed
	db.manifest.Collections[name] = meta
	return db.saveManifestLocked()
}

func (db *DB) loadCollection(name string, meta collectionMeta) (*Collection, error) {
	coll, err := db.newCollection(name, meta)
	if err != nil {
		return nil, err
	}

	snapshotPath, err := snap.FindLatestFile(db.collectionDir(name))
	if err != nil {
		return nil, err
	}
	var snapshotMax uint64
	if snapshotPath != "" {
		data, err := snap.LoadFile(snapshotPath)
		if err != nil {
			return nil, err
		}
		if data.BitWidth != 0 {
			coll.bitWidth = data.BitWidth
		}
		if data.Seed != 0 {
			coll.seed = data.Seed
		}
		for _, record := range data.Records {
			for _, version := range record.Versions {
				if err := coll.loadVersion(record.ID, Version{
					Embedding:    cloneVector(version.Embedding),
					Text:         version.Text,
					Metadata:     cloneMetadata(version.Metadata),
					LamportClock: version.LamportClock,
					ActorID:      version.ActorID,
					WallClock:    version.WallClock,
					Tombstone:    version.Tombstone,
				}); err != nil {
					return nil, err
				}
			}
		}
		snapshotMax = data.MaxLamport
		if restored, restoredLamport, err := db.tryLoadCollectionIndex(name); err == nil && restored != nil && restoredLamport == snapshotMax {
			coll.mu.Lock()
			coll.index = restored
			if coll.dim == 0 {
				coll.dim = restored.Dim()
			}
			coll.mu.Unlock()
		}
	}

	segments, err := walpkg.ListSegments(db.collectionWALDir(name))
	if err != nil {
		return nil, err
	}
	for _, segment := range segments {
		reader, err := walpkg.NewReader(segment)
		if err != nil {
			return nil, err
		}
		for reader.Next() {
			entry := reader.Entry()
			if entry.LamportClock <= snapshotMax {
				continue
			}
			if err := coll.loadVersion(entry.VectorID, Version{
				Embedding:    cloneVector(entry.Embedding),
				Text:         entry.Text,
				Metadata:     cloneMetadata(entry.Metadata),
				LamportClock: entry.LamportClock,
				ActorID:      entry.ActorID,
				WallClock:    entry.WallClock,
				Tombstone:    entry.Kind == walpkg.EntryTombstone,
			}); err != nil {
				_ = reader.Close()
				return nil, err
			}
		}
		if err := reader.Err(); err != nil {
			_ = reader.Close()
			return nil, err
		}
		if err := reader.Close(); err != nil {
			return nil, err
		}
	}
	coll.mu.Lock()
	coll.dirty = false
	coll.mu.Unlock()
	if coll.dim != meta.Dim || coll.bitWidth != meta.BitWidth || coll.seed != meta.Seed {
		if err := db.persistCollectionMeta(name, coll.bitWidth, coll.dim, coll.seed); err != nil {
			return nil, err
		}
	}
	return coll, nil
}

func (db *DB) newCollection(name string, meta collectionMeta) (*Collection, error) {
	if meta.BitWidth == 0 {
		meta.BitWidth = db.manifest.DefaultBitWidth
	}
	if meta.Seed == 0 {
		meta.Seed = generateSeed()
	}
	if err := os.MkdirAll(db.collectionDir(name), 0o755); err != nil {
		return nil, err
	}
	manager, err := walpkg.NewManager(db.collectionWALDir(name), db.walSegmentSize)
	if err != nil {
		return nil, err
	}
	return &Collection{
		db:       db,
		name:     name,
		bitWidth: meta.BitWidth,
		seed:     meta.Seed,
		encoder:  db.encoder,
		history:  make(map[string][]Version),
		clock:    newLamportClock(db.manifest.ActorID),
		wal:      manager,
	}, nil
}

func (db *DB) pruneSnapshots(name, keep string) error {
	matches, err := filepath.Glob(filepath.Join(db.collectionDir(name), "snapshot-*.csdb"))
	if err != nil {
		return err
	}
	for _, match := range matches {
		if match == keep {
			continue
		}
		if err := os.Remove(match); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (db *DB) collectionDir(name string) string {
	return filepath.Join(db.path, "collections", name)
}

func (db *DB) collectionIndexDir(name string) string {
	return filepath.Join(db.collectionDir(name), "index")
}

func (db *DB) collectionIndexPath(name string) string {
	return filepath.Join(db.collectionIndexDir(name), "quantized.tqi")
}

func (db *DB) collectionWALDir(name string) string {
	return filepath.Join(db.collectionDir(name), "wal")
}

func validCollectionName(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.Contains(name, string(os.PathSeparator))
}

func applyRuntimeConfig(m *manifest, provider EmbeddingProvider, peers []string) error {
	if m.Collections == nil {
		m.Collections = make(map[string]collectionMeta)
	}
	desired := describeEmbeddingProvider(provider)
	switch {
	case m.Embedding.ID == "":
		m.Embedding = desired
	case m.Embedding != desired:
		return fmt.Errorf("corkscrewdb: embedding config mismatch: manifest=%s/%d runtime=%s/%d", m.Embedding.ID, m.Embedding.Dim, desired.ID, desired.Dim)
	}
	if len(peers) > 0 {
		m.Peers = append([]string(nil), peers...)
	}
	return nil
}

func describeEmbeddingProvider(provider EmbeddingProvider) embeddingConfig {
	if provider == nil {
		return embeddingConfig{ID: "none", Dim: 0}
	}
	if named, ok := provider.(providerIdentifier); ok {
		return embeddingConfig{ID: named.ProviderID(), Dim: provider.Dim()}
	}
	return embeddingConfig{ID: reflect.TypeOf(provider).String(), Dim: provider.Dim()}
}

func sanitizePeers(peers []string) []string {
	if len(peers) == 0 {
		return nil
	}
	out := make([]string, 0, len(peers))
	seen := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		out = append(out, peer)
	}
	return out
}

func (db *DB) tryLoadCollectionIndex(name string) (*index, uint64, error) {
	path := db.collectionIndexPath(name)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	return loadIndexFile(path)
}
