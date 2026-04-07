package corkscrewdb

import (
	"sort"
	"time"
)

// Entry is the input payload for Put operations.
type Entry struct {
	Text     string
	Vector   []float32
	Metadata map[string]string
}

// Version is one immutable version in an entry's history.
type Version struct {
	Embedding    []float32
	Text         string
	Metadata     map[string]string
	LamportClock uint64
	ActorID      string
	WallClock    time.Time
	Tombstone    bool
}

// SearchResult is one ranked similarity-search hit.
type SearchResult struct {
	ID       string
	Score    float32
	Text     string
	Metadata map[string]string
	Version  uint64
}

func sortSearchResults(results []SearchResult) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].ID < results[j].ID
	})
}

// FilterOption restricts search results by exact metadata match.
type FilterOption struct {
	key   string
	value string
}

// Filter creates a metadata filter for search operations.
func Filter(key, value string) FilterOption {
	return FilterOption{key: key, value: value}
}

func (f FilterOption) matches(meta map[string]string) bool {
	if meta == nil {
		return false
	}
	return meta[f.key] == f.value
}

type putVectorConfig struct {
	text     string
	metadata map[string]string
}

// PutVectorOption configures PutVector calls.
type PutVectorOption interface {
	applyPutVector(*putVectorConfig)
}

type putVectorOptionFunc func(*putVectorConfig)

func (f putVectorOptionFunc) applyPutVector(cfg *putVectorConfig) {
	f(cfg)
}

// WithMetadata attaches metadata to PutVector.
func WithMetadata(meta map[string]string) PutVectorOption {
	return putVectorOptionFunc(func(cfg *putVectorConfig) {
		cfg.metadata = cloneMetadata(meta)
	})
}

// WithText stores source text alongside a PutVector write.
func WithText(text string) PutVectorOption {
	return putVectorOptionFunc(func(cfg *putVectorConfig) {
		cfg.text = text
	})
}

type collectionConfig struct {
	bitWidth int
}

// CollectionOption configures Collection creation.
type CollectionOption interface {
	applyCollection(*collectionConfig)
}

type collectionOptionFunc func(*collectionConfig)

func (f collectionOptionFunc) applyCollection(cfg *collectionConfig) {
	f(cfg)
}

// WithBitWidth sets the collection quantization bit width.
func WithBitWidth(bits int) CollectionOption {
	return collectionOptionFunc(func(cfg *collectionConfig) {
		cfg.bitWidth = bits
	})
}

func cloneMetadata(meta map[string]string) map[string]string {
	if len(meta) == 0 {
		return nil
	}
	out := make(map[string]string, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	return out
}

func cloneVector(vec []float32) []float32 {
	if len(vec) == 0 {
		return nil
	}
	out := make([]float32, len(vec))
	copy(out, vec)
	return out
}

func cloneVersion(v Version) Version {
	return Version{
		Embedding:    cloneVector(v.Embedding),
		Text:         v.Text,
		Metadata:     cloneMetadata(v.Metadata),
		LamportClock: v.LamportClock,
		ActorID:      v.ActorID,
		WallClock:    v.WallClock,
		Tombstone:    v.Tombstone,
	}
}

func matchesFilters(meta map[string]string, filters []FilterOption) bool {
	for _, f := range filters {
		if !f.matches(meta) {
			return false
		}
	}
	return true
}

func collectPutVectorOptions(opts []PutVectorOption) putVectorConfig {
	var cfg putVectorConfig
	for _, opt := range opts {
		if opt != nil {
			opt.applyPutVector(&cfg)
		}
	}
	return cfg
}
