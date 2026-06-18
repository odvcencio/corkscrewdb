package corkscrewdb

import (
	"errors"
	"fmt"
)

// multiOverfetch is the per-channel candidate depth used before fusion. It is a
// tunable (§4.3, R4): max(k, 100). Correctness is depth-independent.
const multiOverfetch = 100

// MultiQuery describes a hybrid retrieval request across the dense and sparse
// channels. At least one of Dense, Sparse, or Text must be present. Text, when
// Dense is empty, is encoded into Dense via the collection's provider. Fusion
// selects the policy used to combine channels; nil defaults to RRFFusion{K:60}.
type MultiQuery struct {
	Text    string
	Dense   []float32
	Sparse  *SparseVector
	Filters []FilterOption
	Fusion  FusionPolicy
}

// hydrateMeta is a per-id snapshot of the fields the sparse path needs after
// scoring outside the lock.
type hydrateMeta struct {
	text     string
	metadata map[string]string
	version  uint64
}

// SearchMulti runs a hybrid (dense + sparse) retrieval and ALWAYS returns the
// fused ranking; SearchResult.Score is always the fused score, even for a single
// present channel.
func (c *Collection) SearchMulti(q MultiQuery, k int) ([]SearchResult, error) {
	if err := c.usable(); err != nil {
		return nil, err
	}
	if c.remote != nil || c.db.shouldFederate() {
		return nil, errRemoteUnsupportedPendingDistribution
	}

	// Resolve Text -> Dense via the provider when Dense is absent.
	dense := q.Dense
	if len(dense) == 0 && q.Text != "" {
		encoded, err := c.encoder.Encode(q.Text)
		if err != nil {
			return nil, err
		}
		dense = encoded
	}

	// Snapshot index / dim / sparse set / per-id hydrate map under the RLock; do
	// all scoring outside the lock (mirrors searchVectorLocal).
	c.mu.RLock()
	idx := c.index
	dim := c.dim
	sparseSet := c.sparseSet
	var hydrateByID map[string]hydrateMeta
	if q.Sparse != nil && len(q.Sparse.Indices) > 0 {
		hydrateByID = make(map[string]hydrateMeta, len(sparseSet))
		for id := range sparseSet {
			versions := c.history[id]
			if len(versions) == 0 {
				continue
			}
			latest := versions[len(versions)-1]
			if latest.Tombstone {
				continue
			}
			hydrateByID[id] = hydrateMeta{
				text:     latest.Text,
				metadata: cloneMetadata(latest.Metadata),
				version:  latest.LamportClock,
			}
		}
	}
	c.mu.RUnlock()

	return fuseChannels(idx, dim, sparseSet, hydrateByID, c.sparseEnabled, dense, q, k)
}

// SearchMulti runs a hybrid (dense + sparse) retrieval against the point-in-time
// view and ALWAYS returns the fused ranking; SearchResult.Score is always the
// fused score. This is an additive sibling of Collection.SearchMulti.
func (v *CollectionView) SearchMulti(q MultiQuery, k int) ([]SearchResult, error) {
	if v.err != nil {
		return nil, v.err
	}
	if v.remote != nil {
		return nil, errRemoteUnsupportedPendingDistribution
	}

	dense := q.Dense
	if len(dense) == 0 && q.Text != "" {
		encoded, err := v.encoder.Encode(q.Text)
		if err != nil {
			return nil, err
		}
		dense = encoded
	}

	var hydrateByID map[string]hydrateMeta
	if q.Sparse != nil && len(q.Sparse.Indices) > 0 {
		hydrateByID = make(map[string]hydrateMeta, len(v.sparseSet))
		for id := range v.sparseSet {
			versions := v.history[id]
			if len(versions) == 0 {
				continue
			}
			latest := versions[len(versions)-1]
			if latest.Tombstone {
				continue
			}
			hydrateByID[id] = hydrateMeta{
				text:     latest.Text,
				metadata: cloneMetadata(latest.Metadata),
				version:  latest.LamportClock,
			}
		}
	}

	return fuseChannels(v.index, v.dim, v.sparseSet, hydrateByID, v.sparseEnabled, dense, q, k)
}

// fuseChannels is the shared resolve/score/fuse core used by both
// Collection.SearchMulti and CollectionView.SearchMulti. dense is the
// already-resolved dense query (Text already encoded by the caller).
func fuseChannels(
	idx indexer,
	dim int,
	sparseSet map[string]*SparseVector,
	hydrateByID map[string]hydrateMeta,
	sparseEnabled bool,
	dense []float32,
	q MultiQuery,
	k int,
) ([]SearchResult, error) {
	densePresent := len(dense) > 0
	if densePresent && idx != nil && len(dense) != dim {
		return nil, fmt.Errorf("corkscrewdb: query dimension %d does not match collection dimension %d", len(dense), dim)
	}

	if q.Sparse != nil {
		if err := validateSparseVector(q.Sparse); err != nil {
			return nil, err
		}
		if !sparseEnabled {
			return nil, errors.New("corkscrewdb: sparse query supplied but collection was not created WithSparse")
		}
	}
	sparsePresent := q.Sparse != nil && len(q.Sparse.Indices) > 0

	if !densePresent && !sparsePresent {
		return nil, errors.New("corkscrewdb: SearchMulti requires at least one of Dense, Sparse, or Text")
	}
	if k <= 0 {
		return nil, nil
	}

	depth := k
	if depth < multiOverfetch {
		depth = multiOverfetch
	}

	var denseRanking []SearchResult
	if densePresent && idx != nil {
		denseRanking = idx.Search(dense, depth, q.Filters)
	}

	var sparseRanking []SearchResult
	if sparsePresent {
		accept := func(id string) bool {
			meta, ok := hydrateByID[id]
			if !ok {
				return false
			}
			return matchesFilters(meta.metadata, q.Filters)
		}
		hydrate := func(id string) (string, map[string]string, uint64) {
			meta := hydrateByID[id]
			return meta.text, meta.metadata, meta.version
		}
		sparseRanking = sparseSearch(sparseSet, q.Sparse, depth, accept, hydrate)
	}

	policy := q.Fusion
	if policy == nil {
		policy = RRFFusion{K: 60}
	}
	fused := policy.fuse(denseRanking, sparseRanking)
	if len(fused) > k {
		fused = fused[:k]
	}
	return fused, nil
}
