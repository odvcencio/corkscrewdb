package corkscrewdb

import (
	"bytes"
	"errors"
	"math"
	"sort"

	"m31labs.dev/turboquant"
)

// candidate is a prospective search result carrying the stored quantized code
// and source text needed for the drift-check exact rerank (§5, Task 4).
type candidate struct {
	id             string
	storedCode     *turboquant.IPQuantized // pointer into live flat slice; valid under RLock
	text           string
	quantizedScore float32
	version        uint64
	metadata       map[string]string
}

// byteEqualQuantized compares two IPQuantized values for byte-level equality.
// ResNorm is compared BITWISE via math.Float32bits per §5.2 — this is
// load-bearing for NaN/-0 bit-pattern correctness. Two values are equal iff
// their MSE bytes, Signs bytes, AND ResNorm bit patterns are all identical.
func byteEqualQuantized(a, b *turboquant.IPQuantized) bool {
	return bytes.Equal(a.MSE, b.MSE) &&
		bytes.Equal(a.Signs, b.Signs) &&
		math.Float32bits(a.ResNorm) == math.Float32bits(b.ResNorm)
}

// coldFloats derives raw float32 vectors for the given candidates, branching on
// the collection's cold-tier mode:
//   - coldRecompute: one EncodeBatch call over all candidate texts (the SAME
//     graph entry point as the recompute write path — Resolution 1 / §5.6a).
//     No drift-check here; rerankExact does the per-candidate drift-check.
//   - coldRaw: read raw blobs from the raw store by each candidate's per-version
//     RawHash (looked up from history by ID+version). No drift-check needed:
//     raw bytes ARE the ground truth.
//   - coldNone: no raw floats available; returns an error. Callers fall back to
//     quantized scores for all candidates (§5.4).
//
// Returns (nil, err) on whole-batch failure so rerankExact can apply §5.4.
// The len(encoded)==0 guard is applied before indexing — a misbehaving provider
// returning (nil, nil) is treated as a failure.
func coldFloats(c *Collection, cands []candidate) ([][]float32, error) {
	switch c.coldTier {
	case coldRecompute:
		texts := make([]string, len(cands))
		for i, cand := range cands {
			texts[i] = cand.text
		}
		encoded, err := c.encoder.EncodeBatch(texts)
		if err != nil {
			return nil, err
		}
		// Guard against a misbehaving provider returning (nil, nil).
		if len(encoded) == 0 {
			return nil, errors.New("corkscrewdb: EncodeBatch returned no vectors")
		}
		return encoded, nil

	case coldRaw:
		// Read raw blobs for each candidate by looking up their version's RawHash
		// from the collection history (by ID+lamport clock). Best-effort per-candidate;
		// missing blobs result in nil entries (caller treats as quantized-score fallback).
		out := make([][]float32, len(cands))
		c.mu.RLock()
		hist := c.history
		c.mu.RUnlock()
		for i, cand := range cands {
			versions := hist[cand.id]
			var hash []byte
			for _, v := range versions {
				if v.Tombstone {
					continue
				}
				if v.LamportClock == cand.version && len(v.RawHash) > 0 {
					hash = v.RawHash
					break
				}
			}
			if len(hash) == 0 {
				continue
			}
			raw, err := c.getRaw(hash)
			if err != nil {
				continue
			}
			vec, err := decodeRawFloat32(raw)
			if err != nil {
				continue
			}
			out[i] = vec
		}
		return out, nil

	default:
		return nil, errors.New("corkscrewdb: cold-tier mode does not support raw floats")
	}
}

// decodeRawFloat32 decodes a little-endian byte slice back to a float32 slice.
// Inverse of rawFloat32Bytes in vector_storage.go.
func decodeRawFloat32(raw []byte) ([]float32, error) {
	if len(raw)%4 != 0 {
		return nil, errors.New("corkscrewdb: raw blob length is not a multiple of 4")
	}
	n := len(raw) / 4
	out := make([]float32, n)
	for i := range out {
		bits := uint32(raw[i*4]) | uint32(raw[i*4+1])<<8 | uint32(raw[i*4+2])<<16 | uint32(raw[i*4+3])<<24
		out[i] = math.Float32frombits(bits)
	}
	return out, nil
}

// rerankExact performs the drift-check exact rerank:
//
//  1. Call coldFloats once over ALL candidate texts (one EncodeBatch per query).
//  2. Per candidate: requantize the recomputed raw with the collection's own
//     quantizer and byteEqualQuantized against storedCode.
//     - MATCH ⇒ score = plain float dot Σ qvec[i]*raw[i] (exact).
//     - MISMATCH / empty text / zero vector / any failure ⇒ score = quantizedScore
//     (fallback — the stored quantized score).
//  3. Re-sort by score descending, truncate to k.
//
// On whole-batch coldFloats failure (§5.4): re-sort candidates by quantizedScore
// and return codes-only top-k. No error is surfaced to the caller.
func rerankExact(c *Collection, cands []candidate, qvec []float32, k int) []candidate {
	if len(cands) == 0 {
		return nil
	}

	raws, err := coldFloats(c, cands)
	if err != nil {
		// §5.4 whole-batch failure: fall back to codes-only ranking.
		sort.Slice(cands, func(i, j int) bool {
			if cands[i].quantizedScore != cands[j].quantizedScore {
				return cands[i].quantizedScore > cands[j].quantizedScore
			}
			return cands[i].id < cands[j].id
		})
		if len(cands) > k {
			cands = cands[:k]
		}
		return cands
	}

	q := turboquant.NewIPWithSeed(c.dim, c.bitWidth, c.seed)

	// Score each candidate.
	scores := make([]float32, len(cands))
	for i, cand := range cands {
		raw := raws[i]
		if len(raw) == 0 {
			// Empty text, zero vector, or missing raw — fallback.
			scores[i] = cand.quantizedScore
			continue
		}
		// Check for zero vector.
		allZero := true
		for _, v := range raw {
			if v != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			scores[i] = cand.quantizedScore
			continue
		}
		// Drift-check: requantize the recomputed raw and compare byte-for-byte.
		recomputed := q.Quantize(raw)
		if cand.storedCode == nil || !byteEqualQuantized(&recomputed, cand.storedCode) {
			// MISMATCH — fallback.
			scores[i] = cand.quantizedScore
			continue
		}
		// MATCH — plain float dot (NOT turboquant.InnerProduct).
		var dot float32
		for j, v := range qvec {
			dot += v * raw[j]
		}
		scores[i] = dot
	}

	// Re-sort by score descending (stable by ID for ties).
	type scoredCand struct {
		cand  candidate
		score float32
	}
	scored := make([]scoredCand, len(cands))
	for i, cand := range cands {
		scored[i] = scoredCand{cand: cand, score: scores[i]}
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].cand.id < scored[j].cand.id
	})

	// Build result slice with updated scores and truncate to k.
	if len(scored) > k {
		scored = scored[:k]
	}
	out := make([]candidate, len(scored))
	for i, sc := range scored {
		out[i] = sc.cand
		out[i].quantizedScore = sc.score // carry exact score through for result construction
	}
	return out
}
