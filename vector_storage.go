package corkscrewdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	snap "m31labs.dev/corkscrewdb/snapshot"
	walpkg "m31labs.dev/corkscrewdb/wal"
	"m31labs.dev/turboquant"
)

// errRemoteUnsupportedPendingDistribution gates every multi-node path that
// would serialize stored versions / inline embeddings over the wire. WAL v5
// dropped inline Embedding; the Distribution phase rebuilds replication to
// stream codes over a new wire and pull raw by hash, at which point this error
// and these gates are removed. Returning it IS the Phase 1 behavior.
var errRemoteUnsupportedPendingDistribution = errors.New(
	"corkscrewdb: multi-node replication/federation/rebalance is disabled in v0.3.0 Phase 1; restored in the Distribution phase (code-carrying replication over codes + raw pull-by-hash)")

func indexTypeString(t IndexType) string {
	if t == IndexHNSW {
		return "hnsw"
	}
	return "flat"
}

// rawFloat32Bytes encodes a float32 slice little-endian for the raw store.
func rawFloat32Bytes(vec []float32) []byte {
	out := make([]byte, len(vec)*4)
	for i, v := range vec {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(v))
	}
	return out
}

func toWALQuantized(qv *turboquant.IPQuantized) *walpkg.QuantizedVector {
	if qv == nil {
		return nil
	}
	return &walpkg.QuantizedVector{
		MSE:     append([]byte(nil), qv.MSE...),
		Signs:   append([]byte(nil), qv.Signs...),
		ResNorm: qv.ResNorm,
	}
}

// cloneWALQuantized deep-clones a wal.QuantizedVector so a replicated/streamed
// WAL payload is carried verbatim with no aliasing of the wire buffers.
func cloneWALQuantized(qv *walpkg.QuantizedVector) *walpkg.QuantizedVector {
	if qv == nil {
		return nil
	}
	return &walpkg.QuantizedVector{
		MSE:     append([]byte(nil), qv.MSE...),
		Signs:   append([]byte(nil), qv.Signs...),
		ResNorm: qv.ResNorm,
	}
}

// cloneWALSparse deep-clones a wal.SparseBlock.
func cloneWALSparse(sb *walpkg.SparseBlock) *walpkg.SparseBlock {
	if sb == nil {
		return nil
	}
	return &walpkg.SparseBlock{
		Indices: append([]uint32(nil), sb.Indices...),
		Values:  append([]float32(nil), sb.Values...),
	}
}

// cloneWALChildren deep-clones a slice of wal.ChildVector.
func cloneWALChildren(children []walpkg.ChildVector) []walpkg.ChildVector {
	if len(children) == 0 {
		return nil
	}
	out := make([]walpkg.ChildVector, len(children))
	for i, child := range children {
		out[i] = walpkg.ChildVector{
			ID:        child.ID,
			Quantized: cloneWALQuantized(child.Quantized),
			Dim:       child.Dim,
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
		}
	}
	return out
}

func fromWALQuantized(qv *walpkg.QuantizedVector) *turboquant.IPQuantized {
	if qv == nil {
		return nil
	}
	out := turboquant.IPQuantized{
		MSE:     append([]byte(nil), qv.MSE...),
		Signs:   append([]byte(nil), qv.Signs...),
		ResNorm: qv.ResNorm,
	}
	return &out
}

func toWALSparse(sv *SparseVector) *walpkg.SparseBlock {
	if sv == nil || len(sv.Indices) == 0 {
		return nil
	}
	return &walpkg.SparseBlock{
		Indices: append([]uint32(nil), sv.Indices...),
		Values:  append([]float32(nil), sv.Values...),
	}
}

func fromWALSparse(sb *walpkg.SparseBlock) *SparseVector {
	if sb == nil {
		return nil
	}
	return &SparseVector{Indices: append([]uint32(nil), sb.Indices...), Values: append([]float32(nil), sb.Values...)}
}

func toWALChildren(children []ChildVector) []walpkg.ChildVector {
	if len(children) == 0 {
		return nil
	}
	out := make([]walpkg.ChildVector, len(children))
	for i, child := range children {
		out[i] = walpkg.ChildVector{
			ID:        child.ID,
			Quantized: toWALQuantized(child.Quantized),
			Dim:       child.dim,
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
		}
	}
	return out
}

func fromWALChildren(children []walpkg.ChildVector) []ChildVector {
	if len(children) == 0 {
		return nil
	}
	out := make([]ChildVector, len(children))
	for i, child := range children {
		out[i] = ChildVector{
			ID:        child.ID,
			Quantized: fromWALQuantized(child.Quantized),
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
			dim:       child.Dim,
		}
	}
	return out
}

func toSnapshotQuantized(qv *turboquant.IPQuantized) *snap.QuantizedVector {
	if qv == nil {
		return nil
	}
	return &snap.QuantizedVector{
		MSE:     append([]byte(nil), qv.MSE...),
		Signs:   append([]byte(nil), qv.Signs...),
		ResNorm: qv.ResNorm,
	}
}

func fromSnapshotQuantized(qv *snap.QuantizedVector) *turboquant.IPQuantized {
	if qv == nil {
		return nil
	}
	out := turboquant.IPQuantized{
		MSE:     append([]byte(nil), qv.MSE...),
		Signs:   append([]byte(nil), qv.Signs...),
		ResNorm: qv.ResNorm,
	}
	return &out
}

func toSnapshotSparse(sv *SparseVector) *snap.SparseBlock {
	if sv == nil || len(sv.Indices) == 0 {
		return nil
	}
	return &snap.SparseBlock{
		Indices: append([]uint32(nil), sv.Indices...),
		Values:  append([]float32(nil), sv.Values...),
	}
}

func fromSnapshotSparse(sb *snap.SparseBlock) *SparseVector {
	if sb == nil {
		return nil
	}
	return &SparseVector{Indices: append([]uint32(nil), sb.Indices...), Values: append([]float32(nil), sb.Values...)}
}

func toSnapshotChildren(children []ChildVector) []snap.ChildVector {
	if len(children) == 0 {
		return nil
	}
	out := make([]snap.ChildVector, len(children))
	for i, child := range children {
		out[i] = snap.ChildVector{
			ID:        child.ID,
			Quantized: toSnapshotQuantized(child.Quantized),
			Dim:       child.dim,
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
		}
	}
	return out
}

func fromSnapshotChildren(children []snap.ChildVector) []ChildVector {
	if len(children) == 0 {
		return nil
	}
	out := make([]ChildVector, len(children))
	for i, child := range children {
		out[i] = ChildVector{
			ID:        child.ID,
			Quantized: fromSnapshotQuantized(child.Quantized),
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
			dim:       child.Dim,
		}
	}
	return out
}

func validateQuantizedPayload(qv *turboquant.IPQuantized, dim, bitWidth int) error {
	if qv == nil {
		return errors.New("corkscrewdb: missing quantized embedding")
	}
	if dim <= 0 {
		return errors.New("corkscrewdb: collection dimension is required for quantized versions")
	}
	if bitWidth < 2 {
		return fmt.Errorf("corkscrewdb: invalid quantized payload bit width %d", bitWidth)
	}
	var mseBytes, signBytes int
	var panicValue any
	func() {
		defer func() {
			panicValue = recover()
		}()
		mseBytes, signBytes = turboquant.IPQuantizedSizes(dim, bitWidth)
	}()
	if panicValue != nil {
		return fmt.Errorf("corkscrewdb: invalid quantized payload parameters dim=%d bit_width=%d: %v", dim, bitWidth, panicValue)
	}
	if len(qv.MSE) != mseBytes {
		return fmt.Errorf("corkscrewdb: invalid quantized payload MSE length %d for dim=%d bit_width=%d, want %d", len(qv.MSE), dim, bitWidth, mseBytes)
	}
	if len(qv.Signs) != signBytes {
		return fmt.Errorf("corkscrewdb: invalid quantized payload sign length %d for dim=%d bit_width=%d, want %d", len(qv.Signs), dim, bitWidth, signBytes)
	}
	return nil
}
