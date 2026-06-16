package corkscrewdb

import (
	"errors"
	"fmt"

	snap "m31labs.dev/corkscrewdb/snapshot"
	walpkg "m31labs.dev/corkscrewdb/wal"
	"m31labs.dev/turboquant"
)

var errQuantizedOnlyRemoteWrite = errors.New("corkscrewdb: quantized_only vector storage is unsupported for federated or remote writes")

func normalizeVectorStorage(mode VectorStorageMode) VectorStorageMode {
	if mode == "" {
		return VectorStorageRaw
	}
	return mode
}

func validateVectorStorage(mode VectorStorageMode) (VectorStorageMode, error) {
	mode = normalizeVectorStorage(mode)
	switch mode {
	case VectorStorageRaw, VectorStorageQuantizedOnly:
		return mode, nil
	default:
		return "", fmt.Errorf("corkscrewdb: unsupported vector storage mode %q", mode)
	}
}

func manifestVectorStorage(mode VectorStorageMode) VectorStorageMode {
	if normalizeVectorStorage(mode) == VectorStorageRaw {
		return ""
	}
	return mode
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

func toWALChildren(children []MultiVectorChildVersion) []walpkg.ChildVector {
	if len(children) == 0 {
		return nil
	}
	out := make([]walpkg.ChildVector, len(children))
	for i, child := range children {
		out[i] = walpkg.ChildVector{
			ID:        child.ID,
			Embedding: cloneVector(child.Embedding),
			Quantized: toWALQuantized(child.quantized),
			Dim:       child.dim,
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
		}
	}
	return out
}

func fromWALChildren(children []walpkg.ChildVector) []MultiVectorChildVersion {
	if len(children) == 0 {
		return nil
	}
	out := make([]MultiVectorChildVersion, len(children))
	for i, child := range children {
		out[i] = MultiVectorChildVersion{
			ID:        child.ID,
			Embedding: cloneVector(child.Embedding),
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
			quantized: fromWALQuantized(child.Quantized),
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

func toSnapshotChildren(children []MultiVectorChildVersion) []snap.ChildVector {
	if len(children) == 0 {
		return nil
	}
	out := make([]snap.ChildVector, len(children))
	for i, child := range children {
		out[i] = snap.ChildVector{
			ID:        child.ID,
			Embedding: cloneVector(child.Embedding),
			Quantized: toSnapshotQuantized(child.quantized),
			Dim:       child.dim,
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
		}
	}
	return out
}

func fromSnapshotChildren(children []snap.ChildVector) []MultiVectorChildVersion {
	if len(children) == 0 {
		return nil
	}
	out := make([]MultiVectorChildVersion, len(children))
	for i, child := range children {
		out[i] = MultiVectorChildVersion{
			ID:        child.ID,
			Embedding: cloneVector(child.Embedding),
			Text:      child.Text,
			Metadata:  cloneMetadata(child.Metadata),
			quantized: fromSnapshotQuantized(child.Quantized),
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
		return errors.New("corkscrewdb: collection dimension is required for quantized_only versions")
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
