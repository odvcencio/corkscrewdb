package corkscrewdb

import (
	"bytes"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	grpcapi "m31labs.dev/corkscrewdb/grpc"
	snap "m31labs.dev/corkscrewdb/snapshot"
	walpkg "m31labs.dev/corkscrewdb/wal"
	"m31labs.dev/turboquant"
)

// unitAndScaledVectors returns a random unit-length direction vector and an
// exact 2x-scaled copy (same direction, double magnitude). turboquant computes
// IPQuantized.Norm directly from the input's exact L2 norm, so doubling the
// input must exactly double Norm, leave the direction-only fields (MSE/Signs/
// ResNorm) byte-identical, and exactly double the InnerProductPrepared score
// against any fixed query — this is the true-MIPS property the migration
// exists to preserve through persistence (as opposed to a cosine/unit-space
// scheme, where a 2x-scaled vector would score identically to the original).
func unitAndScaledVectors(dim int, seed int64) (unit, scaled []float32) {
	rng := rand.New(rand.NewSource(seed))
	unit = randVec(rng, dim)
	normalizeVector(unit)
	scaled = make([]float32, dim)
	for i, v := range unit {
		scaled[i] = v * 2
	}
	return unit, scaled
}

// assertDirectionOnlyFieldsEqual verifies that two IPQuantized values which
// differ only in input magnitude quantize to byte-identical MSE/Signs/ResNorm
// (the direction-only fields) and differ only in Norm. This is the property
// that makes byteEqualQuantized's Norm comparison (rerank.go) load-bearing:
// without it, a magnitude-only drift would be misreported as a MATCH.
func assertDirectionOnlyFieldsEqual(t *testing.T, unit, scaled turboquant.IPQuantized) {
	t.Helper()
	if !bytes.Equal(unit.MSE, scaled.MSE) {
		t.Fatalf("MSE differs for a pure magnitude scale: unit=%x scaled=%x", unit.MSE, scaled.MSE)
	}
	if !bytes.Equal(unit.Signs, scaled.Signs) {
		t.Fatalf("Signs differs for a pure magnitude scale: unit=%x scaled=%x", unit.Signs, scaled.Signs)
	}
	if unit.ResNorm != scaled.ResNorm {
		t.Fatalf("ResNorm differs for a pure magnitude scale: unit=%v scaled=%v", unit.ResNorm, scaled.ResNorm)
	}
	if math.Abs(float64(scaled.Norm-2*unit.Norm)) > 1e-3*float64(unit.Norm) {
		t.Fatalf("Norm did not double: unit=%v scaled=%v want~%v", unit.Norm, scaled.Norm, 2*unit.Norm)
	}
}

// assertTrueMIPSRatio verifies scoreScaled/scoreUnit ~= 2, the true-MIPS
// signature: a cosine/unit-space scheme would instead score the pair
// identically (ratio ~= 1).
func assertTrueMIPSRatio(t *testing.T, label string, scoreUnit, scoreScaled float32) {
	t.Helper()
	if scoreUnit == 0 {
		t.Fatalf("%s: scoreUnit is exactly 0; test does not exercise a meaningful score", label)
	}
	ratio := float64(scoreScaled) / float64(scoreUnit)
	if math.Abs(ratio-2) > 1e-3 {
		t.Fatalf("%s: true-MIPS score ratio = %v, want ~2 (scoreUnit=%v scoreScaled=%v)", label, ratio, scoreUnit, scoreScaled)
	}
}

// TestWALTrueMIPSNonUnitNormRoundTrip proves a non-unit vector round-trips
// through the WAL wire format (wal/entry.go) with its Norm intact, and that
// the reloaded codes score as true MIPS: a 2x-scaled vector scores 2x the
// unit vector's score against the same query.
func TestWALTrueMIPSNonUnitNormRoundTrip(t *testing.T) {
	const dim, bitWidth = 16, 4
	const seed = int64(2026)
	q := turboquant.NewIPWithSeed(dim, bitWidth, seed)

	unit, scaled := unitAndScaledVectors(dim, seed)
	qxUnit := q.Quantize(unit)
	qxScaled := q.Quantize(scaled)
	assertDirectionOnlyFieldsEqual(t, qxUnit, qxScaled)

	entryUnit := walpkg.Entry{
		Kind: walpkg.EntryPut, CollectionID: "c", VectorID: "unit",
		Quantized: toWALQuantized(&qxUnit), Dim: dim, ActorID: "a", WallClock: time.Now().UTC(),
	}
	entryScaled := walpkg.Entry{
		Kind: walpkg.EntryPut, CollectionID: "c", VectorID: "scaled",
		Quantized: toWALQuantized(&qxScaled), Dim: dim, ActorID: "a", WallClock: time.Now().UTC(),
	}

	dataUnit, err := entryUnit.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary(unit): %v", err)
	}
	dataScaled, err := entryScaled.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary(scaled): %v", err)
	}

	gotUnit, err := walpkg.ReadEntry(bytes.NewReader(dataUnit))
	if err != nil {
		t.Fatalf("ReadEntry(unit): %v", err)
	}
	gotScaled, err := walpkg.ReadEntry(bytes.NewReader(dataScaled))
	if err != nil {
		t.Fatalf("ReadEntry(scaled): %v", err)
	}

	if gotUnit.Quantized.Norm != qxUnit.Norm {
		t.Fatalf("WAL: unit Norm not preserved: got %v want %v", gotUnit.Quantized.Norm, qxUnit.Norm)
	}
	if gotScaled.Quantized.Norm != qxScaled.Norm {
		t.Fatalf("WAL: scaled Norm not preserved: got %v want %v", gotScaled.Quantized.Norm, qxScaled.Norm)
	}

	rtUnit := fromWALQuantized(gotUnit.Quantized)
	rtScaled := fromWALQuantized(gotScaled.Quantized)

	pq := q.PrepareQuery(unit) // query = the unit direction itself: robust, clearly non-zero alignment
	scoreUnit := q.InnerProductPrepared(*rtUnit, pq)
	scoreScaled := q.InnerProductPrepared(*rtScaled, pq)
	assertTrueMIPSRatio(t, "WAL", scoreUnit, scoreScaled)
}

// TestSnapshotTrueMIPSNonUnitNormRoundTrip proves a non-unit vector round-trips
// through the snapshot wire format (snapshot/snapshot.go) with its Norm intact,
// and that the reloaded codes score as true MIPS.
func TestSnapshotTrueMIPSNonUnitNormRoundTrip(t *testing.T) {
	const dim, bitWidth = 16, 4
	const seed = int64(3033)
	q := turboquant.NewIPWithSeed(dim, bitWidth, seed)

	unit, scaled := unitAndScaledVectors(dim, seed)
	qxUnit := q.Quantize(unit)
	qxScaled := q.Quantize(scaled)
	assertDirectionOnlyFieldsEqual(t, qxUnit, qxScaled)

	now := time.Now().UTC()
	data := snap.Data{
		Collection: "c", BitWidth: bitWidth, Seed: seed, Dim: dim, CreatedAt: now,
		Records: []snap.Record{
			{ID: "unit", Versions: []snap.Version{{
				Quantized: toSnapshotQuantized(&qxUnit), LamportClock: 1, ActorID: "a", WallClock: now,
			}}},
			{ID: "scaled", Versions: []snap.Version{{
				Quantized: toSnapshotQuantized(&qxScaled), LamportClock: 2, ActorID: "a", WallClock: now,
			}}},
		},
	}

	path := filepath.Join(t.TempDir(), "snapshot-mips.csdb")
	if err := snap.WriteFile(path, data); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	loaded, err := snap.LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	var gotUnit, gotScaled *snap.QuantizedVector
	for _, rec := range loaded.Records {
		switch rec.ID {
		case "unit":
			gotUnit = rec.Versions[0].Quantized
		case "scaled":
			gotScaled = rec.Versions[0].Quantized
		}
	}
	if gotUnit == nil || gotScaled == nil {
		t.Fatalf("missing records after reload: %+v", loaded.Records)
	}
	if gotUnit.Norm != qxUnit.Norm {
		t.Fatalf("snapshot: unit Norm not preserved: got %v want %v", gotUnit.Norm, qxUnit.Norm)
	}
	if gotScaled.Norm != qxScaled.Norm {
		t.Fatalf("snapshot: scaled Norm not preserved: got %v want %v", gotScaled.Norm, qxScaled.Norm)
	}

	rtUnit := fromSnapshotQuantized(gotUnit)
	rtScaled := fromSnapshotQuantized(gotScaled)

	pq := q.PrepareQuery(unit)
	scoreUnit := q.InnerProductPrepared(*rtUnit, pq)
	scoreScaled := q.InnerProductPrepared(*rtScaled, pq)
	assertTrueMIPSRatio(t, "snapshot", scoreUnit, scoreScaled)
}

// TestSnapshotCompactOrdinalTrueMIPSNonUnitNormRoundTrip covers the compact
// columnar child encoding (childEncodingCompactQuantizedOrdinal) specifically,
// since it stores the Norm column separately from the per-record path above.
func TestSnapshotCompactOrdinalTrueMIPSNonUnitNormRoundTrip(t *testing.T) {
	const dim, bitWidth = 16, 4
	const seed = int64(3131)
	q := turboquant.NewIPWithSeed(dim, bitWidth, seed)

	unit, scaled := unitAndScaledVectors(dim, seed)
	qxUnit := q.Quantize(unit)
	qxScaled := q.Quantize(scaled)
	assertDirectionOnlyFieldsEqual(t, qxUnit, qxScaled)

	now := time.Now().UTC()
	data := snap.Data{
		Collection: "c", BitWidth: bitWidth, Seed: seed, Dim: dim, CreatedAt: now,
		Records: []snap.Record{
			{ID: "parent", Versions: []snap.Version{{
				Children: []snap.ChildVector{
					{ID: "0", Quantized: toSnapshotQuantized(&qxUnit), Dim: dim},
					{ID: "1", Quantized: toSnapshotQuantized(&qxScaled), Dim: dim},
				},
				LamportClock: 1, ActorID: "a", WallClock: now,
			}}},
		},
	}

	path := filepath.Join(t.TempDir(), "snapshot-mips-compact.csdb")
	if err := snap.WriteFile(path, data); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	loaded, err := snap.LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	children := loaded.Records[0].Versions[0].Children
	if len(children) != 2 {
		t.Fatalf("children = %+v, want 2", children)
	}
	gotUnit, gotScaled := children[0].Quantized, children[1].Quantized
	if gotUnit.Norm != qxUnit.Norm {
		t.Fatalf("compact: unit Norm not preserved: got %v want %v", gotUnit.Norm, qxUnit.Norm)
	}
	if gotScaled.Norm != qxScaled.Norm {
		t.Fatalf("compact: scaled Norm not preserved: got %v want %v", gotScaled.Norm, qxScaled.Norm)
	}

	rtUnit := fromSnapshotQuantized(gotUnit)
	rtScaled := fromSnapshotQuantized(gotScaled)

	pq := q.PrepareQuery(unit)
	scoreUnit := q.InnerProductPrepared(*rtUnit, pq)
	scoreScaled := q.InnerProductPrepared(*rtScaled, pq)
	assertTrueMIPSRatio(t, "snapshot-compact", scoreUnit, scoreScaled)
}

// TestIndexFileTrueMIPSNonUnitNormRoundTrip proves a non-unit vector round-trips
// through the .tqi index file format (index_file.go) with its Norm intact, and
// that Search over the reloaded index ranks by true MIPS end to end.
func TestIndexFileTrueMIPSNonUnitNormRoundTrip(t *testing.T) {
	const dim, bitWidth = 16, 4
	const seed = int64(4044)
	idx := newIndex(dim, bitWidth, seed)

	unit, scaled := unitAndScaledVectors(dim, seed)
	idx.Add("unit", unit, "", nil, 1)
	idx.Add("scaled", scaled, "", nil, 2)

	path := filepath.Join(t.TempDir(), "mips.tqi")
	if err := saveIndexFile(path, idx, 2, false, false); err != nil {
		t.Fatalf("saveIndexFile: %v", err)
	}

	loaded, _, err := loadIndexFile(path)
	if err != nil {
		t.Fatalf("loadIndexFile: %v", err)
	}

	results := loaded.Search(unit, 2, nil) // query = the unit direction itself
	if len(results) != 2 {
		t.Fatalf("results = %+v, want 2 hits", results)
	}
	scores := map[string]float32{}
	for _, r := range results {
		scores[r.ID] = r.Score
	}
	assertTrueMIPSRatio(t, "index_file", scores["unit"], scores["scaled"])

	// "scaled" must outrank "unit" for the same direction under true MIPS.
	if results[0].ID != "scaled" {
		t.Fatalf("top result = %q, want %q (true MIPS ranks the larger-magnitude same-direction vector first)", results[0].ID, "scaled")
	}
}

// TestFromProtoQuantizedNormPresence proves fromProtoQuantized's B1 decode
// semantics end to end: an ABSENT norm field (old peer, pre-B1 rolling
// upgrade) defaults Norm to 1, matching every at-rest reader's legacy
// default, while a PRESENT norm of 0 (a genuine zero vector) is trusted
// as-is and decodes to Norm == 0 — the two cases a plain (non-optional)
// proto3 float could not distinguish.
func TestFromProtoQuantizedNormPresence(t *testing.T) {
	t.Run("absent defaults to 1", func(t *testing.T) {
		qv, err := fromProtoQuantized(&grpcapi.QuantizedVector{Mse: []byte{1}, Signs: []byte{2}, ResNorm: 0.5})
		if err != nil {
			t.Fatalf("fromProtoQuantized: %v", err)
		}
		if qv.Norm != 1 {
			t.Fatalf("Norm = %v, want 1 (absent field defaults to unit-space)", qv.Norm)
		}
	})

	t.Run("present zero is trusted as-is", func(t *testing.T) {
		qv, err := fromProtoQuantized(&grpcapi.QuantizedVector{Mse: []byte{1}, Signs: []byte{2}, ResNorm: 0.5, Norm: proto.Float32(0)})
		if err != nil {
			t.Fatalf("fromProtoQuantized: %v", err)
		}
		if qv.Norm != 0 {
			t.Fatalf("Norm = %v, want 0 (present zero is a genuine zero vector, not a legacy default)", qv.Norm)
		}
	})

	t.Run("present nonzero round-trips", func(t *testing.T) {
		qv, err := fromProtoQuantized(&grpcapi.QuantizedVector{Mse: []byte{1}, Signs: []byte{2}, ResNorm: 0.5, Norm: proto.Float32(3.5)})
		if err != nil {
			t.Fatalf("fromProtoQuantized: %v", err)
		}
		if qv.Norm != 3.5 {
			t.Fatalf("Norm = %v, want 3.5", qv.Norm)
		}
	})
}

// TestFromProtoQuantizedRejectsInvalidNorm proves m1's decode-time
// validation: fromProtoQuantized rejects a present NaN, +/-Inf, or negative
// norm, mirroring turboquant's own wire codec (wire.go:144). A negative norm
// silently inverts ScoreUpperBound into a lower bound and drops real top-k
// members, so untrusted wire input must never construct one.
func TestFromProtoQuantizedRejectsInvalidNorm(t *testing.T) {
	cases := []struct {
		name string
		norm float32
	}{
		{"NaN", float32(math.NaN())},
		{"+Inf", float32(math.Inf(1))},
		{"-Inf", float32(math.Inf(-1))},
		{"negative", -1.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fromProtoQuantized(&grpcapi.QuantizedVector{Mse: []byte{1}, Signs: []byte{2}, ResNorm: 0.5, Norm: proto.Float32(tc.norm)})
			if err == nil {
				t.Fatalf("fromProtoQuantized(norm=%v): want error, got nil", tc.norm)
			}
		})
	}
}
