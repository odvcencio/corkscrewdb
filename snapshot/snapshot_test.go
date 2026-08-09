package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot-00001.csdb")
	want := Data{
		Collection: "docs",
		BitWidth:   2,
		Seed:       42,
		Dim:        3,
		MaxLamport: 7,
		CreatedAt:  time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
		Records: []Record{
			{
				ID: "doc-1",
				Versions: []Version{
					{
						Quantized:    &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 0.5},
						Text:         "hello world",
						Metadata:     map[string]string{"source": "test"},
						LamportClock: 7,
						ActorID:      "actor-1",
						WallClock:    time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	}
	if err := WriteFile(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Collection != want.Collection {
		t.Fatalf("Collection = %q, want %q", got.Collection, want.Collection)
	}
	if len(got.Records) != 1 || got.Records[0].ID != "doc-1" {
		t.Fatalf("Records = %+v", got.Records)
	}
	if len(got.Records[0].Versions) != 1 {
		t.Fatalf("Versions = %+v", got.Records[0].Versions)
	}
	if got.Records[0].Versions[0].Text != "hello world" {
		t.Fatalf("Text = %q", got.Records[0].Versions[0].Text)
	}
}

func TestSnapshotCompactOrdinalQuantizedChildrenRejectsHugeLength(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot-huge-compact.csdb")
	payload, err := marshal(Data{
		Collection: "docs",
		BitWidth:   2,
		Seed:       42,
		Dim:        4,
		MaxLamport: 12,
		CreatedAt:  time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
		Records: []Record{
			{
				ID: "parent-1",
				Versions: []Version{
					{
						Children: []ChildVector{
							{
								ID:        "0",
								Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{2}, ResNorm: 0.5},
								Dim:       4,
							},
						},
						LamportClock: 12,
						ActorID:      "actor-1",
						WallClock:    time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	compactHeader := []byte{
		1, 0, 0, 0, // child count
		childEncodingCompactQuantizedOrdinal,
		4, 0, 0, 0, // child dim
		1, 0, 0, 0, // MSE length
		1, 0, 0, 0, // signs length
	}
	offset := bytes.Index(payload, compactHeader)
	if offset < 0 {
		t.Fatal("compact child header not found")
	}
	binary.LittleEndian.PutUint32(payload[offset+9:], uint32(maxCompactChildBlockBytes+1))
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = LoadFile(path)
	if err == nil || !strings.Contains(err.Error(), "compact child MSE block too large") {
		t.Fatalf("LoadFile err = %v, want compact child MSE block too large", err)
	}
}

func TestSnapshotPackedChildrenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot-00002.csdb")
	want := Data{
		Collection: "docs",
		BitWidth:   4,
		Seed:       42,
		Dim:        4,
		MaxLamport: 9,
		CreatedAt:  time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
		Records: []Record{
			{
				ID: "parent-1",
				Versions: []Version{
					{
						Children: []ChildVector{
							{
								ID:        "child-1",
								Quantized: &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 0.75},
								Dim:       4,
								Text:      "child text",
								Metadata:  map[string]string{"slot": "one"},
							},
						},
						Text:         "parent text",
						Metadata:     map[string]string{"tenant": "acme"},
						LamportClock: 9,
						ActorID:      "actor-1",
						WallClock:    time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	}
	if err := WriteFile(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Records) != 1 || len(got.Records[0].Versions) != 1 {
		t.Fatalf("got = %+v", got)
	}
	children := got.Records[0].Versions[0].Children
	if len(children) != 1 || children[0].ID != "child-1" || children[0].Quantized == nil || children[0].Metadata["slot"] != "one" {
		t.Fatalf("children = %+v", children)
	}
}

func TestSnapshotCompactOrdinalQuantizedChildrenRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot-00003.csdb")
	children := make([]ChildVector, 64)
	for i := range children {
		children[i] = ChildVector{
			ID: strconv.Itoa(i),
			Quantized: &QuantizedVector{
				MSE:     []byte{byte(i), byte(i + 1), byte(i + 2)},
				Signs:   []byte{byte(255 - i), byte(i % 7)},
				ResNorm: float32(i) + 0.5,
			},
			Dim: 128,
		}
	}
	want := Data{
		Collection: "docs",
		BitWidth:   2,
		Seed:       42,
		Dim:        128,
		MaxLamport: 11,
		CreatedAt:  time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
		Records: []Record{
			{
				ID: "parent-1",
				Versions: []Version{
					{
						Children:     children,
						LamportClock: 11,
						ActorID:      "actor-1",
						WallClock:    time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC),
					},
				},
			},
		},
	}
	payload, err := marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) >= 1200 {
		t.Fatalf("compact payload len = %d, want less than 1200", len(payload))
	}
	if err := WriteFile(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	gotChildren := got.Records[0].Versions[0].Children
	if len(gotChildren) != len(children) {
		t.Fatalf("child count = %d, want %d", len(gotChildren), len(children))
	}
	for i, child := range gotChildren {
		if child.ID != strconv.Itoa(i) || child.Dim != 128 || child.Text != "" || len(child.Metadata) != 0 {
			t.Fatalf("child[%d] identity fields = %+v", i, child)
		}
		if child.Quantized == nil {
			t.Fatalf("child[%d] missing quantized payload", i)
		}
		wantChild := children[i].Quantized
		if string(child.Quantized.MSE) != string(wantChild.MSE) || string(child.Quantized.Signs) != string(wantChild.Signs) || child.Quantized.ResNorm != wantChild.ResNorm {
			t.Fatalf("child[%d] quantized = %+v, want %+v", i, child.Quantized, wantChild)
		}
	}
}

func TestSnapshotV6RawHashAndSparse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot-00000000000000000007.csdb")
	rawHash := make([]byte, 32)
	for i := range rawHash {
		rawHash[i] = byte(i)
	}
	in := Data{
		Collection: "c", BitWidth: 2, Seed: 5, Dim: 4, RawStore: true, SparseEnabled: true,
		MaxLamport: 7, CreatedAt: time.Unix(0, 0).UTC(),
		Records: []Record{{ID: "a", Versions: []Version{{
			Quantized:    &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 1.0},
			RawHash:      rawHash,
			Sparse:       &SparseBlock{Indices: []uint32{2, 9}, Values: []float32{0.5, 0.1}},
			LamportClock: 7, ActorID: "x",
		}}}},
	}
	if err := WriteFile(path, in); err != nil {
		t.Fatal(err)
	}
	out, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !out.RawStore || !out.SparseEnabled || out.Dim != 4 {
		t.Fatalf("header mismatch: %+v", out)
	}
	v := out.Records[0].Versions[0]
	if len(v.RawHash) != 32 || v.RawHash[5] != 5 {
		t.Fatalf("rawhash mismatch: %x", v.RawHash)
	}
	if v.Sparse == nil || len(v.Sparse.Indices) != 2 || v.Sparse.Values[0] != 0.5 {
		t.Fatalf("sparse mismatch: %+v", v.Sparse)
	}
}

func TestSnapshotFloorGuardRejectsV5(t *testing.T) {
	// magic + version=5
	bad := []byte{0x42, 0x44, 0x53, 0x43, 0x05}
	_, err := read(bytes.NewReader(bad))
	if !errors.Is(err, ErrFormatTooOld) {
		t.Fatalf("want ErrFormatTooOld, got %v", err)
	}
}

// TestSnapshotV6LegacyLoadsNormDefaultOne proves the true-MIPS migration's
// backward-compat contract: a v6 payload (predates QuantizedVector.Norm)
// loads with Norm defaulted to 1 (unit-space/cosine semantics), leaving
// ResNorm/MSE/Signs untouched. It simulates a v6 payload by marshaling a
// current (v7) snapshot, splicing out the 4-byte Norm field that immediately
// follows ResNorm in the v7 wire layout, downgrading the version byte, and
// recomputing the CRC trailer over the shortened body.
func TestSnapshotV6LegacyLoadsNormDefaultOne(t *testing.T) {
	in := Data{
		Collection: "c", BitWidth: 2, Seed: 5, Dim: 4,
		MaxLamport: 1, CreatedAt: time.Unix(0, 0).UTC(),
		Records: []Record{{ID: "a", Versions: []Version{{
			Quantized:    &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 0.75, Norm: 3.5},
			LamportClock: 1, ActorID: "x",
		}}}},
	}
	data, err := marshal(in)
	if err != nil {
		t.Fatal(err)
	}

	// Locate the 4-byte ResNorm bit pattern; the v7 layout writes Norm
	// immediately after it.
	var resNormBytes [4]byte
	binary.LittleEndian.PutUint32(resNormBytes[:], math.Float32bits(0.75))
	idx := bytes.Index(data, resNormBytes[:])
	if idx < 0 {
		t.Fatal("could not locate ResNorm bit pattern in the v7-encoded snapshot")
	}
	var normBytes [4]byte
	binary.LittleEndian.PutUint32(normBytes[:], math.Float32bits(3.5))
	if !bytes.Equal(data[idx+4:idx+8], normBytes[:]) {
		t.Fatal("Norm bytes are not immediately after ResNorm bytes as the v7 layout requires")
	}

	// Splice out the Norm field and downgrade the version byte (offset 4,
	// after the 4-byte magic) to simulate a genuine v6 payload.
	legacy := append([]byte{}, data[:idx+4]...)
	legacy = append(legacy, data[idx+8:]...)
	legacy[4] = snapshotMinVersion
	body := legacy[:len(legacy)-4]
	binary.LittleEndian.PutUint32(legacy[len(legacy)-4:], crc32.ChecksumIEEE(body))

	got, err := read(bytes.NewReader(legacy))
	if err != nil {
		t.Fatalf("read(legacy v6): %v", err)
	}
	if len(got.Records) != 1 || len(got.Records[0].Versions) != 1 || got.Records[0].Versions[0].Quantized == nil {
		t.Fatalf("legacy v6 load: unexpected shape: %+v", got)
	}
	qv := got.Records[0].Versions[0].Quantized
	if qv.Norm != 1 {
		t.Fatalf("legacy v6 load: Norm = %v, want 1 (unit-space default)", qv.Norm)
	}
	if qv.ResNorm != 0.75 {
		t.Fatalf("legacy v6 load: ResNorm = %v, want 0.75 (unaffected by the splice)", qv.ResNorm)
	}
}

// TestReadRejectsInvalidNorm proves m1's decode-time validation: a
// NaN/Inf/negative Norm is rejected, matching turboquant's own wire codec
// (wire.go:144). A negative norm silently inverts ScoreUpperBound into a
// lower bound and drops real top-k members, so untrusted/corrupt input must
// never decode into a QuantizedVector carrying one. Covers the top-level
// Quantized field, a legacy-encoded child's Quantized field, and the
// compact-ordinal column encoding.
func TestReadRejectsInvalidNorm(t *testing.T) {
	invalidNorms := []struct {
		name string
		norm float32
	}{
		{"NaN", float32(math.NaN())},
		{"+Inf", float32(math.Inf(1))},
		{"-Inf", float32(math.Inf(-1))},
		{"negative", -1.0},
	}
	for _, tc := range invalidNorms {
		t.Run("top-level/"+tc.name, func(t *testing.T) {
			in := Data{
				Collection: "c", BitWidth: 2, Seed: 1, Dim: 4, CreatedAt: time.Unix(0, 0).UTC(),
				Records: []Record{{ID: "a", Versions: []Version{{
					Quantized:    &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 0.5, Norm: tc.norm},
					LamportClock: 1, ActorID: "x",
				}}}},
			}
			data, err := marshal(in)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if _, err := read(bytes.NewReader(data)); err == nil {
				t.Fatalf("read(norm=%v): want error, got nil", tc.norm)
			}
		})
		t.Run("legacy-child/"+tc.name, func(t *testing.T) {
			in := Data{
				Collection: "c", BitWidth: 2, Seed: 1, Dim: 4, CreatedAt: time.Unix(0, 0).UTC(),
				Records: []Record{{ID: "parent", Versions: []Version{{
					Children: []ChildVector{
						{ID: "c0", Quantized: &QuantizedVector{MSE: []byte{1, 2}, Signs: []byte{3}, ResNorm: 0.5, Norm: tc.norm}, Dim: 4, Text: "t"},
					},
					LamportClock: 1, ActorID: "x",
				}}}},
			}
			data, err := marshal(in)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if _, err := read(bytes.NewReader(data)); err == nil {
				t.Fatalf("read(legacy-child norm=%v): want error, got nil", tc.norm)
			}
		})
		t.Run("compact-ordinal/"+tc.name, func(t *testing.T) {
			in := Data{
				Collection: "c", BitWidth: 2, Seed: 1, Dim: 4, CreatedAt: time.Unix(0, 0).UTC(),
				Records: []Record{{ID: "parent", Versions: []Version{{
					Children: []ChildVector{
						{ID: "0", Quantized: &QuantizedVector{MSE: []byte{1}, Signs: []byte{2}, ResNorm: 0.5, Norm: tc.norm}, Dim: 4},
						{ID: "1", Quantized: &QuantizedVector{MSE: []byte{3}, Signs: []byte{4}, ResNorm: 0.5, Norm: 1}, Dim: 4},
					},
					LamportClock: 1, ActorID: "x",
				}}}},
			}
			data, err := marshal(in)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if _, err := read(bytes.NewReader(data)); err == nil {
				t.Fatalf("read(compact-ordinal norm=%v): want error, got nil", tc.norm)
			}
		})
	}
}
