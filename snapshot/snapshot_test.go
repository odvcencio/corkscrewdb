package snapshot

import (
	"bytes"
	"encoding/binary"
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
						Embedding:    []float32{0.1, 0.2, 0.3},
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
		Storage:    "quantized_only",
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
		Storage:    "quantized_only",
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
	if got.Storage != "quantized_only" || len(got.Records) != 1 || len(got.Records[0].Versions) != 1 {
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
		Storage:    "quantized_only",
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
		if child.ID != strconv.Itoa(i) || child.Dim != 128 || len(child.Embedding) != 0 || child.Text != "" || len(child.Metadata) != 0 {
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
