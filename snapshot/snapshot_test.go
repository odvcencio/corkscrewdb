package snapshot

import (
	"path/filepath"
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
