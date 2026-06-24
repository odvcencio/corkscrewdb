package corkscrewdb

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestV03DefaultOptionOpensRecomputeSilently verifies §9.3 / Fix 2 / Resolution 6:
//
//  1. A v0.4.0 recompute manifest (cold_tier:"recompute", raw_store:false) opened
//     with DEFAULT collection options — no WithoutRawStore, no
//     WithRecomputeRawFromText — succeeds silently: Open and db.Collection return
//     no error, rawStoreEnabled==false, coldTier==coldRecompute.
//
//  2. SearchVector on codes works (returns nil, nil for the empty fixture collection).
//
//  3. A raw-needing operation (getRaw) returns ErrRawStoreRequired DEFERRED at the
//     call, not at open — proving the error is not surfaced eagerly.
//
//  4. Opening with WithoutRawStore() is likewise transparent (same rawStoreEnabled).
//
//  5. The parsed golden fixture fields (cold_tier, rerank_depth, raw_store) match
//     their expected values — this assertion locks the manifest format so a future
//     struct/JSON rename fails loudly here.
//
// The golden fixture lives at testdata/manifest_recompute_v040.json (top-level,
// outside the gitignored /testdata/ann/ directory) and is a TRACKED file.
func TestV03DefaultOptionOpensRecomputeSilently(t *testing.T) {
	fixturePath := filepath.Join("testdata", "manifest_recompute_v040.json")

	// --- Assert the golden fixture parses to the expected fields (format lock). ---
	t.Run("golden_fixture_fields", func(t *testing.T) {
		data, err := os.ReadFile(fixturePath)
		if err != nil {
			t.Fatalf("read golden fixture: %v", err)
		}
		var parsed struct {
			FormatVersion int    `json:"format_version"`
			FormatFloor   string `json:"format_floor"`
			Embedding     struct {
				ID  string `json:"id"`
				Dim int    `json:"dim"`
			} `json:"embedding"`
			Collections map[string]struct {
				ColdTier    string `json:"cold_tier"`
				RawStore    bool   `json:"raw_store"`
				RerankDepth int    `json:"rerank_depth"`
				BitWidth    int    `json:"bit_width"`
			} `json:"collections"`
		}
		if err := json.Unmarshal(data, &parsed); err != nil {
			t.Fatalf("unmarshal golden fixture: %v", err)
		}
		if parsed.FormatVersion != 2 {
			t.Errorf("format_version = %d, want 2", parsed.FormatVersion)
		}
		if parsed.FormatFloor != "v0.3.0" {
			t.Errorf("format_floor = %q, want %q", parsed.FormatFloor, "v0.3.0")
		}
		coll, ok := parsed.Collections["r"]
		if !ok {
			t.Fatal("golden fixture missing collection \"r\"")
		}
		if coll.ColdTier != "recompute" {
			t.Errorf("cold_tier = %q, want %q", coll.ColdTier, "recompute")
		}
		if coll.RawStore {
			t.Errorf("raw_store = true, want false for recompute collection")
		}
		if coll.RerankDepth != 4 {
			t.Errorf("rerank_depth = %d, want 4", coll.RerankDepth)
		}
		if coll.BitWidth != 2 {
			t.Errorf("bit_width = %d, want 2", coll.BitWidth)
		}
	})

	// setupDB copies the golden manifest into a fresh temp DB directory and
	// returns the directory path. The fixture's embedding ID is "mock-provider"
	// with dim 8, matching &mockProvider{dim: 8}.
	setupDB := func(t *testing.T) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "compat.csdb")
		if err := os.MkdirAll(filepath.Join(dir, "collections"), 0o755); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(fixturePath)
		if err != nil {
			t.Fatalf("read golden fixture: %v", err)
		}
		dest := filepath.Join(dir, "manifest.json")
		if err := os.WriteFile(dest, data, 0o644); err != nil {
			t.Fatalf("write manifest: %v", err)
		}
		return dir
	}

	// --- Main sub-test: default collection options, no v0.4.0 options. ---
	t.Run("default_options_open_succeeds", func(t *testing.T) {
		dir := setupDB(t)
		provider := &mockProvider{dim: 8}

		db, err := Open(dir, WithProvider(provider))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer db.Close()

		// db.Collection with NO collection options must succeed silently.
		c := db.Collection("r")
		if c.err != nil {
			t.Fatalf("Collection: %v", c.err)
		}

		// rawStoreEnabled must be false (recompute collection, no raw store).
		if c.rawStoreEnabled {
			t.Errorf("rawStoreEnabled = true, want false")
		}
		// coldTier must be coldRecompute (loaded from manifest cold_tier field).
		if c.coldTier != coldRecompute {
			t.Errorf("coldTier = %v, want coldRecompute", c.coldTier)
		}
		// rerankDepth must be 4 (loaded from manifest rerank_depth field).
		if c.rerankDepth != 4 {
			t.Errorf("rerankDepth = %d, want 4", c.rerankDepth)
		}

		// SearchVector on an empty collection returns nil, nil — codes-only path works.
		results, err := c.SearchVector(make([]float32, 8), 3)
		if err != nil {
			t.Errorf("SearchVector: %v", err)
		}
		if results != nil {
			t.Errorf("SearchVector on empty collection = %v, want nil", results)
		}

		// getRaw DEFERRED: error must come at the call, NOT at Open or Collection.
		// This proves the v0.3.x client does not see an error until it actually needs
		// the raw store.
		_, rawErr := c.getRaw(make([]byte, 32))
		if !errors.Is(rawErr, ErrRawStoreRequired) {
			t.Errorf("getRaw: err = %v, want errors.Is ErrRawStoreRequired", rawErr)
		}
	})

	// --- Sub-test: WithoutRawStore() open is likewise transparent. ---
	t.Run("without_raw_store_open_succeeds", func(t *testing.T) {
		dir := setupDB(t)
		provider := &mockProvider{dim: 8}

		db, err := Open(dir, WithProvider(provider))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer db.Close()

		// WithoutRawStore disagrees with the existing rawStoreEnabled==false, but
		// rawStore==false is already false so the check is cfg.rawStore != existing.rawStoreEnabled
		// where cfg.rawStore=false and existing.rawStoreEnabled=false → no mismatch.
		c := db.Collection("r", WithoutRawStore())
		if c.err != nil {
			t.Fatalf("Collection with WithoutRawStore: %v", c.err)
		}
		if c.rawStoreEnabled {
			t.Errorf("rawStoreEnabled = true, want false")
		}
	})

	// --- Sub-test: v0.3.x manifest (no cold_tier, raw_store:false) opens as coldNone. ---
	// This is back-compat: a v0.3.x manifest lacking cold_tier must derive the
	// tier from the raw_store field — false ⇒ coldNone, no error.
	t.Run("v03x_no_cold_tier_raw_false_is_coldNone", func(t *testing.T) {
		dir := setupDB(t)
		provider := &mockProvider{dim: 8}

		// Strip cold_tier from the manifest to simulate a v0.3.x manifest.
		mpath := filepath.Join(dir, "manifest.json")
		mraw, err := os.ReadFile(mpath)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]interface{}
		if err := json.Unmarshal(mraw, &m); err != nil {
			t.Fatal(err)
		}
		if colls, ok := m["collections"].(map[string]interface{}); ok {
			if coll, ok := colls["r"].(map[string]interface{}); ok {
				delete(coll, "cold_tier")
				delete(coll, "rerank_depth")
			}
		}
		b, _ := json.Marshal(m)
		if err := os.WriteFile(mpath, b, 0o644); err != nil {
			t.Fatal(err)
		}

		db, err := Open(dir, WithProvider(provider))
		if err != nil {
			t.Fatalf("Open v0.3.x manifest: %v", err)
		}
		defer db.Close()

		c := db.Collection("r")
		if c.err != nil {
			t.Fatalf("Collection: %v", c.err)
		}
		// raw_store=false + no cold_tier ⇒ coldNone (v0.3.x back-compat derive rule).
		if c.coldTier != coldNone {
			t.Errorf("v0.3.x raw_store:false coldTier = %v, want coldNone", c.coldTier)
		}
		if c.rerankDepth != 0 {
			t.Errorf("v0.3.x rerankDepth = %d, want 0", c.rerankDepth)
		}
	})

	// --- Sub-test: v0.3.x manifest (no cold_tier, raw_store:true) opens as coldRaw. ---
	t.Run("v03x_no_cold_tier_raw_true_is_coldRaw", func(t *testing.T) {
		dir := setupDB(t)
		provider := &mockProvider{dim: 8}

		// Modify the manifest: set raw_store:true and remove cold_tier.
		mpath := filepath.Join(dir, "manifest.json")
		mraw, err := os.ReadFile(mpath)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]interface{}
		if err := json.Unmarshal(mraw, &m); err != nil {
			t.Fatal(err)
		}
		if colls, ok := m["collections"].(map[string]interface{}); ok {
			if coll, ok := colls["r"].(map[string]interface{}); ok {
				delete(coll, "cold_tier")
				delete(coll, "rerank_depth")
				coll["raw_store"] = true
			}
		}
		b, _ := json.Marshal(m)
		if err := os.WriteFile(mpath, b, 0o644); err != nil {
			t.Fatal(err)
		}

		db, err := Open(dir, WithProvider(provider))
		if err != nil {
			t.Fatalf("Open v0.3.x raw manifest: %v", err)
		}
		defer db.Close()

		c := db.Collection("r")
		if c.err != nil {
			t.Fatalf("Collection: %v", c.err)
		}
		// raw_store=true + no cold_tier ⇒ coldRaw (v0.3.x back-compat derive rule).
		if c.coldTier != coldRaw {
			t.Errorf("v0.3.x raw_store:true coldTier = %v, want coldRaw", c.coldTier)
		}
	})
}
