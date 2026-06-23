package corkscrewdb

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// deterministicMockProvider is a mockProvider that also satisfies
// deterministicProvider (Deterministic() returns true).
type deterministicMockProvider struct {
	*mockProvider
}

func (d *deterministicMockProvider) Deterministic() bool { return true }

// nonDeterministicMockProvider satisfies deterministicProvider but returns false.
type nonDeterministicMockProvider struct {
	*mockProvider
}

func (n *nonDeterministicMockProvider) Deterministic() bool { return false }

// TestRecomputeManifestRoundTrip verifies that:
//   - a recompute collection persists cold_tier="recompute", raw_store=false,
//     rerank_depth in the manifest JSON.
//   - reopening restores coldTier==coldRecompute and rerankDepth==8.
//   - a v0.3.x-style manifest (no cold_tier field) with raw_store:true loads as
//     coldRaw; raw_store:false loads as coldNone.
func TestRecomputeManifestRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "recompute.csdb")
	provider := &deterministicMockProvider{&mockProvider{dim: 8}}

	// Create the collection with recompute mode.
	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText(), WithRerank(8))
	if c.err != nil {
		t.Fatalf("create failed: %v", c.err)
	}
	if c.coldTier != coldRecompute {
		t.Fatalf("coldTier = %v, want coldRecompute", c.coldTier)
	}
	if c.rerankDepth != 8 {
		t.Fatalf("rerankDepth = %d, want 8", c.rerankDepth)
	}
	// Put an entry (text drives the embedding; no raw store).
	if err := c.Put("k1", Entry{Text: "hello world"}); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify the raw manifest JSON has the expected fields.
	manifestPath := filepath.Join(dir, "manifest.json")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Collections map[string]struct {
			ColdTier    string `json:"cold_tier"`
			RawStore    bool   `json:"raw_store"`
			RerankDepth int    `json:"rerank_depth"`
		} `json:"collections"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("manifest parse: %v", err)
	}
	got := parsed.Collections["r"]
	if got.ColdTier != "recompute" {
		t.Errorf("manifest cold_tier = %q, want %q", got.ColdTier, "recompute")
	}
	if got.RawStore != false {
		t.Errorf("manifest raw_store = %v, want false", got.RawStore)
	}
	if got.RerankDepth != 8 {
		t.Errorf("manifest rerank_depth = %d, want 8", got.RerankDepth)
	}

	// Reopen and verify in-memory state.
	db2, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	c2 := db2.Collection("r")
	if c2.err != nil {
		t.Fatalf("reopen collection: %v", c2.err)
	}
	if c2.coldTier != coldRecompute {
		t.Errorf("reopened coldTier = %v, want coldRecompute", c2.coldTier)
	}
	if c2.rerankDepth != 8 {
		t.Errorf("reopened rerankDepth = %d, want 8", c2.rerankDepth)
	}

	// Sub-case: v0.3.x-style manifest (no cold_tier, raw_store:true) → coldRaw.
	t.Run("v03x_raw_true", func(t *testing.T) {
		dir2 := t.TempDir()
		db3, err := Open(filepath.Join(dir2, "v03raw.csdb"), WithProvider(provider))
		if err != nil {
			t.Fatal(err)
		}
		c3 := db3.Collection("legacy", WithBitWidth(2))
		if c3.err != nil {
			t.Fatal(c3.err)
		}
		if err := c3.Put("k", Entry{Text: "test"}); err != nil {
			t.Fatal(err)
		}
		if err := db3.Close(); err != nil {
			t.Fatal(err)
		}
		// Strip the cold_tier field from the manifest to simulate v0.3.x.
		mpath := filepath.Join(dir2, "v03raw.csdb", "manifest.json")
		mraw, _ := os.ReadFile(mpath)
		var m map[string]interface{}
		json.Unmarshal(mraw, &m)
		if colls, ok := m["collections"].(map[string]interface{}); ok {
			if coll, ok := colls["legacy"].(map[string]interface{}); ok {
				delete(coll, "cold_tier")
				delete(coll, "rerank_depth")
				// raw_store must be true (already the default).
			}
		}
		b, _ := json.Marshal(m)
		os.WriteFile(mpath, b, 0o644)

		db4, err := Open(filepath.Join(dir2, "v03raw.csdb"), WithProvider(provider))
		if err != nil {
			t.Fatal(err)
		}
		defer db4.Close()
		c4 := db4.Collection("legacy")
		if c4.err != nil {
			t.Fatal(c4.err)
		}
		if c4.coldTier != coldRaw {
			t.Errorf("v0.3.x raw_store:true → coldTier = %v, want coldRaw", c4.coldTier)
		}
		if c4.rerankDepth != 0 {
			t.Errorf("v0.3.x rerankDepth = %d, want 0", c4.rerankDepth)
		}
	})

	// Sub-case: v0.3.x-style manifest (no cold_tier, raw_store:false) → coldNone.
	t.Run("v03x_raw_false", func(t *testing.T) {
		dir2 := t.TempDir()
		db3, err := Open(filepath.Join(dir2, "v03none.csdb"), WithProvider(provider))
		if err != nil {
			t.Fatal(err)
		}
		c3 := db3.Collection("leg2", WithBitWidth(2), WithoutRawStore())
		if c3.err != nil {
			t.Fatal(c3.err)
		}
		if err := c3.Put("k", Entry{Text: "test"}); err != nil {
			t.Fatal(err)
		}
		if err := db3.Close(); err != nil {
			t.Fatal(err)
		}
		// Strip cold_tier from manifest.
		mpath := filepath.Join(dir2, "v03none.csdb", "manifest.json")
		mraw, _ := os.ReadFile(mpath)
		var m map[string]interface{}
		json.Unmarshal(mraw, &m)
		if colls, ok := m["collections"].(map[string]interface{}); ok {
			if coll, ok := colls["leg2"].(map[string]interface{}); ok {
				delete(coll, "cold_tier")
			}
		}
		b, _ := json.Marshal(m)
		os.WriteFile(mpath, b, 0o644)

		db4, err := Open(filepath.Join(dir2, "v03none.csdb"), WithProvider(provider))
		if err != nil {
			t.Fatal(err)
		}
		defer db4.Close()
		c4 := db4.Collection("leg2")
		if c4.err != nil {
			t.Fatal(c4.err)
		}
		if c4.coldTier != coldNone {
			t.Errorf("v0.3.x raw_store:false → coldTier = %v, want coldNone", c4.coldTier)
		}
	})
}

// TestRecomputeMutualExclusion verifies that combining WithRecomputeRawFromText
// and WithoutRawStore produces a config error regardless of option order.
func TestRecomputeMutualExclusion(t *testing.T) {
	provider := &deterministicMockProvider{&mockProvider{dim: 8}}

	cases := []struct {
		name string
		opts []CollectionOption
	}{
		{
			name: "recompute_then_without_raw",
			opts: []CollectionOption{WithRecomputeRawFromText(), WithoutRawStore()},
		},
		{
			name: "without_raw_then_recompute",
			opts: []CollectionOption{WithoutRawStore(), WithRecomputeRawFromText()},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "mutex.csdb")
			db, err := Open(dir, WithProvider(provider))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			opts := append([]CollectionOption{WithBitWidth(2)}, tc.opts...)
			c := db.Collection("x", opts...)
			if c.err == nil {
				t.Fatal("expected error for WithRecomputeRawFromText+WithoutRawStore, got nil")
			}
			if !strings.Contains(c.err.Error(), "mutually exclusive") {
				t.Fatalf("err = %q does not mention mutual exclusion", c.err)
			}
		})
	}
}

// TestRecomputeRequiresDeterministicProvider verifies that a non-deterministic
// provider causes WithRecomputeRawFromText create to fail, while an explicit
// deterministic provider succeeds.
func TestRecomputeRequiresDeterministicProvider(t *testing.T) {
	t.Run("nondeterministic provider rejected", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nondeter.csdb")
		nonDeter := &nonDeterministicMockProvider{&mockProvider{dim: 8}}
		db, err := Open(dir, WithProvider(nonDeter))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText())
		if c.err == nil {
			t.Fatal("expected ErrRecomputeRequiresDeterministicProvider, got nil")
		}
		if !errors.Is(c.err, ErrRecomputeRequiresDeterministicProvider) {
			t.Fatalf("err = %v, want ErrRecomputeRequiresDeterministicProvider", c.err)
		}
	})

	t.Run("deterministic provider accepted", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "deter.csdb")
		deter := &deterministicMockProvider{&mockProvider{dim: 8}}
		db, err := Open(dir, WithProvider(deter))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText())
		if c.err != nil {
			t.Fatalf("unexpected error with deterministic provider: %v", c.err)
		}
	})
}

// TestRecomputeRejectsBuiltinFallback verifies Resolution 4 (intended-provider
// gate): the silent builtin fallback is rejected for recompute collections, but
// an explicit WithProvider(newBuiltinProvider()) is accepted.
func TestRecomputeRejectsBuiltinFallback(t *testing.T) {
	t.Run("default path with builtin fallback rejected", func(t *testing.T) {
		// Simulate the silent-builtin-fallback scenario in-package: open a DB with
		// the builtin provider (via WithProvider so Open succeeds) but then clear
		// providerSet to simulate the "no explicit WithProvider was given" path.
		// This tests the gate regardless of whether Eos assets are installed.
		dir := filepath.Join(t.TempDir(), "fallback.csdb")
		db, err := Open(dir, WithProvider(newBuiltinProvider()))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		// Simulate fallback: the user did NOT call WithProvider; the builtin was
		// chosen implicitly by newDefaultProvider().
		db.providerSet = false

		c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText())
		if c.err == nil {
			t.Fatal("expected rejection of recompute on silent builtin fallback, got nil")
		}
		if !errors.Is(c.err, ErrRecomputeRequiresDeterministicProvider) {
			t.Fatalf("err = %v, want ErrRecomputeRequiresDeterministicProvider", c.err)
		}
	})

	t.Run("explicit WithProvider(builtin) accepted", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "explicit_builtin.csdb")
		// Explicitly provide the builtin — this is a deliberate choice, must succeed.
		db, err := Open(dir, WithProvider(newBuiltinProvider()))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText())
		if c.err != nil {
			t.Fatalf("explicit WithProvider(builtin) should be accepted: %v", c.err)
		}
	})
}
