package corkscrewdb

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// fingerprintMockProvider is a deterministicMockProvider that also implements
// backendFingerprintProvider, allowing tests to control the fingerprint string.
type fingerprintMockProvider struct {
	*mockProvider
	fingerprint string
}

func (f *fingerprintMockProvider) Deterministic() bool        { return true }
func (f *fingerprintMockProvider) BackendFingerprint() string { return f.fingerprint }

// TestRecomputeBackendMismatchFailsOpen verifies §9.4: reopening a recompute
// collection with a provider whose BackendFingerprint differs from the persisted
// one returns ErrRecomputeBackendMismatch; same fingerprint succeeds.
func TestRecomputeBackendMismatchFailsOpen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "mismatch.csdb")
	providerA := &fingerprintMockProvider{&mockProvider{dim: 8}, "backend:aaa:zzz"}

	// Create with provider A.
	db, err := Open(dir, WithProvider(providerA))
	if err != nil {
		t.Fatal(err)
	}
	c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText())
	if c.err != nil {
		t.Fatalf("create failed: %v", c.err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with provider B (same ProviderID "mock-provider", same Dim 8, but different fingerprint).
	providerB := &fingerprintMockProvider{&mockProvider{dim: 8}, "backend:bbb:yyy"}
	_, err = Open(dir, WithProvider(providerB))
	if err == nil {
		t.Fatal("expected ErrRecomputeBackendMismatch, got nil")
	}
	if !errors.Is(err, ErrRecomputeBackendMismatch) {
		t.Fatalf("err = %v, want ErrRecomputeBackendMismatch", err)
	}

	// Reopen with provider A again — should succeed.
	db2, err := Open(dir, WithProvider(providerA))
	if err != nil {
		t.Fatalf("reopen with matching fingerprint failed: %v", err)
	}
	db2.Close()
}

// TestRawNoneIgnoreFingerprintDiff verifies back-compat: raw and none collections
// do NOT enforce the fingerprint; a v0.3.x manifest with no stored fingerprint
// also never triggers a mismatch.
func TestRawNoneIgnoreFingerprintDiff(t *testing.T) {
	t.Run("raw collection ignores fingerprint diff", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "raw.csdb")
		providerA := &fingerprintMockProvider{&mockProvider{dim: 8}, "backend:aaa:zzz"}

		db, err := Open(dir, WithProvider(providerA))
		if err != nil {
			t.Fatal(err)
		}
		c := db.Collection("r", WithBitWidth(2)) // raw (default)
		if c.err != nil {
			t.Fatalf("create: %v", c.err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		// Reopen with a different fingerprint — must succeed for a raw collection.
		providerB := &fingerprintMockProvider{&mockProvider{dim: 8}, "backend:bbb:yyy"}
		db2, err := Open(dir, WithProvider(providerB))
		if err != nil {
			t.Fatalf("raw collection reopen with diff fingerprint: %v", err)
		}
		db2.Close()
	})

	t.Run("none collection ignores fingerprint diff", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "none.csdb")
		providerA := &fingerprintMockProvider{&mockProvider{dim: 8}, "backend:aaa:zzz"}

		db, err := Open(dir, WithProvider(providerA))
		if err != nil {
			t.Fatal(err)
		}
		c := db.Collection("n", WithBitWidth(2), WithoutRawStore())
		if c.err != nil {
			t.Fatalf("create: %v", c.err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		// Reopen with a different fingerprint — must succeed for a none collection.
		providerB := &fingerprintMockProvider{&mockProvider{dim: 8}, "backend:bbb:yyy"}
		db2, err := Open(dir, WithProvider(providerB))
		if err != nil {
			t.Fatalf("none collection reopen with diff fingerprint: %v", err)
		}
		db2.Close()
	})

	t.Run("v03x empty stored fingerprint never mismatches", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "v03x.csdb")
		providerA := &fingerprintMockProvider{&mockProvider{dim: 8}, "backend:aaa:zzz"}

		db, err := Open(dir, WithProvider(providerA))
		if err != nil {
			t.Fatal(err)
		}
		c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText())
		if c.err != nil {
			t.Fatalf("create: %v", c.err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		// Simulate a v0.3.x manifest: strip backend_fingerprint from the manifest.
		mpath := filepath.Join(dir, "manifest.json")
		mraw, err := os.ReadFile(mpath)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]interface{}
		if err := json.Unmarshal(mraw, &m); err != nil {
			t.Fatal(err)
		}
		if emb, ok := m["embedding"].(map[string]interface{}); ok {
			delete(emb, "backend_fingerprint")
		}
		b, _ := json.Marshal(m)
		if err := os.WriteFile(mpath, b, 0o644); err != nil {
			t.Fatal(err)
		}

		// Reopen with any fingerprint — empty stored fingerprint must not mismatch.
		providerB := &fingerprintMockProvider{&mockProvider{dim: 8}, "backend:bbb:yyy"}
		db2, err := Open(dir, WithProvider(providerB))
		if err != nil {
			t.Fatalf("v0.3.x empty fingerprint: reopen should succeed, got: %v", err)
		}
		db2.Close()
	})
}

// TestFreshOpenBackfillsFingerprint verifies that opening a fresh DB (no
// prior manifest) with a fingerprint provider persists the fingerprint in
// embedding.backend_fingerprint.
func TestFreshOpenBackfillsFingerprint(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "fresh.csdb")
	provider := &fingerprintMockProvider{&mockProvider{dim: 8}, "backend:abc:def"}

	db, err := Open(dir, WithProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	// Create a recompute collection so the fingerprint matters.
	c := db.Collection("r", WithBitWidth(2), WithRecomputeRawFromText())
	if c.err != nil {
		t.Fatalf("create: %v", c.err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Read the manifest and check that backend_fingerprint is persisted.
	mpath := filepath.Join(dir, "manifest.json")
	mraw, err := os.ReadFile(mpath)
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		Embedding struct {
			BackendFingerprint string `json:"backend_fingerprint"`
		} `json:"embedding"`
	}
	if err := json.Unmarshal(mraw, &parsed); err != nil {
		t.Fatalf("manifest parse: %v", err)
	}
	if parsed.Embedding.BackendFingerprint != "backend:abc:def" {
		t.Errorf("backend_fingerprint = %q, want %q", parsed.Embedding.BackendFingerprint, "backend:abc:def")
	}
}
