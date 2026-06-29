package corkscrewdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const legacy256DMigrationPolicySmokeSchema = "eos.embedder1_legacy_256d_migration_policy_smoke.v1"

type legacy256DMigrationPolicySmokeEvidence struct {
	Schema                  string `json:"schema"`
	Legacy256DOpenPassed    bool   `json:"legacy_256d_open_passed"`
	LegacyProviderAvailable bool   `json:"legacy_provider_available"`
	MismatchRejectsClearly  bool   `json:"mismatch_rejects_clearly"`
	InPlaceUpgradeSupported bool   `json:"in_place_upgrade_supported"`
	ReembedRebuildRequired  bool   `json:"reembed_rebuild_required"`
	TempDBPath              string `json:"temp_db_path,omitempty"`
	MismatchOperation       string `json:"mismatch_operation,omitempty"`
	ObservedMismatchError   string `json:"observed_mismatch_error,omitempty"`
	LegacyProviderID        string `json:"legacy_provider_id,omitempty"`
	LegacyDim               int    `json:"legacy_dim,omitempty"`
	CandidateProviderID     string `json:"candidate_provider_id,omitempty"`
	CandidateDim            int    `json:"candidate_dim,omitempty"`
	GeneratedAtUTC          string `json:"generated_at_utc,omitempty"`
}

func TestLegacy256DMigrationPolicySmokeEvidence(t *testing.T) {
	packagePath := strings.TrimSpace(os.Getenv("CORKSCREWDB_IMPORTED_BGE_CANDIDATE_PACKAGE"))
	evidencePath := strings.TrimSpace(os.Getenv("CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_EVIDENCE"))
	reportPath := strings.TrimSpace(os.Getenv("CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_REPORT"))
	dbPath := strings.TrimSpace(os.Getenv("CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_DB"))
	if packagePath == "" || evidencePath == "" || reportPath == "" || dbPath == "" {
		t.Skip("set CORKSCREWDB_IMPORTED_BGE_CANDIDATE_PACKAGE, CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_EVIDENCE, CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_REPORT, and CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_DB to run")
	}
	if err := os.RemoveAll(dbPath); err != nil {
		t.Fatalf("reset smoke db path: %v", err)
	}

	legacyDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("create legacy default DB: %v", err)
	}
	if legacyDB.manifest.Embedding.ID != defaultEosProviderID {
		t.Fatalf("legacy provider id = %q, want %q", legacyDB.manifest.Embedding.ID, defaultEosProviderID)
	}
	if legacyDB.manifest.Embedding.Dim != 256 {
		t.Fatalf("legacy dim = %d, want 256", legacyDB.manifest.Embedding.Dim)
	}
	collection := legacyDB.Collection("legacy_docs", WithRecomputeRawFromText())
	if collection.err != nil {
		t.Fatalf("create legacy recompute collection: %v", collection.err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("close legacy DB: %v", err)
	}

	reopenedLegacyDB, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen legacy 256d DB with default provider: %v", err)
	}
	if reopenedLegacyDB.manifest.Embedding.ID != defaultEosProviderID || reopenedLegacyDB.manifest.Embedding.Dim != 256 {
		t.Fatalf("reopened legacy embedding = %s/%d, want %s/256", reopenedLegacyDB.manifest.Embedding.ID, reopenedLegacyDB.manifest.Embedding.Dim, defaultEosProviderID)
	}
	if err := reopenedLegacyDB.Close(); err != nil {
		t.Fatalf("close reopened legacy DB: %v", err)
	}

	candidateProvider, err := LoadImportedBGECandidateProvider(packagePath)
	if err != nil {
		t.Fatalf("load imported BGE candidate provider: %v", err)
	}
	defer candidateProvider.Close()
	candidateID := ""
	if named, ok := candidateProvider.(providerIdentifier); ok {
		candidateID = named.ProviderID()
	}
	if candidateID != ImportedBGECandidateProviderID {
		t.Fatalf("candidate provider id = %q, want %q", candidateID, ImportedBGECandidateProviderID)
	}
	if candidateProvider.Dim() != 384 {
		t.Fatalf("candidate provider dim = %d, want 384", candidateProvider.Dim())
	}

	_, mismatchErr := Open(dbPath, WithProvider(candidateProvider))
	if mismatchErr == nil {
		t.Fatal("expected legacy 256d DB plus BGE 384d provider to fail")
	}
	mismatchRejectsClearly := strings.Contains(mismatchErr.Error(), "embedding config mismatch") &&
		strings.Contains(mismatchErr.Error(), defaultEosProviderID+"/256") &&
		strings.Contains(mismatchErr.Error(), ImportedBGECandidateProviderID+"/384")
	if !mismatchRejectsClearly {
		t.Fatalf("mismatch error = %q, want clear provider/dim mismatch", mismatchErr.Error())
	}
	if errors.Is(mismatchErr, ErrRecomputeBackendMismatch) {
		t.Fatalf("mismatch error = %v, want provider/dim mismatch before backend-fingerprint mismatch", mismatchErr)
	}

	evidence := legacy256DMigrationPolicySmokeEvidence{
		Schema:                  legacy256DMigrationPolicySmokeSchema,
		Legacy256DOpenPassed:    true,
		LegacyProviderAvailable: true,
		MismatchRejectsClearly:  true,
		InPlaceUpgradeSupported: false,
		ReembedRebuildRequired:  true,
		TempDBPath:              dbPath,
		MismatchOperation:       "Open(legacy 256d DB, WithProvider(imported BGE candidate 384d provider))",
		ObservedMismatchError:   mismatchErr.Error(),
		LegacyProviderID:        defaultEosProviderID,
		LegacyDim:               256,
		CandidateProviderID:     ImportedBGECandidateProviderID,
		CandidateDim:            384,
		GeneratedAtUTC:          time.Now().UTC().Format(time.RFC3339),
	}
	writeLegacy256DSmokeEvidence(t, evidencePath, evidence)
	writeLegacy256DSmokeReport(t, reportPath, packagePath, evidence)
}

func writeLegacy256DSmokeEvidence(t *testing.T, path string, evidence legacy256DMigrationPolicySmokeEvidence) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create evidence dir: %v", err)
	}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("marshal evidence: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write evidence: %v", err)
	}
}

func writeLegacy256DSmokeReport(t *testing.T, path, packagePath string, evidence legacy256DMigrationPolicySmokeEvidence) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create report dir: %v", err)
	}
	report := fmt.Sprintf(`# eos-embedder1-legacy-256d-migration-smoke-v1

## Outcome

Pass. A CorkScrewDB database created with the current default provider persisted %s/%d, reopened with the same legacy default provider, and rejected the imported BGE candidate provider at %s/%d with a clear embedding config mismatch.

## Fixture

- Temporary DB fixture path: %s
- Selected BGE package: %s
- Legacy collection: legacy_docs with recompute raw-from-text enabled

## Smoke Command

GOWORK=off CORKSCREWDB_IMPORTED_BGE_CANDIDATE_PACKAGE=%s CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_EVIDENCE=%s CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_REPORT=%s CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_DB=%s go test . -run TestLegacy256DMigrationPolicySmokeEvidence -count=1 -v

## Mismatch Operation

%s

Observed error:

%s

## Policy Evidence

- legacy_256d_open_passed: %t
- legacy_provider_available: %t
- mismatch_rejects_clearly: %t
- in_place_upgrade_supported: %t
- reembed_rebuild_required: %t
`,
		evidence.LegacyProviderID,
		evidence.LegacyDim,
		evidence.CandidateProviderID,
		evidence.CandidateDim,
		evidence.TempDBPath,
		packagePath,
		packagePath,
		os.Getenv("CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_EVIDENCE"),
		os.Getenv("CORKSCREWDB_LEGACY_256D_MIGRATION_SMOKE_REPORT"),
		evidence.TempDBPath,
		evidence.MismatchOperation,
		evidence.ObservedMismatchError,
		evidence.Legacy256DOpenPassed,
		evidence.LegacyProviderAvailable,
		evidence.MismatchRejectsClearly,
		evidence.InPlaceUpgradeSupported,
		evidence.ReembedRebuildRequired,
	)
	if err := os.WriteFile(path, []byte(report), 0o644); err != nil {
		t.Fatalf("write report: %v", err)
	}
}
