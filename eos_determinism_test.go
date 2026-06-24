package corkscrewdb

// TestEosDeterminismGate is the §5.6 RELEASE GATE.
//
// It does NOT decide eosProvider.Deterministic() — that is a decided `true`
// (see eos_provider.go). It VERIFIES the parity assumption that backs the
// correct-by-construction guarantee: if recompute-write and recompute-rerank
// both call EncodeBatch([text])[0], they produce identical raw vectors on the
// same host with the same binary, so the drift-check MATCHes by construction.
//
// Three comparisons are made for each representative text t:
//
//	(1) Encode(t)            — the non-recompute single-text path
//	(2) EncodeBatch([t])[0]  — the recompute path (single-element batch, no padding)
//	(3) t inside a MIXED length-ragged batch — exercises masked ragged-group
//	                           padding (Eos trigger (ii))
//
// GATE RULE:
//   - (1) != (2): INFORMATIONAL only. Non-recompute writes use Encode; recompute
//     uses EncodeBatch. A split here does not break recompute because both write
//     and rerank use EncodeBatch. The test logs it but does NOT fail.
//   - (2) != (3): HARD FAIL. A ragged-batch padding effect would break a
//     multi-text rerank batch (§7) — this would undermine the correct-by-
//     construction guarantee. If this ever fires, investigation is required.
//
// NOTE: a uniform-length batch does NOT exercise Eos trigger (ii). The mixed
// ragged batch below intentionally includes short and long texts.

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestEosDeterminismGate(t *testing.T) {
	// Load the default provider. If it fell back to the builtin, skip.
	raw := newDefaultProvider()
	ep, ok := raw.(*eosProvider)
	if !ok {
		t.Skip("embedded Eos provider not available (builtin fallback active); skipping determinism gate")
	}
	defer ep.Close()

	// STEP 0: Verify the artifact SHA256 pins have not drifted from default_provider.go:6-7.
	t.Run("artifact_sha_pins", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			path string
			want string
		}{
			{"artifact", "assets/corkscrewdb-default-embedder/corkscrewdb-default-embedder.mll", defaultEosProviderArtifactSHA256},
			{"tokenizer", "assets/corkscrewdb-default-embedder/corkscrewdb-default-embedder.tokenizer.mll", defaultEosProviderTokenizerSHA256},
		} {
			data, err := defaultEosProviderAssets.ReadFile(tc.path)
			if err != nil {
				t.Fatalf("read embedded %s: %v", tc.name, err)
			}
			sum := sha256.Sum256(data)
			if got := hex.EncodeToString(sum[:]); got != tc.want {
				t.Fatalf("%s SHA256 changed: got %s, pinned %s — update default_provider.go pins and re-run this gate", tc.name, got, tc.want)
			}
		}
	})

	// STEP 1: Deterministic() must return true for the eosProvider.
	t.Run("Deterministic_returns_true", func(t *testing.T) {
		if !ep.Deterministic() {
			t.Error("eosProvider.Deterministic() = false, want true")
		}
	})

	// STEP 2: Parity gate on representative texts.
	//
	// The text set MUST include ragged-length inputs to exercise Eos trigger (ii).
	// A short text, a long text, and a medium text ensure the ragged batch
	// (comparison 3) has mixed padding groups.
	texts := []string{
		"ok",
		"hello world",
		"the quick brown fox jumps over the lazy dog and then some more words to make this a longer sentence",
	}

	// Build a ragged mixed-length batch containing all texts interleaved with
	// contrasting-length context texts. Index offsets[i] = position of texts[i].
	raggedBatch := make([]string, 0, len(texts)*2)
	offsets := make([]int, len(texts))
	for i, txt := range texts {
		offsets[i] = len(raggedBatch)
		raggedBatch = append(raggedBatch, txt)
		// Insert a contrasting-length context neighbour to force ragged padding.
		if len(txt) < 20 {
			raggedBatch = append(raggedBatch,
				"a much longer context sentence that exercises the ragged-batch masked padding path in eos embed batch")
		} else {
			raggedBatch = append(raggedBatch, "short")
		}
	}

	// Encode the full ragged batch once.
	raggedOut, err := ep.EncodeBatch(raggedBatch)
	if err != nil {
		t.Fatalf("EncodeBatch (ragged): %v", err)
	}

	singleVseBatchMismatch := 0

	for _, txt := range texts {
		txt := txt
		t.Run("parity/"+txt[:min(len(txt), 20)], func(t *testing.T) {
			// (1) single Encode
			vec1, err := ep.Encode(txt)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			// (2) single-element EncodeBatch
			batch2, err := ep.EncodeBatch([]string{txt})
			if err != nil {
				t.Fatalf("EncodeBatch([txt]): %v", err)
			}
			if len(batch2) != 1 {
				t.Fatalf("EncodeBatch returned %d rows, want 1", len(batch2))
			}
			vec2 := batch2[0]

			// (3) same text extracted from the mixed ragged batch
			idx := -1
			for i, bt := range texts {
				if bt == txt {
					idx = offsets[i]
					break
				}
			}
			if idx < 0 || idx >= len(raggedOut) {
				t.Fatalf("could not locate %q in ragged batch output", txt)
			}
			vec3 := raggedOut[idx]

			if len(vec1) != len(vec2) || len(vec2) != len(vec3) {
				t.Fatalf("dim mismatch: Encode=%d EncodeBatch=%d ragged=%d", len(vec1), len(vec2), len(vec3))
			}

			// Check (1) vs (2): informational only.
			match12 := float32EqualSlice(vec1, vec2)
			if !match12 {
				singleVseBatchMismatch++
				t.Logf("INFORMATIONAL: Encode(t) != EncodeBatch([t])[0] for %q — "+
					"non-recompute writes use Encode; recompute uses EncodeBatch. "+
					"This does NOT break recompute (write+rerank both use EncodeBatch).", txt)
			}

			// Check (2) vs (3): HARD FAIL — ragged-batch padding must not alter per-text output.
			if !float32EqualSlice(vec2, vec3) {
				t.Errorf("GATE FAILURE: EncodeBatch([t])[0] != t-in-ragged-batch for %q — "+
					"ragged-batch padding alters embeddings; this breaks the correct-by-construction "+
					"guarantee. Investigate Eos EmbedBatch masking.", txt)
			}
		})
	}

	// Log a summary if (1)!=(2) for any text.
	if singleVseBatchMismatch > 0 {
		t.Logf("SUMMARY: single-vs-batch mismatch on %d/%d texts. "+
			"This is informational — recompute write and rerank both use EncodeBatch, "+
			"so they still agree. The drift-check protects correctness.", singleVseBatchMismatch, len(texts))
	}
}

// float32EqualSlice reports whether two float32 slices are bit-for-bit equal.
func float32EqualSlice(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
