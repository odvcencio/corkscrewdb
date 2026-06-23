package corkscrewdb

import "testing"

// capableMockProvider embeds mockProvider and additionally satisfies both
// deterministicProvider and backendFingerprintProvider. Used to verify that
// the probe helpers reach through enc.provider, NOT the *encoder shim.
type capableMockProvider struct {
	*mockProvider
}

func (c *capableMockProvider) Deterministic() bool        { return true }
func (c *capableMockProvider) BackendFingerprint() string { return "fp" }

// TestCapabilityProbeReachesThroughEncoder is a regression for Fix 5: the
// probe helpers must unwrap enc.provider, not probe *encoder itself.
func TestCapabilityProbeReachesThroughEncoder(t *testing.T) {
	t.Run("capable provider returns true+fp", func(t *testing.T) {
		enc := newEncoder(&capableMockProvider{&mockProvider{dim: 4}})
		if got := providerDeterministic(enc); !got {
			t.Errorf("providerDeterministic = %v, want true", got)
		}
		if got := providerBackendFingerprint(enc); got != "fp" {
			t.Errorf("providerBackendFingerprint = %q, want %q", got, "fp")
		}
	})

	t.Run("incapable provider returns false+empty", func(t *testing.T) {
		enc := newEncoder(&mockProvider{dim: 4})
		if got := providerDeterministic(enc); got {
			t.Errorf("providerDeterministic = %v, want false", got)
		}
		if got := providerBackendFingerprint(enc); got != "" {
			t.Errorf("providerBackendFingerprint = %q, want %q", got, "")
		}
	})
}
