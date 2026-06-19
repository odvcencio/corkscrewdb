package corkscrewdb

import "embed"

const defaultEosProviderID = "corkscrewdb-default-embedder"
const defaultEosProviderArtifactSHA256 = "8074d2fce1842e232df2b4172d40463d82b57848c719b2d76fdd68aca682ac70"
const defaultEosProviderTokenizerSHA256 = "64cf63223cb3f97125040677a573e6ab6c625cff1f6f338f4e680a4c9f7a42f5"

//go:embed assets/corkscrewdb-default-embedder/*.mll
var defaultEosProviderAssets embed.FS

func newDefaultProvider() EmbeddingProvider {
	provider, err := newEmbeddedEosProvider(defaultEosProviderID, defaultEosProviderAssets, "assets/corkscrewdb-default-embedder")
	if err == nil {
		return provider
	}
	return newBuiltinProvider()
}
