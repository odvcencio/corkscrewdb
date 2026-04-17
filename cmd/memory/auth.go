package main

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
)

// agentContextKey is the context key under which AgentAuth stores the
// matched agent identity on an authenticated request.
type agentContextKey struct{}

// AgentFromContext returns the matched agent name on a request that
// passed through AgentAuth. The second return value is false if the
// request was not agent-authenticated.
func AgentFromContext(ctx context.Context) (string, bool) {
	name, ok := ctx.Value(agentContextKey{}).(string)
	return name, ok && name != ""
}

// AgentAuth returns a middleware that rejects requests whose bearer
// token does not match any entry in tokens. On success it injects the
// matched agent name into the request context.
func AgentAuth(next http.Handler, tokens map[string]string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer, ok := bearerToken(r)
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		agent := matchAgentToken(tokens, bearer)
		if agent == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), agentContextKey{}, agent)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AdminAuth returns a middleware that rejects requests whose bearer
// token does not equal admin (constant-time compare). Agent tokens are
// rejected because they won't match the admin constant.
func AdminAuth(next http.Handler, admin string) http.Handler {
	adminBytes := []byte(admin)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer, ok := bearerToken(r)
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if len(admin) == 0 || subtle.ConstantTimeCompare([]byte(bearer), adminBytes) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerToken extracts the bearer token from an Authorization header.
// Returns ("", false) if the header is missing or not a well-formed
// "Bearer <token>" value with a non-empty token.
func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	token := strings.TrimSpace(h[len(prefix):])
	if token == "" {
		return "", false
	}
	return token, true
}

// matchAgentToken returns the agent name whose stored token equals
// candidate, using constant-time compares. Returns "" on no match.
// The loop runs across every agent even after a match so matching time
// is not dependent on which agent hit.
func matchAgentToken(tokens map[string]string, candidate string) string {
	candidateBytes := []byte(candidate)
	var matched string
	for name, tok := range tokens {
		if len(tok) == len(candidate) && subtle.ConstantTimeCompare([]byte(tok), candidateBytes) == 1 {
			matched = name
		}
	}
	return matched
}
