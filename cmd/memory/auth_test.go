package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// testAgentTokens is the fixture map used across auth tests.
func testAgentTokens() map[string]string {
	return map[string]string{
		"birch": "tok-birch",
		"cedar": "tok-cedar",
	}
}

const testAdminToken = "tok-admin"

// okHandler writes "ok" on 200. When the request was agent-authenticated,
// it appends ":" + agent name so tests can confirm context injection.
func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if name, ok := AgentFromContext(r.Context()); ok {
			fmt.Fprintf(w, "ok:%s", name)
			return
		}
		fmt.Fprint(w, "ok")
	})
}

func TestAuthAgentValidToken(t *testing.T) {
	h := AgentAuth(okHandler(), testAgentTokens())

	req := httptest.NewRequest(http.MethodGet, "/agent/ping", nil)
	req.Header.Set("Authorization", "Bearer tok-cedar")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d (body=%q)", rr.Code, rr.Body.String())
	}
	if got := rr.Body.String(); got != "ok:cedar" {
		t.Fatalf("body: want %q, got %q", "ok:cedar", got)
	}
}

func TestAuthAgentRejectsAdminToken(t *testing.T) {
	h := AgentAuth(okHandler(), testAgentTokens())

	req := httptest.NewRequest(http.MethodGet, "/agent/ping", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d", rr.Code)
	}
}

func TestAuthAgentRejectsUnknownBearer(t *testing.T) {
	h := AgentAuth(okHandler(), testAgentTokens())

	req := httptest.NewRequest(http.MethodGet, "/agent/ping", nil)
	req.Header.Set("Authorization", "Bearer totally-unknown")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d", rr.Code)
	}
}

func TestAuthAgentRejectsMissingHeader(t *testing.T) {
	h := AgentAuth(okHandler(), testAgentTokens())

	req := httptest.NewRequest(http.MethodGet, "/agent/ping", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d", rr.Code)
	}
}

func TestAuthAgentRejectsMalformedHeader(t *testing.T) {
	cases := []struct {
		name   string
		header string
	}{
		{"bearer no token", "Bearer "},
		{"bearer no space", "Bearer"},
		{"basic scheme", "Basic dXNlcjpwYXNz"},
		{"lowercase bearer", "bearer tok-birch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := AgentAuth(okHandler(), testAgentTokens())
			req := httptest.NewRequest(http.MethodGet, "/agent/ping", nil)
			req.Header.Set("Authorization", tc.header)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusUnauthorized {
				t.Fatalf("header=%q: want 401, got %d", tc.header, rr.Code)
			}
		})
	}
}

func TestAuthAdminValidToken(t *testing.T) {
	h := AdminAuth(okHandler(), testAdminToken)

	req := httptest.NewRequest(http.MethodGet, "/admin/ping", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d (body=%q)", rr.Code, rr.Body.String())
	}
	if got := rr.Body.String(); got != "ok" {
		t.Fatalf("body: want %q, got %q", "ok", got)
	}
}

func TestAuthAdminRejectsAgentToken(t *testing.T) {
	h := AdminAuth(okHandler(), testAdminToken)

	req := httptest.NewRequest(http.MethodGet, "/admin/ping", nil)
	req.Header.Set("Authorization", "Bearer tok-birch")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d", rr.Code)
	}
}

func TestAuthAdminRejectsMissingHeader(t *testing.T) {
	h := AdminAuth(okHandler(), testAdminToken)

	req := httptest.NewRequest(http.MethodGet, "/admin/ping", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status: want 401, got %d", rr.Code)
	}
}

// TestAuthAdminContextMarker asserts that AdminAuth annotates the
// request context so downstream admin handlers can verify they were
// reached through the correct middleware, and that IsAdminContext
// returns false for a plain context that did not pass through.
func TestAuthAdminContextMarker(t *testing.T) {
	// Before AdminAuth: plain context → false.
	if IsAdminContext(context.Background()) {
		t.Fatalf("IsAdminContext on plain ctx: want false, got true")
	}

	var observed bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = IsAdminContext(r.Context())
		fmt.Fprint(w, "ok")
	})
	h := AdminAuth(inner, testAdminToken)

	req := httptest.NewRequest(http.MethodGet, "/admin/ping", nil)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d (body=%q)", rr.Code, rr.Body.String())
	}
	if !observed {
		t.Fatalf("IsAdminContext after AdminAuth: want true, got false")
	}
}
