package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/odvcencio/corkscrewdb"
)

// SearchHandler serves GET /memories/search backed by CorkScrewDB. It
// honors the same X-Read-Staleness header as MemoriesHandler so a caller
// can opt into replica reads for large result sets.
type SearchHandler struct {
	clients *DBClients
	logger  *log.Logger
}

// NewSearchHandler returns a handler wired to clients. A nil logger
// falls back to log.Default so callers don't have to supply one.
func NewSearchHandler(clients *DBClients, logger *log.Logger) *SearchHandler {
	if logger == nil {
		logger = log.Default()
	}
	return &SearchHandler{clients: clients, logger: logger}
}

// Register installs the handler's routes on mux. Callers are expected
// to wrap mux in AgentAuth so AgentFromContext works below.
func (h *SearchHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /memories/search", h.handleSearch)
}

// searchResultPayload is the JSON wire-format for one ranked hit.
type searchResultPayload struct {
	ID       string            `json:"id"`
	Score    float32           `json:"score"`
	Text     string            `json:"text,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// searchResponse is the envelope for successful search responses.
type searchResponse struct {
	Results []searchResultPayload `json:"results"`
}

const (
	defaultSearchK = 10
	maxSearchK     = 100
)

// writeJSON encodes v as JSON with the agreed content-type and status.
// Matches the MemoriesHandler helper behavior so responses are shaped
// identically across the API surface.
func (h *SearchHandler) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Printf("search: encode response: %v", err)
	}
}

// writeError sends a JSON error body with the given status. msg is used
// verbatim — callers pass safe, operator-chosen strings only.
func (h *SearchHandler) writeError(w http.ResponseWriter, status int, msg string) {
	h.writeJSON(w, status, map[string]string{"error": msg})
}

// parseFilters extracts repeated ?filter=key:value params from q and
// converts them into corkscrewdb.FilterOption values. A malformed
// entry (no ':' or empty key) causes an error; callers should 400.
func parseFilters(raw []string) ([]corkscrewdb.FilterOption, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]corkscrewdb.FilterOption, 0, len(raw))
	for _, f := range raw {
		idx := strings.Index(f, ":")
		if idx <= 0 {
			// idx == -1 → no colon at all; idx == 0 → empty key.
			return nil, errMalformedFilter
		}
		key := f[:idx]
		value := f[idx+1:]
		out = append(out, corkscrewdb.Filter(key, value))
	}
	return out, nil
}

// errMalformedFilter is returned by parseFilters on an invalid entry.
// Kept unexported so it cannot drift into a public wire contract.
var errMalformedFilter = &searchBadRequestError{"bad filter"}

// searchBadRequestError is a sentinel type for parse errors the handler
// maps to 400. Using a dedicated type avoids string-matching for control
// flow, which bit Task 5's mapWriteError.
type searchBadRequestError struct{ msg string }

func (e *searchBadRequestError) Error() string { return e.msg }

// handleSearch implements GET /memories/search.
func (h *SearchHandler) handleSearch(w http.ResponseWriter, r *http.Request) {
	// Defensive: the AgentAuth middleware must have set this. If it's
	// missing, the handler was wired without the middleware — a server
	// misconfiguration, not a client error.
	if _, ok := AgentFromContext(r.Context()); !ok {
		h.logger.Printf("search: missing agent in context; middleware misconfigured")
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}

	query := r.URL.Query()

	collection := strings.TrimSpace(query.Get("collection"))
	q := query.Get("q")
	// q keeps leading/trailing spaces intact — callers may legitimately
	// search with quoted phrases — but an empty/whitespace-only q is an
	// obvious 400.
	if collection == "" || strings.TrimSpace(q) == "" {
		h.writeError(w, http.StatusBadRequest, "collection and q are required")
		return
	}

	// Parse k with default + bounds.
	k := defaultSearchK
	if s := query.Get("k"); s != "" {
		parsed, err := strconv.Atoi(s)
		if err != nil || parsed <= 0 || parsed > maxSearchK {
			h.writeError(w, http.StatusBadRequest, "k must be an integer in 1..100")
			return
		}
		k = parsed
	}

	// Parse optional point-in-time HLC.
	var atHLC uint64
	hasAt := false
	if s := query.Get("at"); s != "" {
		parsed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "at must be a uint64")
			return
		}
		atHLC = parsed
		hasAt = true
	}

	// Parse optional repeatable filters.
	filters, err := parseFilters(query["filter"])
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "filter must be key:value")
		return
	}

	// Pick client based on staleness opt-in. Search bypasses the
	// read-your-writes guarantee only when the caller explicitly asks.
	staleOK := r.Header.Get("X-Read-Staleness") == "replica-ok"
	db := h.clients.ReadClient(staleOK)
	coll := db.Collection(collection)

	var results []corkscrewdb.SearchResult
	var searchErr error
	if hasAt {
		results, searchErr = coll.At(atHLC).Search(q, k, filters...)
	} else {
		results, searchErr = coll.Search(q, k, filters...)
	}
	if searchErr != nil {
		// Don't log filter values — they can carry PII-ish metadata
		// keys/values in production. Log only that the search failed.
		h.logger.Printf("search: coll=%s err=%v", collection, searchErr)
		h.writeError(w, http.StatusInternalServerError, "internal")
		return
	}

	// Always return a non-nil slice so the JSON body is {"results":[]}
	// rather than {"results":null} when there are no hits.
	payload := searchResponse{Results: make([]searchResultPayload, 0, len(results))}
	for _, r := range results {
		payload.Results = append(payload.Results, searchResultPayload{
			ID:       r.ID,
			Score:    r.Score,
			Text:     r.Text,
			Metadata: r.Metadata,
		})
	}
	h.writeJSON(w, http.StatusOK, payload)
}
