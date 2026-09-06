package middleware

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// statusCodes maps HTTP statuses to stable machine-readable error codes
// per the API contract.
var statusCodes = map[int]string{
	http.StatusBadRequest:          "invalid_request",
	http.StatusUnauthorized:        "unauthorized",
	http.StatusNotFound:            "not_found",
	http.StatusMethodNotAllowed:    "method_not_allowed",
	http.StatusConflict:            "conflict",
	http.StatusInternalServerError: "internal",
}

// WriteError writes {"error":msg,"code":code} JSON with the given status.
func WriteError(w http.ResponseWriter, status int, msg string) {
	code, ok := statusCodes[status]
	if !ok {
		code = "error"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg, "code": code})
}

// Logger logs each request.
func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

// APIKey enforces a shared secret via the X-API-Key header on every route
// except /health. An empty expected key disables auth (open mode) so local
// dev stays frictionless and existing deployments keep working.
func APIKey(expected string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expected == "" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("X-API-Key") != expected {
			WriteError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// JSONContentType ensures JSON responses.
func JSONContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}
