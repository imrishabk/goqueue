package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func hello(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("hi"))
}

func TestAPIKey_OpenWhenUnset(t *testing.T) {
	h := APIKey("", http.HandlerFunc(hello))
	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected open 200, got %d", w.Code)
	}
}

func TestAPIKey_RejectsMissingAndWrong(t *testing.T) {
	h := APIKey("secret", http.HandlerFunc(hello))
	for _, key := range []string{"", "wrong"} {
		req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		if key != "" {
			req.Header.Set("X-API-Key", key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("key %q: expected 401, got %d", key, w.Code)
		}
		var body map[string]string
		if err := json.NewDecoder(w.Body).Decode(&body); err != nil || body["error"] == "" {
			t.Fatalf("expected json error body, got %q err=%v", w.Body.String(), err)
		}
	}
}

func TestAPIKey_AcceptsCorrect(t *testing.T) {
	h := APIKey("secret", http.HandlerFunc(hello))
	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	req.Header.Set("X-API-Key", "secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestAPIKey_HealthBypass(t *testing.T) {
	h := APIKey("secret", http.HandlerFunc(hello))
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected /health bypass 200, got %d", w.Code)
	}
}
