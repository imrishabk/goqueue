package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteError_Shape(t *testing.T) {
	cases := map[int]string{
		http.StatusBadRequest:          "invalid_request",
		http.StatusUnauthorized:        "unauthorized",
		http.StatusNotFound:            "not_found",
		http.StatusMethodNotAllowed:    "method_not_allowed",
		http.StatusConflict:            "conflict",
		http.StatusInternalServerError: "internal",
	}
	for status, code := range cases {
		w := httptest.NewRecorder()
		WriteError(w, status, "boom")
		if w.Code != status {
			t.Fatalf("status %d: got %d", status, w.Code)
		}
		var body map[string]string
		if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
			t.Fatalf("status %d: decode %v", status, err)
		}
		if body["error"] != "boom" || body["code"] != code {
			t.Fatalf("status %d: bad body %v", status, body)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("status %d: content-type %s", status, ct)
		}
	}
}
