package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/Aditya-7826/ledger-service/internal/ledger"
)

// MaxBodyBytes is the largest request body the API accepts.
const MaxBodyBytes = 16 << 10

// problem is an RFC 7807 error body.
type problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail"`
	Code      string `json:"code"`
	RequestID string `json:"request_id,omitempty"`
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:      "urn:ledger:error:" + code,
		Title:     http.StatusText(status),
		Status:    status,
		Detail:    detail,
		Code:      code,
		RequestID: middleware.GetReqID(r.Context()),
	})
}

// writeError maps an error to a problem response. Anything that is not a
// ledger.Error is logged and reported as a plain 500 with no internals.
func (a *api) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var le *ledger.Error
	if errors.As(err, &le) {
		writeProblem(w, r, le.Status, le.Code, le.Detail)
		return
	}
	if r.Context().Err() != nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "timeout", "the request timed out or was cancelled")
		return
	}
	a.log.Error("request failed", "error", err, "request_id", middleware.GetReqID(r.Context()),
		"method", r.Method, "path", r.URL.Path)
	writeProblem(w, r, http.StatusInternalServerError, "internal_error", "an internal error occurred")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSON reads a strict JSON object into dst: unknown fields, wrong
// types (a float or string where an integer is expected), trailing data and
// bodies over MaxBodyBytes are all rejected. It writes the problem response
// itself and reports whether decoding succeeded.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any, allowEmpty bool) bool {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(dst)
	if err == nil {
		// Exactly one JSON value is allowed.
		if _, err = dec.Token(); err == io.EOF {
			return true
		}
		if err == nil {
			err = errors.New("body must contain a single JSON object")
		}
	} else if errors.Is(err, io.EOF) {
		if allowEmpty {
			return true
		}
		err = errors.New("request body is required")
	}

	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeProblem(w, r, http.StatusRequestEntityTooLarge, "body_too_large", "request body must be at most 16 KB")
		return false
	}
	detail := strings.TrimPrefix(err.Error(), "json: ")
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		detail = "field " + typeErr.Field + " has the wrong type: expected " + jsonKind(typeErr) + ", got " + typeErr.Value
	}
	writeProblem(w, r, http.StatusBadRequest, "invalid_json", detail)
	return false
}

func jsonKind(e *json.UnmarshalTypeError) string {
	switch e.Type.Kind().String() {
	case "int64", "int":
		return "an integer"
	case "string":
		return "a string"
	default:
		return e.Type.Kind().String()
	}
}
