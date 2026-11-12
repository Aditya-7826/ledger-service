// Package httpapi exposes the ledger over HTTP.
package httpapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Aditya-7826/ledger-service/internal/ledger"
)

// RequestTimeout bounds how long one request may run.
const RequestTimeout = 10 * time.Second

type api struct {
	svc  *ledger.Service
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewRouter builds the HTTP handler for the ledger API.
func NewRouter(svc *ledger.Service, pool *pgxpool.Pool, log *slog.Logger) http.Handler {
	a := &api{svc: svc, pool: pool, log: log}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(a.recoverer)
	r.Use(a.accessLog)
	r.Use(middleware.Timeout(RequestTimeout))

	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusNotFound, "not_found", "no such route")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed on this route")
	})

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/readyz", a.ready)

	r.Route("/v1", func(r chi.Router) {
		r.Post("/accounts", a.createAccount)
		r.Get("/accounts", a.listAccounts)
		r.Get("/accounts/{id}", a.getAccount)
		r.Get("/accounts/{id}/entries", a.listEntries)
		r.Post("/accounts/{id}/freeze", a.freeze(true))
		r.Post("/accounts/{id}/unfreeze", a.freeze(false))

		r.Post("/transfers", a.createTransfer)
		r.Get("/transfers", a.listTransfers)
		r.Get("/transfers/{id}", a.getTransfer)
		r.Post("/transfers/{id}/reverse", a.reverseTransfer)

		r.Post("/deposits", a.createDeposit)
		r.Get("/reconcile", a.reconcile)
		r.Get("/stats", a.stats)
	})
	return r
}

func (a *api) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.pool.Ping(ctx); err != nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "not_ready", "database is not reachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// recoverer turns a panic into a 500 problem response; the stack goes to the log only.
func (a *api) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				a.log.Error("panic", "panic", fmt.Sprint(rec), "stack", string(debug.Stack()),
					"request_id", middleware.GetReqID(r.Context()))
				writeProblem(w, r, http.StatusInternalServerError, "internal_error", "an internal error occurred")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (a *api) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		a.log.Debug("request", "method", r.Method, "path", r.URL.Path, "status", ww.Status(),
			"duration_ms", time.Since(start).Milliseconds(), "request_id", middleware.GetReqID(r.Context()))
	})
}
