package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Aditya-7826/ledger-service/internal/config"
	"github.com/Aditya-7826/ledger-service/internal/events"
	"github.com/Aditya-7826/ledger-service/internal/store"
)

// BalanceView is the response of GET /v1/views/balances/{id}.
type BalanceView struct {
	AccountID     uuid.UUID  `json:"account_id"`
	BalanceMinor  int64      `json:"balance_minor"`
	LastSeq       int64      `json:"last_seq"`
	UpdatedAt     *time.Time `json:"updated_at"`
	LedgerLastSeq int64      `json:"ledger_last_seq"`
	// LagSeq is how many entries the view is behind the ledger (0 = caught up).
	LagSeq int64 `json:"lag_seq"`
}

// BalanceViewHandler serves the read model together with its lag against the ledger.
func BalanceViewHandler(pool *pgxpool.Pool, ledger *HTTPLedger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil || len(r.PathValue("id")) != 36 {
			writeProblem(w, http.StatusBadRequest, "invalid_uuid", "the id in the path is not a valid UUID")
			return
		}
		ledgerSeq, err := ledger.LastSeq(r.Context(), id)
		if errors.Is(err, ErrAccountNotFound) {
			writeProblem(w, http.StatusNotFound, "account_not_found", fmt.Sprintf("account %s not found", id))
			return
		}
		if err != nil {
			writeProblem(w, http.StatusBadGateway, "ledger_unreachable", "the ledger API could not be reached")
			return
		}
		view := BalanceView{AccountID: id, LedgerLastSeq: ledgerSeq}
		var updated time.Time
		err = pool.QueryRow(r.Context(), `SELECT balance_minor, last_seq, updated_at FROM views.balance_view
			WHERE account_id = $1`, id).Scan(&view.BalanceMinor, &view.LastSeq, &updated)
		switch {
		case err == nil:
			updated = updated.UTC()
			view.UpdatedAt = &updated
		case !errors.Is(err, pgx.ErrNoRows): // no row yet simply means "nothing applied"
			writeProblem(w, http.StatusInternalServerError, "internal_error", "an internal error occurred")
			return
		}
		view.LagSeq = max(ledgerSeq-view.LastSeq, 0)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(view)
	}
}

func writeProblem(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "urn:ledger:error:" + code, "title": http.StatusText(status), "status": status, "detail": detail, "code": code,
	})
}

// App is what a consumer binary supplies to Main.
type App struct {
	Service string // log and metrics name, e.g. "consumer-balances"
	Topic   string
	// Build returns the runner and any extra HTTP routes. It runs after the
	// pool and Kafka client exist.
	Build func(cfg config.Config, pool *pgxpool.Pool, log *slog.Logger, mux *http.ServeMux, registry *prometheus.Registry) *Runner
}

// Main runs a consumer service: config, pool, topics, HTTP server, runner,
// graceful shutdown. It returns the process exit code.
func Main(app App) int {
	config.LoadDotEnv(".env")
	cfg, err := config.Load()
	if err == nil && len(cfg.KafkaBrokers) == 0 {
		err = errors.New("KAFKA_BROKERS is required")
	}
	if err != nil {
		slog.Error(app.Service+" stopped", "error", err)
		return 1
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).With("service", app.Service)
	slog.SetDefault(log)
	fail := func(what string, err error) int {
		log.Error(what, "error", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := store.NewPool(ctx, cfg.DatabaseURL, 8)
	if err != nil {
		return fail("connect to the database", err)
	}
	defer pool.Close()
	if err := events.EnsureTopics(ctx, cfg.KafkaBrokers, app.Topic, app.Topic+DLQSuffix); err != nil {
		return fail("create topics", err)
	}
	client, err := NewClient(cfg.KafkaBrokers, app.Service, app.Topic)
	if err != nil {
		return fail("connect to the broker", err)
	}
	defer client.Close()

	registry := prometheus.NewRegistry()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })

	runner := app.Build(cfg, pool, log, mux, registry)
	runner.Pool, runner.Client, runner.Log = pool, client, log
	counter := func(name, help string, read func() int64) {
		registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help},
			func() float64 { return float64(read()) }))
	}
	counter("ledger_consumer_applied_total", "Events applied.", runner.Stats.Applied.Load)
	counter("ledger_consumer_duplicates_total", "Redelivered events skipped by event_id.", runner.Stats.Duplicates.Load)
	counter("ledger_consumer_dead_lettered_total", "Poison messages moved to the DLQ.", runner.Stats.DeadLettered.Load)

	// No WriteTimeout: the notification stream is a long-lived response.
	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.MetricsPort), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "error", err)
		}
	}()

	log.Info("consumer running", "topic", app.Topic, "group", app.Service, "http_addr", srv.Addr)
	runner.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("consumer stopped")
	return 0
}
