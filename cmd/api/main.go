// Command api serves the ledger HTTP API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Aditya-7826/ledger-service/internal/config"
	"github.com/Aditya-7826/ledger-service/internal/httpapi"
	"github.com/Aditya-7826/ledger-service/internal/idempotency"
	"github.com/Aditya-7826/ledger-service/internal/ledger"
	"github.com/Aditya-7826/ledger-service/internal/store"
)

const shutdownGrace = 10 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("api stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	config.LoadDotEnv(".env")
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).With("service", "api")
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := store.Migrate(ctx, cfg.DatabaseURL); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	pool, err := store.NewPool(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()

	svc := ledger.NewService(pool, ledger.Options{MaxTransferMinor: cfg.MaxTransferMinor, IdempotencyTTL: cfg.IdempotencyTTL})
	go idempotency.RunCleanup(ctx, pool, time.Minute, log)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.APIPort),
		Handler:           httpapi.NewRouter(svc, pool, log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Info("api listening", "addr", srv.Addr)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down: finishing in-flight requests", "grace", shutdownGrace.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}
