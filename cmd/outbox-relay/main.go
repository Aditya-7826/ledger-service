// Command outbox-relay publishes outbox rows to Redpanda (Kafka API).
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Aditya-7826/ledger-service/internal/config"
	"github.com/Aditya-7826/ledger-service/internal/events"
	"github.com/Aditya-7826/ledger-service/internal/outbox"
	"github.com/Aditya-7826/ledger-service/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("outbox-relay stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	config.LoadDotEnv(".env")
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if len(cfg.KafkaBrokers) == 0 {
		return errors.New("KAFKA_BROKERS is required")
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).With("service", "outbox-relay")
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The relay needs few connections: one transaction at a time plus the backlog query.
	pool, err := store.NewPool(ctx, cfg.DatabaseURL, 4)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := events.EnsureTopics(ctx, cfg.KafkaBrokers, events.TopicEntries, events.TopicTransfers); err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	publisher, err := outbox.NewKafkaPublisher(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer publisher.Close()

	registry := prometheus.NewRegistry()
	backlog := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ledger_outbox_unpublished",
		Help: "Outbox rows that have not been published to the broker yet.",
	})
	published := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ledger_outbox_published_total",
		Help: "Outbox rows published and marked since this relay started.",
	})
	registry.MustRegister(backlog, published)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.MetricsPort), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "error", err)
		}
	}()

	relay := &outbox.Relay{
		Pool:      pool,
		Publisher: countingPublisher{publisher, published},
		Log:       log,
		OnBacklog: func(n int64) { backlog.Set(float64(n)) },
	}
	log.Info("outbox relay running", "brokers", cfg.KafkaBrokers, "metrics_addr", srv.Addr)
	relay.Run(ctx, 200*time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("outbox relay stopped")
	return nil
}

type countingPublisher struct {
	outbox.Publisher
	published prometheus.Counter
}

func (c countingPublisher) Publish(ctx context.Context, msgs []outbox.Message) error {
	if err := c.Publisher.Publish(ctx, msgs); err != nil {
		return err
	}
	c.published.Add(float64(len(msgs)))
	return nil
}
