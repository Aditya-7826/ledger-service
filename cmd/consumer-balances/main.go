// Command consumer-balances builds the balance read model from EntryPosted
// events and serves it on GET /v1/views/balances/{id}.
package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Aditya-7826/ledger-service/internal/config"
	"github.com/Aditya-7826/ledger-service/internal/consumer"
	"github.com/Aditya-7826/ledger-service/internal/events"
)

func main() {
	os.Exit(consumer.Main(consumer.App{
		Service: "consumer-balances",
		Topic:   events.TopicEntries,
		Build: func(cfg config.Config, pool *pgxpool.Pool, log *slog.Logger, mux *http.ServeMux, registry *prometheus.Registry) *consumer.Runner {
			ledger := &consumer.HTTPLedger{BaseURL: cfg.LedgerAPIURL, Client: &http.Client{Timeout: 10 * time.Second}}
			handler := &consumer.Balances{Ledger: ledger, GapTimeout: cfg.GapTimeout, Log: log}
			registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
				Name: "ledger_balance_view_rebuilds_total",
				Help: "Accounts rebuilt from the ledger after a sequence gap persisted.",
			}, func() float64 { return float64(handler.Rebuilds.Load()) }))
			mux.Handle("GET /v1/views/balances/{id}", consumer.BalanceViewHandler(pool, ledger))
			return &consumer.Runner{Name: consumer.BalancesName, EventType: events.TypeEntryPosted, Handler: handler}
		},
	}))
}
