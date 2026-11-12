// Command consumer-notify turns TransferPosted events into notifications and
// streams them to clients on GET /v1/stream/events (Server-Sent Events).
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
		Service: "consumer-notify",
		Topic:   events.TopicTransfers,
		Build: func(_ config.Config, pool *pgxpool.Pool, _ *slog.Logger, mux *http.ServeMux, _ *prometheus.Registry) *consumer.Runner {
			hub := &consumer.Hub{}
			mux.Handle("GET /v1/stream/events", &consumer.Stream{Pool: pool, Hub: hub, Heartbeat: 15 * time.Second})
			return &consumer.Runner{
				Name: consumer.NotifyName, EventType: events.TypeTransferPosted, Handler: consumer.Notify{},
				OnCommitted: func(events.Header) { hub.Wake() },
			}
		},
	}))
}
