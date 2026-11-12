// Package testutil starts the containers the integration tests need. Each
// test package calls these once from TestMain and shares the containers
// between all of its tests.
package testutil

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

// Images match docker-compose.yml.
const (
	PostgresImage = "postgres:16-alpine"
	RedpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"
)

// StartPostgres starts PostgreSQL and returns its connection URL and a stop function.
func StartPostgres(ctx context.Context) (url string, stop func(), err error) {
	pg, err := postgres.Run(ctx, PostgresImage,
		postgres.WithDatabase("ledger"), postgres.WithUsername("ledger"), postgres.WithPassword("test"),
		postgres.BasicWaitStrategies())
	stop = func() {
		if pg != nil {
			_ = testcontainers.TerminateContainer(pg)
		}
	}
	if err != nil {
		return "", stop, err
	}
	url, err = pg.ConnectionString(ctx, "sslmode=disable")
	return url, stop, err
}

// StartRedpanda starts a single-node Redpanda and returns its Kafka address
// and a stop function.
func StartRedpanda(ctx context.Context) (broker string, stop func(), err error) {
	rp, err := redpanda.Run(ctx, RedpandaImage)
	stop = func() {
		if rp != nil {
			_ = testcontainers.TerminateContainer(rp)
		}
	}
	if err != nil {
		return "", stop, err
	}
	broker, err = rp.KafkaSeedBroker(ctx)
	return broker, stop, err
}
