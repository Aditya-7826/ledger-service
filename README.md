<h1 align="center">Ledger Service</h1>

<p align="center">A double-entry payment ledger: balanced entries, retries that never charge twice, and events that always arrive.</p>

> **Work in progress.** This README only describes what exists today. The table below shows what is built.

## Project status

| Phase | Scope | Status |
|---|---|---|
| 0 | Repo setup: Go module, Docker Compose (PostgreSQL 16 + Redpanda), dev scripts | Done |
| 1 | Ledger core: accounts, transfers, deposits, reversals, idempotency keys, reconciliation | Not started |
| 2 | Transactional outbox and relay to Redpanda | Not started |
| 3 | Consumers: balance read model, notifications over SSE, dead-letter topics | Not started |
| 4 | Web dashboard (React + Vite + TypeScript + Tailwind) | Not started |
| 5 | Seed data and k6 load tests | Not started |
| 6 | CI and final documentation | Not started |

Performance and correctness numbers: **not yet measured**.

## What runs today

Phase 0 gives a local infrastructure stack sized for an 8 GB laptop:

| Container | Image | Memory limit | Host port |
|---|---|---|---|
| `ledger-postgres` | `postgres:16-alpine` | 512 MB | `localhost:5434` |
| `ledger-redpanda` | `redpandadata/redpanda` (single node, dev mode, `--smp 1 --memory 900M --overprovisioned`) | 1 GB | `localhost:19092` (Kafka API) |

Both have health checks, and both ports are bound to `localhost` only.

## Run it

Prerequisites: Docker Desktop (or Docker Engine with the Compose plugin) and Git.

**PowerShell**

```powershell
git clone https://github.com/Aditya-7826/ledger-service.git
cd ledger-service
Copy-Item .env.example .env      # then change POSTGRES_PASSWORD in .env
.\scripts\dev.ps1 up             # start Postgres + Redpanda, wait until healthy
.\scripts\dev.ps1 down           # stop (data is kept)
.\scripts\dev.ps1 reset          # stop, delete data, start again
```

**bash**

```bash
git clone https://github.com/Aditya-7826/ledger-service.git
cd ledger-service
cp .env.example .env             # then change POSTGRES_PASSWORD in .env
./scripts/dev.sh up              # start Postgres + Redpanda, wait until healthy
./scripts/dev.sh down            # stop (data is kept)
./scripts/dev.sh reset           # stop, delete data, start again
```

The `test`, `seed` and `loadtest` subcommands exist in both scripts; `seed` and `loadtest` report that they are not built yet until Phase 5.

## Project structure

```
cmd/                 one folder per service: api, outbox-relay, consumer-notify, consumer-balances
internal/            ledger, idempotency, outbox, events, httpapi, store, config
migrations/          SQL migrations (goose)
web/                 dashboard
loadtest/            k6 scenarios
scripts/             dev.ps1 and dev.sh
docs/                diagrams and screenshots
results/             load-test result files
.github/workflows/   CI
docker-compose.yml
```

Most of these folders are empty until their phase is built.

## Licence

[MIT](LICENSE)
