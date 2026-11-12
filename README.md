<h1 align="center">Ledger Service</h1>

<p align="center">A double-entry payment ledger: balanced entries, retries that never charge twice, and events that always arrive.</p>

> **Work in progress.** This README only describes what exists today. The table below shows what is built.

## Project status

| Phase | Scope | Status |
|---|---|---|
| 0 | Repo setup: Go module, Docker Compose (PostgreSQL 16 + Redpanda), dev scripts | Done |
| 1 | Ledger core: accounts, transfers, deposits, reversals, idempotency keys, reconciliation | Done |
| 2 | Transactional outbox and relay to Redpanda | Done |
| 3 | Consumers: balance read model, notifications over SSE, dead-letter topics | Not started |
| 4 | Web dashboard (React + Vite + TypeScript + Tailwind) | Not started |
| 5 | Seed data and k6 load tests | Not started |
| 6 | CI and final documentation | Not started |

Performance numbers: **not yet measured** (Phase 5).

## What runs today

`scripts/dev up` starts four containers, sized for an 8 GB laptop. All ports are bound to `localhost` only.

| Container | What it is | Memory limit | Host port |
|---|---|---|---|
| `ledger-api` | The ledger HTTP API (Go, chi, pgx) | 256 MB | `localhost:8080` |
| `ledger-postgres` | PostgreSQL 16, the single source of truth | 512 MB | `localhost:5434` |
| `ledger-outbox-relay` | Publishes outbox rows to Redpanda | 256 MB | `localhost:9101` (`/metrics`, `/healthz`) |
| `ledger-redpanda` | Redpanda, single node, dev mode (`--smp 1 --memory 900M --overprovisioned`), Kafka API | 1 GB | `localhost:19092` |

The API applies its database migrations (goose SQL files in `migrations/`) when it starts.

## API

Money is always an integer number of minor units (paise): `BIGINT` in PostgreSQL, `int64` in Go, a JSON integer on the wire. `10.5`, `"100"` and `1e3` are rejected.

Every write that moves money needs two headers: `Idempotency-Key` and `X-Client-Id` (each 1–64 characters of `A-Z a-z 0-9 _ -`).

| Method and path | What it does |
|---|---|
| `POST /v1/accounts` | Create an account (`name`, `currency`) |
| `GET /v1/accounts?cursor=&limit=` | List accounts with balances |
| `GET /v1/accounts/{id}` | One account with its balance |
| `GET /v1/accounts/{id}/entries?cursor=&limit=` | The account's entries, newest first |
| `POST /v1/accounts/{id}/freeze`, `/unfreeze` | Block or unblock an account |
| `POST /v1/deposits` | Add money to an account from the SYSTEM account (idempotent) |
| `POST /v1/transfers` | Move money between two accounts (idempotent) |
| `GET /v1/transfers?cursor=&limit=` | List transfers with their entries, newest first |
| `GET /v1/transfers/{id}` | One transfer with its DEBIT and CREDIT entry |
| `POST /v1/transfers/{id}/reverse` | Create a compensating REVERSAL (idempotent) |
| `GET /v1/reconcile` | Audit the whole ledger (four checks, below) |
| `GET /healthz`, `GET /readyz` | Liveness, and readiness (database reachable) |

Lists use keyset pagination (`limit` 1–100, default 50; pass `next_cursor` back as `cursor`). Errors are RFC 7807 `application/problem+json` with a stable `code` and no internals.

## How a transfer works

`POST /v1/transfers` validates the request before touching the database (strict JSON with no unknown fields, integer amount within `MAX_TRANSFER_MINOR`, two different valid UUIDs, 3-letter currency, memo of at most 280 characters, body of at most 16 KB). Then **one database transaction** does everything else:

1. **Claim the idempotency key** with `INSERT ... ON CONFLICT DO NOTHING`. A concurrent request with the same key waits on the primary-key index until the first one commits or rolls back. If the key already has a stored response and the request hash (SHA-256 of the canonical request) matches, that response is replayed with the header `Idempotent-Replayed: true`. If the hash differs, the answer is `422 idempotency_key_reused`.
2. **Lock both accounts** with `SELECT ... FOR UPDATE`, always in ascending UUID order, so A→B and B→A can never deadlock.
3. **Re-validate under the lock**: both accounts exist and are ACTIVE, currencies match each other and the request, the sender has enough money, and the new balances fit in `int64`. Only the SYSTEM account may go below zero; that is how deposits enter the ledger.
4. **Write** the transfer, one DEBIT entry and one CREDIT entry (each with the account's next `account_seq`), update both rows of the `account_balances` projection, and insert three events into the `outbox` table (below).
5. **Store the response** on the idempotency key and commit.

If PostgreSQL reports a deadlock (`40P01`) or serialization failure (`40001`), the transaction is retried up to 3 times with jittered backoff. Validation and business errors are never retried.

A T-account view of a ₹125.50 transfer from Asha to Ravi (amounts in paise):

```
        Asha                      Ravi
  DEBIT  | CREDIT           DEBIT  | CREDIT
  12550  |                         | 12550
```

Debits always equal credits, so the signed sum of all entries in the ledger is always zero.

## Events: the transactional outbox

Other services need to hear about every transfer. Publishing to a broker from the request handler is not safe: if the process dies after the database commit but before the publish, the event is lost; publish first, and a rollback leaves an event for money that never moved.

So the API never talks to the broker. Inside the transfer transaction it inserts rows into an `outbox` table:

| Event | When | Kafka key | Topic |
|---|---|---|---|
| `EntryPosted` | once per entry (two per transfer), carries `account_seq` | account ID | `ledger.entries` |
| `TransferPosted` | once per transfer | transfer ID | `ledger.transfers` |

Every event carries `event_id`, `event_type`, `schema_version` and `occurred_at`. An event exists exactly when its transfer committed.

The **outbox relay** (`cmd/outbox-relay`) is a separate process that loops:

1. `SELECT ... WHERE published_at IS NULL ORDER BY id LIMIT 500 FOR UPDATE SKIP LOCKED`. The row locks are held until step 3, so several relays can run at once and never pick the same row.
2. Produce the batch with franz-go's idempotent producer (`acks=all`). The key is the aggregate ID, so all events of one account land in one partition, in order.
3. Set `published_at` only after the broker has acknowledged every record. If publishing fails, the transaction rolls back, nothing is marked, and the next loop retries with backoff (100 ms doubling to 5 s).

It also creates both topics at startup (3 partitions each), prunes published rows older than 7 days, and exposes the gauge `ledger_outbox_unpublished` (the backlog) and the counter `ledger_outbox_published_total` on `/metrics`.

**Delivery is at-least-once.** If the relay dies after the broker acknowledged but before `published_at` was written, the event is published again after restart. That cannot be avoided: the database and the broker are two systems, and no transaction spans both. Exactly-once *processing* therefore has to come from the consumer, which records each `event_id` it has handled in its own database transaction (Phase 3).

### Optional monitoring

Prometheus and Grafana are in a Compose profile that is off by default, to save RAM (256 MB limit each):

```powershell
docker compose --profile monitoring up -d      # PowerShell and bash are the same
```

Prometheus is then on `http://localhost:9090` and Grafana on `http://localhost:3000` (anonymous read-only access, Prometheus already added as a data source).

## Design decisions

<details>
<summary><b>Idempotency keys live in PostgreSQL, not Redis</b></summary>

The key is written in the same transaction as the ledger entries. With a cache in front, "check the key" and "write the money" would be two systems, and a crash between them either charges twice (key lost, money written) or loses the request (key written, money not). One transaction in one database cannot end up half done.
</details>

<details>
<summary><b>A business rejection does not use up the idempotency key</b></summary>

Insufficient funds, a frozen account and similar rejections return a 4xx and roll back the whole transaction, including the key. A client that retries the same key after a deposit arrives gets a real attempt, not a replay of the old rejection. The trade-off: a rejection is not "sticky", so two retries of a rejected request can give different answers if the account changed in between. For a ledger that is the more useful behaviour, because only successful money movement must be exactly-once.
</details>

<details>
<summary><b>Replays are byte-identical</b></summary>

The response is stored as `jsonb`, and both the first response and every replay are sent exactly as PostgreSQL returns that column. A replay can therefore never differ from the original, even in whitespace or key order.
</details>

<details>
<summary><b>Balances are a projection; entries are the truth</b></summary>

`entries` is append-only: a trigger rejects `UPDATE`, `DELETE` and `TRUNCATE`. `account_balances` is updated in the same transaction and exists only so that reads and the funds check are fast. `GET /v1/reconcile` proves the two agree.
</details>

<details>
<summary><b>Freeze takes the same lock as a transfer</b></summary>

Freezing locks the account row a transfer locks. An in-flight transfer either commits before the freeze, or sees `FROZEN` afterwards and gets `403 account_frozen`. There is no window where a transfer slips through after the freeze has returned.
</details>

<details>
<summary><b>Other choices</b></summary>

- **Reversals** are new compensating transfers, never edits. A unique constraint on `reverses_transfer_id` means a transfer can be reversed only once, and a reversal cannot itself be reversed. If the original receiver no longer has the money, the answer is `409 insufficient_funds`.
- **Keyset pagination**: transfer and entry lists page by an indexed key (`id`, `account_seq`), so page 1,000 costs the same as page 1.
- **Expired keys**: a background job deletes idempotency keys past their TTL (24 h by default) in batches of 1,000. After that, the key counts as new.
- **Graceful shutdown**: on SIGTERM or Ctrl+C the API finishes in-flight requests for up to 10 seconds.
</details>

## Reconciliation

`GET /v1/reconcile` runs four checks on one consistent snapshot and reports each one:

1. the signed sum of all entries is zero;
2. every `account_balances` row equals the sum and last sequence number of its entries;
3. every transfer has exactly one DEBIT and one CREDIT of the transfer's amount;
4. every account's `account_seq` runs 1, 2, 3 … with no gaps.

## Run it

Prerequisites: Docker Desktop (or Docker Engine with the Compose plugin), Git, and Go 1.26 or newer to run the tests.

**PowerShell**

```powershell
git clone https://github.com/Aditya-7826/ledger-service.git
cd ledger-service
Copy-Item .env.example .env      # then change POSTGRES_PASSWORD in .env (both places)
.\scripts\dev.ps1 up             # build the API, start everything, wait until healthy
.\scripts\dev.ps1 test           # run the Go tests (needs Docker running)
.\scripts\dev.ps1 down           # stop (data is kept)
.\scripts\dev.ps1 reset          # stop, delete data, start again
```

**bash**

```bash
git clone https://github.com/Aditya-7826/ledger-service.git
cd ledger-service
cp .env.example .env             # then change POSTGRES_PASSWORD in .env (both places)
./scripts/dev.sh up              # build the API, start everything, wait until healthy
./scripts/dev.sh test            # run the Go tests (needs Docker running)
./scripts/dev.sh down            # stop (data is kept)
./scripts/dev.sh reset           # stop, delete data, start again
```

`seed` and `loadtest` exist in both scripts but report that they are not built yet (Phase 5).

### Try it

**PowerShell**

```powershell
$api = "http://localhost:8080"
$asha = Invoke-RestMethod -Method Post "$api/v1/accounts" -ContentType "application/json" -Body '{"name":"Asha","currency":"INR"}'
$ravi = Invoke-RestMethod -Method Post "$api/v1/accounts" -ContentType "application/json" -Body '{"name":"Ravi","currency":"INR"}'
$h = @{ "Idempotency-Key" = "deposit-1"; "X-Client-Id" = "demo" }
Invoke-RestMethod -Method Post "$api/v1/deposits" -Headers $h -ContentType "application/json" -Body (@{ account_id = $asha.id; amount_minor = 50000; currency = "INR" } | ConvertTo-Json)

$h = @{ "Idempotency-Key" = "transfer-1"; "X-Client-Id" = "demo" }
$body = @{ from_account_id = $asha.id; to_account_id = $ravi.id; amount_minor = 12550; currency = "INR"; memo = "lunch" } | ConvertTo-Json
Invoke-WebRequest -Method Post "$api/v1/transfers" -Headers $h -ContentType "application/json" -Body $body -UseBasicParsing | Select-Object StatusCode, Headers
# Run the last line again: same response, plus the header Idempotent-Replayed: true, and no second charge.
Invoke-RestMethod "$api/v1/reconcile"
```

**bash**

```bash
api=http://localhost:8080
asha=$(curl -s -H 'Content-Type: application/json' -d '{"name":"Asha","currency":"INR"}' $api/v1/accounts | sed 's/.*"id":"\([^"]*\)".*/\1/')
ravi=$(curl -s -H 'Content-Type: application/json' -d '{"name":"Ravi","currency":"INR"}' $api/v1/accounts | sed 's/.*"id":"\([^"]*\)".*/\1/')
curl -s -H 'Content-Type: application/json' -H 'Idempotency-Key: deposit-1' -H 'X-Client-Id: demo' \
  -d "{\"account_id\":\"$asha\",\"amount_minor\":50000,\"currency\":\"INR\"}" $api/v1/deposits

curl -si -H 'Content-Type: application/json' -H 'Idempotency-Key: transfer-1' -H 'X-Client-Id: demo' \
  -d "{\"from_account_id\":\"$asha\",\"to_account_id\":\"$ravi\",\"amount_minor\":12550,\"currency\":\"INR\",\"memo\":\"lunch\"}" $api/v1/transfers
# Run the last command again: same response, plus the header Idempotent-Replayed: true, and no second charge.
curl -s $api/v1/reconcile
```

## Tests

The tests run the real HTTP API against a real PostgreSQL 16 started by testcontainers-go (one container for the whole package; packages run one at a time with `-p 1`).

| Test | What it proves |
|---|---|
| Concurrency invariant | 50 goroutines make random transfers among 20 accounts for 15 s; afterwards reconcile passes, no balance is below zero and no money appeared or vanished |
| Deadlock | 2,000 opposing A→B / B→A transfers from 8 workers; every one succeeds |
| Idempotency | a replay is byte-identical and carries the replay header; a different body gets 422; 20 concurrent identical requests create exactly 1 transfer; a key rejected for insufficient funds succeeds on retry after a deposit; expired keys are cleaned up and count as new |
| Freeze race | once the freeze call has returned, no entry is written to the account and every later transfer gets 403 |
| Reversals | once works; twice is refused; a reversal of a reversal is refused; a receiver without funds gets 409 |
| Append-only | the trigger rejects UPDATE, DELETE and TRUNCATE on `entries` |
| Validation table | 29 bad requests (zero, negative, float, string, overflow, self-transfer, currency mismatch, unknown or closed account, long memo, malformed UUID, missing headers, oversized body, unknown field, …) each get the right status and code, create no transfer and leave no key behind |
| Reconcile | a tampered balance is detected |
| Retry | deadlocks and serialization failures are retried at most 3 times; other errors never |
| Outbox written atomically | a committed transfer has exactly 3 outbox rows; rejected requests write none |
| Relay: crash before publish | the publish fails, nothing is marked, and the restarted relay publishes every event exactly once, each account's events in one partition and in `account_seq` order |
| Relay: crash after publish, before marking | every event is delivered exactly twice (the at-least-once case) |
| Relay: two instances | two relays draining 600 rows in batches of 20 never publish the same row |
| Relay: pruning and topics | only rows published more than 7 days ago are deleted; both topics have 3 partitions |

The relay tests run against a real Redpanda container as well as PostgreSQL.

## Project structure

```
cmd/api/             the HTTP API binary
cmd/outbox-relay/    publishes outbox rows to Redpanda
cmd/                 consumer-notify, consumer-balances (Phase 3)
internal/events/     event types, topics, topic creation
internal/outbox/     the relay and its Kafka publisher
internal/testutil/   starts the PostgreSQL and Redpanda test containers
monitoring/          Prometheus and Grafana config for the optional profile
internal/config/     environment configuration
internal/store/      connection pool, migrations, retrying transaction helper
internal/idempotency/  idempotency keys in PostgreSQL, cleanup job
internal/ledger/     accounts, transfers, deposits, reversals, reconciliation
internal/httpapi/    routes, strict JSON decoding, problem+json errors, tests
migrations/          SQL migrations (goose)
scripts/             dev.ps1 and dev.sh
web/ loadtest/ docs/ results/   empty until their phase is built
Dockerfile           one image for every Go service
docker-compose.yml
```

## Licence

[MIT](LICENSE)
