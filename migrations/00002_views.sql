-- +goose Up

-- Everything the consumers own lives in its own schema, apart from the ledger tables.
CREATE SCHEMA views;

-- Deduplication: a consumer inserts the event_id here in the same transaction
-- as its side effect. A second delivery of the same event conflicts and is skipped.
CREATE TABLE views.processed_events (
    consumer     text        NOT NULL,
    event_id     uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- Read model built by consumer-balances from EntryPosted events. It can lag
-- behind the ledger; account_balances stays the source of truth.
CREATE TABLE views.balance_view (
    account_id    uuid PRIMARY KEY,
    balance_minor bigint      NOT NULL,
    last_seq      bigint      NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Built by consumer-notify from TransferPosted events: one row for the
-- sender (DEBITED) and one for the receiver (CREDITED).
CREATE TABLE views.notifications (
    id                      bigserial PRIMARY KEY,
    event_id                uuid        NOT NULL,
    transfer_id             uuid        NOT NULL,
    account_id              uuid        NOT NULL,
    kind                    text        NOT NULL CHECK (kind IN ('DEBITED', 'CREDITED')),
    amount_minor            bigint      NOT NULL,
    currency                char(3)     NOT NULL,
    counterparty_account_id uuid        NOT NULL,
    transfer_type           text        NOT NULL,
    memo                    varchar(280) NOT NULL DEFAULT '',
    occurred_at             timestamptz NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (transfer_id, kind)
);

-- +goose Down
DROP SCHEMA views CASCADE;
