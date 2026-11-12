-- +goose Up

-- All timestamps are timestamptz (stored in UTC). All money is BIGINT minor units (paise).

CREATE TABLE accounts (
    id         uuid PRIMARY KEY,
    name       varchar(100) NOT NULL CHECK (length(btrim(name)) > 0),
    currency   char(3)      NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status     text         NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    kind       text         NOT NULL DEFAULT 'USER'   CHECK (kind IN ('USER', 'SYSTEM')),
    created_at timestamptz  NOT NULL DEFAULT now()
);
-- One SYSTEM account per currency: the source of deposits, and the only kind allowed to go negative.
CREATE UNIQUE INDEX accounts_one_system_per_currency ON accounts (currency) WHERE kind = 'SYSTEM';

CREATE TABLE transfers (
    id                   uuid PRIMARY KEY,
    client_id            varchar(64)  NOT NULL,
    idempotency_key      varchar(64)  NOT NULL,
    from_account_id      uuid         NOT NULL REFERENCES accounts (id),
    to_account_id        uuid         NOT NULL REFERENCES accounts (id),
    amount_minor         bigint       NOT NULL CHECK (amount_minor > 0),
    currency             char(3)      NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    memo                 varchar(280) NOT NULL DEFAULT '',
    type                 text         NOT NULL CHECK (type IN ('TRANSFER', 'DEPOSIT', 'REVERSAL')),
    -- UNIQUE: a transfer can be reversed only once.
    reverses_transfer_id uuid         NULL UNIQUE REFERENCES transfers (id),
    created_at           timestamptz  NOT NULL DEFAULT now(),
    CHECK (from_account_id <> to_account_id),
    CHECK ((type = 'REVERSAL') = (reverses_transfer_id IS NOT NULL))
);
-- Not unique: a key may be used again once it has expired from idempotency_keys.
CREATE INDEX transfers_client_key_idx ON transfers (client_id, idempotency_key);

CREATE TABLE entries (
    id           uuid PRIMARY KEY,
    transfer_id  uuid        NOT NULL REFERENCES transfers (id),
    account_id   uuid        NOT NULL REFERENCES accounts (id),
    direction    text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor bigint      NOT NULL CHECK (amount_minor > 0),
    -- Per-account sequence 1, 2, 3 ... ; consumers use it to detect gaps.
    account_seq  bigint      NOT NULL CHECK (account_seq > 0),
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, account_seq),
    -- One DEBIT and one CREDIT per transfer at most.
    UNIQUE (transfer_id, direction)
);

-- +goose StatementBegin
CREATE FUNCTION entries_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'entries is append-only: % is not allowed', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CREATE TRIGGER entries_no_update_delete
    BEFORE UPDATE OR DELETE ON entries
    FOR EACH ROW EXECUTE FUNCTION entries_append_only();

CREATE TRIGGER entries_no_truncate
    BEFORE TRUNCATE ON entries
    FOR EACH STATEMENT EXECUTE FUNCTION entries_append_only();

-- Projection of entries, updated in the same transaction that writes them.
CREATE TABLE account_balances (
    account_id    uuid PRIMARY KEY REFERENCES accounts (id),
    balance_minor bigint      NOT NULL DEFAULT 0,
    last_seq      bigint      NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_keys (
    client_id       varchar(64) NOT NULL,
    key             varchar(64) NOT NULL,
    request_hash    char(64)    NOT NULL,
    response_status int         NULL,
    response_body   jsonb       NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    PRIMARY KEY (client_id, key)
);
CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);

-- Transactional outbox. Created now, filled from Phase 2.
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    event_id     uuid        NOT NULL UNIQUE,
    aggregate_id uuid        NOT NULL,
    event_type   text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz NULL
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
DROP TABLE idempotency_keys;
DROP TABLE account_balances;
DROP TABLE entries;
DROP FUNCTION entries_append_only();
DROP TABLE transfers;
DROP TABLE accounts;
