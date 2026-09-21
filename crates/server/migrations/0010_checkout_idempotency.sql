-- A durable, short-lived reservation prevents duplicate checkout sessions when
-- a user double-clicks, two tabs race, or the API restarts between the request
-- and the provider response. The provider idempotency key is reused for the
-- whole five-minute request window.
CREATE TABLE IF NOT EXISTS billing_checkout_idempotency (
    account_id TEXT NOT NULL,
    interval TEXT NOT NULL CHECK (interval IN ('month', 'year')),
    bucket BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    checkout_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, interval, bucket)
);

CREATE INDEX IF NOT EXISTS billing_checkout_idempotency_updated_idx
    ON billing_checkout_idempotency (updated_at);
