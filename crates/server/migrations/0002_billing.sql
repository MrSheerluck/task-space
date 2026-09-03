-- Server-owned entitlements. Provider ids are references only; access is
-- decided from this table, never from browser-supplied plan flags.
CREATE TABLE IF NOT EXISTS billing_entitlements (
    account_id TEXT PRIMARY KEY,
    plan TEXT NOT NULL,
    status TEXT NOT NULL,
    sync_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    max_spaces INTEGER NOT NULL,
    provider TEXT,
    provider_customer_id TEXT,
    provider_subscription_id TEXT,
    current_period_end BIGINT,
    cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 0,
    last_event_at BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Provider event ids are the idempotency key. Keeping a compact audit row
-- prevents retries from applying the same payment transition twice.
CREATE TABLE IF NOT EXISTS billing_events (
    provider TEXT NOT NULL,
    provider_event_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    occurred_at BIGINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_hash BYTEA NOT NULL,
    PRIMARY KEY (provider, provider_event_id)
);

CREATE INDEX IF NOT EXISTS billing_events_account_idx
    ON billing_events (account_id, occurred_at DESC);
