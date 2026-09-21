-- Durable receipt of the exact verified webhook bytes. A provider retry can
-- resume processing after a server crash without losing the idempotency claim.
CREATE TABLE IF NOT EXISTS billing_webhook_inbox (
    provider TEXT NOT NULL,
    webhook_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload BYTEA NOT NULL,
    payload_hash BYTEA NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (provider, webhook_id)
);

CREATE INDEX IF NOT EXISTS billing_webhook_inbox_pending_idx
    ON billing_webhook_inbox (state, received_at);
