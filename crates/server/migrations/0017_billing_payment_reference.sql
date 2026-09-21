ALTER TABLE billing_events
    ADD COLUMN IF NOT EXISTS provider_payment_id TEXT;

CREATE INDEX IF NOT EXISTS billing_events_payment_idx
    ON billing_events (provider, provider_payment_id, received_at DESC)
    WHERE provider_payment_id IS NOT NULL;
