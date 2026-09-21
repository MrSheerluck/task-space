ALTER TABLE billing_webhook_inbox
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE billing_webhook_inbox
    ADD COLUMN IF NOT EXISTS last_error TEXT;

ALTER TABLE billing_webhook_inbox
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS billing_webhook_inbox_retry_idx
    ON billing_webhook_inbox (state, updated_at)
    WHERE state <> 'processed';
