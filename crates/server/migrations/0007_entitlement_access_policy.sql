ALTER TABLE billing_entitlements
    ADD COLUMN IF NOT EXISTS access_mode TEXT NOT NULL DEFAULT 'paused_not_entitled';

ALTER TABLE billing_entitlements
    ADD COLUMN IF NOT EXISTS access_until BIGINT;

ALTER TABLE billing_entitlements
    ADD COLUMN IF NOT EXISTS retention_until BIGINT;

ALTER TABLE billing_entitlements
    ADD COLUMN IF NOT EXISTS access_reason TEXT;
