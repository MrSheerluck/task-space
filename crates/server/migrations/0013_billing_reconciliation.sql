ALTER TABLE billing_entitlements
    ADD COLUMN IF NOT EXISTS last_reconciled_at TIMESTAMPTZ;

ALTER TABLE billing_entitlements
    ADD COLUMN IF NOT EXISTS reconciliation_error TEXT;

CREATE INDEX IF NOT EXISTS billing_entitlements_reconciliation_idx
    ON billing_entitlements (last_reconciled_at, provider, provider_subscription_id)
    WHERE provider_subscription_id IS NOT NULL;
