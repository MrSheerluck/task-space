-- Keep entitlement and inbox state machine values closed under migrations.
-- These checks make a malformed provider adapter or manual SQL change fail
-- before it can silently grant or revoke sync access.
DO $$
BEGIN
    ALTER TABLE billing_entitlements
        ADD CONSTRAINT billing_entitlements_account_nonempty
        CHECK (length(trim(account_id)) > 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_entitlements
        ADD CONSTRAINT billing_entitlements_plan_valid
        CHECK (plan IN ('free', 'pro'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_entitlements
        ADD CONSTRAINT billing_entitlements_status_valid
        CHECK (status IN ('free', 'pending', 'active', 'past_due', 'canceled', 'ended'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_entitlements
        ADD CONSTRAINT billing_entitlements_access_mode_valid
        CHECK (access_mode IN (
            'read_write', 'grace_read_write', 'paused_not_entitled',
            'paused_payment', 'paused_dispute', 'paused_expired'
        ));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_entitlements
        ADD CONSTRAINT billing_entitlements_space_limit_nonnegative
        CHECK (max_spaces >= 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_webhook_inbox
        ADD CONSTRAINT billing_webhook_state_valid
        CHECK (state IN ('pending', 'processing', 'processed', 'needs_reconciliation'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
