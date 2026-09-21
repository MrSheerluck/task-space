-- Keep provider identifiers bounded even when a verified provider payload is
-- malformed or unexpectedly verbose. The application validates the same
-- limits before writing, while these checks protect direct SQL and old code.
-- Migration 0018 originally used 128 characters. Keep upgrades from that
-- version compatible with the application/provider identifier bound.
ALTER TABLE billing_entitlements
    DROP CONSTRAINT IF EXISTS billing_entitlements_last_event_id_bounded;

DO $$
BEGIN
    ALTER TABLE billing_events
        ADD CONSTRAINT billing_events_provider_bounded
        CHECK (length(provider) BETWEEN 1 AND 256);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_events
        ADD CONSTRAINT billing_events_payment_id_bounded
        CHECK (provider_payment_id IS NULL OR length(provider_payment_id) <= 256);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_entitlements
        ADD CONSTRAINT billing_entitlements_last_event_id_bounded
        CHECK (length(last_event_id) <= 256);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_events
        ADD CONSTRAINT billing_events_event_id_bounded
        CHECK (length(provider_event_id) BETWEEN 1 AND 256);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_events
        ADD CONSTRAINT billing_events_account_bounded
        CHECK (length(account_id) BETWEEN 1 AND 256);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_webhook_inbox
        ADD CONSTRAINT billing_webhook_provider_bounded
        CHECK (length(provider) BETWEEN 1 AND 256);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_webhook_inbox
        ADD CONSTRAINT billing_webhook_id_bounded
        CHECK (length(webhook_id) BETWEEN 1 AND 256);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_webhook_inbox
        ADD CONSTRAINT billing_webhook_event_type_bounded
        CHECK (length(event_type) BETWEEN 1 AND 256);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE billing_webhook_inbox
        ADD CONSTRAINT billing_webhook_payload_bounded
        CHECK (octet_length(payload) <= 524288);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
