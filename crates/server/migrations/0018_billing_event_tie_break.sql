ALTER TABLE billing_entitlements
    ADD COLUMN IF NOT EXISTS last_event_id TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
    ALTER TABLE billing_entitlements
        ADD CONSTRAINT billing_entitlements_last_event_id_bounded
        CHECK (length(last_event_id) <= 256);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
