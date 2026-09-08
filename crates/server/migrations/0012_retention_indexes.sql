CREATE INDEX IF NOT EXISTS sync_events_created_at_idx
    ON sync_events (created_at, account_id, event_id);

CREATE INDEX IF NOT EXISTS crdt_updates_created_at_idx
    ON crdt_updates (created_at, account_id, space_id, event_id);

CREATE INDEX IF NOT EXISTS billing_webhook_inbox_state_received_idx
    ON billing_webhook_inbox (state, received_at);
