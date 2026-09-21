-- Durable account-scoped event log. The CRDT update log remains the source of
-- document replay data, while this table gives SSE a stable event envelope
-- that can later be retained or compacted independently.
CREATE TABLE IF NOT EXISTS sync_events (
    event_id BIGINT PRIMARY KEY,
    account_id TEXT NOT NULL,
    space_id BIGINT NOT NULL,
    event_kind TEXT NOT NULL DEFAULT 'document',
    update BYTEA NOT NULL,
    metadata JSONB,
    entitlement_version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (account_id, space_id)
        REFERENCES spaces (account_id, space_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS sync_events_account_event_idx
    ON sync_events (account_id, event_id);

-- Backfill events that were written before this dedicated envelope existed.
INSERT INTO sync_events (event_id, account_id, space_id, event_kind, update, metadata, created_at)
SELECT event_id, account_id, space_id, COALESCE(event_kind, 'document'), update, metadata, created_at
FROM crdt_updates
ON CONFLICT (event_id) DO NOTHING;
