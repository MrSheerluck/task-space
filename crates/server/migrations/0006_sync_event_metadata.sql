ALTER TABLE crdt_updates
    ADD COLUMN IF NOT EXISTS metadata JSONB;

ALTER TABLE crdt_updates
    ADD COLUMN IF NOT EXISTS event_kind TEXT NOT NULL DEFAULT 'document';
