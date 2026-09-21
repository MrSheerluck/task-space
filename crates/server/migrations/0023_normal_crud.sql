-- Simple account-backed CRUD storage. The current deployment is pre-production,
-- so the old CRDT/sync tables can remain for rollback while the application
-- switches to this JSON board representation.

ALTER TABLE spaces
    ADD COLUMN IF NOT EXISTS board_json JSONB NOT NULL DEFAULT
        '{"schema_version":3,"notes":[],"groups":[],"tombstones":[]}'::jsonb;

ALTER TABLE spaces
    ADD COLUMN IF NOT EXISTS board_version BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS spaces_account_active_idx
    ON spaces (account_id, deleted_at, archived, updated_at DESC);
