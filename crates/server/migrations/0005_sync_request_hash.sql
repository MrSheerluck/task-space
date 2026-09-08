ALTER TABLE crdt_updates
    ADD COLUMN IF NOT EXISTS request_hash BYTEA;
