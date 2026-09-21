ALTER TABLE spaces
    ADD COLUMN IF NOT EXISTS last_operation_hash BYTEA;
