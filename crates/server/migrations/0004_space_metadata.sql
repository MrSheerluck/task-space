ALTER TABLE spaces
    ADD COLUMN IF NOT EXISTS metadata_version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE spaces
    ADD COLUMN IF NOT EXISTS last_operation_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS spaces_operation_id_idx
    ON spaces (account_id, last_operation_id)
    WHERE last_operation_id IS NOT NULL;
