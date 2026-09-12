-- Canonical UUID identity for spaces. The legacy BIGINT remains as a
-- compatibility alias for existing routes and foreign keys during the
-- staged rollout; new code must preserve and return stable_id.
ALTER TABLE spaces
    ADD COLUMN IF NOT EXISTS stable_id UUID;

-- Existing rows have no client-provided UUID. A deterministic UUIDv5-style
-- value derived from the account and legacy id gives every replica the same
-- identity without renumbering data or depending on a clock.
UPDATE spaces
SET stable_id = md5('task-space:space:' || account_id || ':' || space_id::TEXT)::UUID
WHERE stable_id IS NULL;

ALTER TABLE spaces
    ALTER COLUMN stable_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS spaces_stable_id_idx
    ON spaces (account_id, stable_id);
