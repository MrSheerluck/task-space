-- Explicitly record when a durable replacement snapshot was compacted. The
-- snapshot remains the recovery source; this marker is operational evidence
-- that replay history was only pruned after compaction had a durable result.
ALTER TABLE crdt_documents
    ADD COLUMN IF NOT EXISTS compacted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS crdt_documents_compaction_idx
    ON crdt_documents (updated_at, compacted_at, account_id, space_id);
