-- Keep mutation identity claims longer than the replay/event log. Replay rows
-- may be pruned after the supported event-retention window, but a delayed
-- response or a very old client retry must still be rejected if it reuses an
-- id with different bytes.
CREATE TABLE IF NOT EXISTS sync_mutation_claims (
    account_id TEXT NOT NULL,
    space_id BIGINT NOT NULL,
    mutation_kind TEXT NOT NULL,
    mutation_id TEXT NOT NULL,
    request_hash BYTEA,
    update BYTEA NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, space_id, mutation_kind, mutation_id),
    FOREIGN KEY (account_id, space_id)
        REFERENCES spaces (account_id, space_id)
        ON DELETE CASCADE
);

-- Existing document/metadata claims are copied before the new code starts
-- pruning replay rows. The nullable hash preserves compatibility with legacy
-- push rows that never carried a request hash; their update bytes remain the
-- idempotency comparison.
INSERT INTO sync_mutation_claims
    (account_id, space_id, mutation_kind, mutation_id, request_hash, update, metadata, created_at)
SELECT account_id,
       space_id,
       CASE
           WHEN event_kind = 'metadata' THEN 'metadata'
           WHEN request_hash IS NOT NULL THEN 'reconcile'
           ELSE 'push'
       END,
       mutation_id,
       request_hash,
       update,
       metadata,
       created_at
FROM crdt_updates
ON CONFLICT (account_id, space_id, mutation_kind, mutation_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS sync_mutation_claims_created_at_idx
    ON sync_mutation_claims (created_at);
