-- Keep the database bound aligned with the application snapshot limit. The
-- conditional DO block makes this migration safe for databases that already
-- contain a constraint created manually during an incident.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'crdt_documents_snapshot_bounded'
    ) THEN
        ALTER TABLE crdt_documents
            ADD CONSTRAINT crdt_documents_snapshot_bounded
            CHECK (octet_length(snapshot) <= 8388608);
    END IF;
END
$$;
