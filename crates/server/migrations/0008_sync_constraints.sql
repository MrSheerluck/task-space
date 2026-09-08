DO $$
BEGIN
    ALTER TABLE spaces
        ADD CONSTRAINT spaces_account_id_nonempty CHECK (length(trim(account_id)) > 0);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE spaces
        ADD CONSTRAINT spaces_id_positive CHECK (space_id > 0 AND space_id <= 9007199254740991);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE spaces
        ADD CONSTRAINT spaces_name_bounded CHECK (length(name) BETWEEN 1 AND 48);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE crdt_updates
        ADD CONSTRAINT crdt_updates_mutation_bounded CHECK (length(mutation_id) BETWEEN 1 AND 128);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE crdt_updates
        ADD CONSTRAINT crdt_updates_payload_bounded CHECK (octet_length(update) <= 2097152);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
