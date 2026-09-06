-- Durable storage for authenticated Yrs synchronization.
-- Account ids are provider-neutral strings (for example, a WorkOS user or
-- organization id). Yrs updates remain opaque binary data to the database.

CREATE TABLE IF NOT EXISTS spaces (
    account_id TEXT NOT NULL,
    space_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (account_id, space_id)
);

CREATE TABLE IF NOT EXISTS crdt_documents (
    account_id TEXT NOT NULL,
    space_id BIGINT NOT NULL,
    snapshot BYTEA NOT NULL,
    snapshot_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, space_id),
    FOREIGN KEY (account_id, space_id)
        REFERENCES spaces (account_id, space_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS crdt_updates (
    event_id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    space_id BIGINT NOT NULL,
    mutation_id TEXT NOT NULL,
    update BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, space_id, mutation_id),
    FOREIGN KEY (account_id, space_id)
        REFERENCES spaces (account_id, space_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS crdt_updates_space_event_idx
    ON crdt_updates (account_id, space_id, event_id);
