-- Block-shaped source table: the unit Notion's own data model is built on.
CREATE TABLE blocks (
    id           uuid PRIMARY KEY,
    workspace_id text        NOT NULL,
    parent_id    uuid,
    type         text        NOT NULL,
    content      text,
    version      bigint      NOT NULL DEFAULT 1,
    is_deleted   boolean     NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX blocks_workspace_idx ON blocks (workspace_id);

-- REPLICA IDENTITY FULL puts the pre-image in the WAL, so a delete arrives with
-- its row rather than a bare key. Without it the delete branch of the MERGE has
-- nothing to match on.
ALTER TABLE blocks REPLICA IDENTITY FULL;

CREATE PUBLICATION dbz_pub FOR TABLE blocks;
