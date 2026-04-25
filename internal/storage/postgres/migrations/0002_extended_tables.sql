-- Postgres Backend for Beads — Extended Tables (Migration 0002)
--
-- Adds the remaining 19 Beads tables that were marked as TODO at the bottom
-- of 0001_init.sql. Mirrors Dolt migrations 0006-0032 with the same pattern
-- decisions established in 0001_init.sql:
--
--   - "rig" column where the table is rig-scoped (most tables); omitted for
--     genuinely machine-local tables (local_metadata)
--   - TIMESTAMPTZ throughout, BOOLEAN for flags, JSONB for free-form data
--   - Soft-deletes via deleted_at where applicable
--   - REPLICA IDENTITY FULL on tables intended for ElectricSQL replication
--   - UUID v7 for synthetic primary keys (replaces MySQL CHAR(36) DEFAULT uuid())
--
-- Plus: ready_issues + blocked_issues views are updated to incorporate
-- custom_statuses (mirrors Dolt migrations 0025+0026).

BEGIN;

SET search_path TO beads, public;

INSERT INTO beads.schema_migrations (version) VALUES (2);

-- ════════════════════════════════════════════════════════════════════════
-- Config tables (rig-scoped key-value stores)
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE beads.config (
    rig         VARCHAR(64)  NOT NULL,
    key         VARCHAR(255) NOT NULL,
    value       TEXT         NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rig, key)
);
ALTER TABLE beads.config REPLICA IDENTITY FULL;
CREATE TRIGGER tr_config_updated_at BEFORE UPDATE ON beads.config
    FOR EACH ROW EXECUTE FUNCTION beads.fn_set_updated_at();

CREATE TABLE beads.metadata (
    rig         VARCHAR(64)  NOT NULL,
    key         VARCHAR(255) NOT NULL,
    value       TEXT         NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rig, key)
);
ALTER TABLE beads.metadata REPLICA IDENTITY FULL;
CREATE TRIGGER tr_metadata_updated_at BEFORE UPDATE ON beads.metadata
    FOR EACH ROW EXECUTE FUNCTION beads.fn_set_updated_at();

-- local_metadata is machine-local, NOT replicated. Hence no rig column,
-- no REPLICA IDENTITY FULL, and stays simple key-value.
CREATE TABLE beads.local_metadata (
    key         VARCHAR(255) NOT NULL,
    value       TEXT         NOT NULL DEFAULT '',
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (key)
);
CREATE TRIGGER tr_local_metadata_updated_at BEFORE UPDATE ON beads.local_metadata
    FOR EACH ROW EXECUTE FUNCTION beads.fn_set_updated_at();

-- ════════════════════════════════════════════════════════════════════════
-- ID counters
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE beads.child_counters (
    rig         VARCHAR(64)  NOT NULL,
    parent_id   VARCHAR(255) NOT NULL,
    last_child  INTEGER      NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rig, parent_id)
);
ALTER TABLE beads.child_counters REPLICA IDENTITY FULL;
CREATE TRIGGER tr_child_counters_updated_at BEFORE UPDATE ON beads.child_counters
    FOR EACH ROW EXECUTE FUNCTION beads.fn_set_updated_at();

CREATE TABLE beads.issue_counter (
    rig         VARCHAR(64)  NOT NULL,
    prefix      VARCHAR(255) NOT NULL,
    last_id     INTEGER      NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rig, prefix)
);
ALTER TABLE beads.issue_counter REPLICA IDENTITY FULL;
CREATE TRIGGER tr_issue_counter_updated_at BEFORE UPDATE ON beads.issue_counter
    FOR EACH ROW EXECUTE FUNCTION beads.fn_set_updated_at();

-- ════════════════════════════════════════════════════════════════════════
-- Compaction / snapshots
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE beads.issue_snapshots (
    id                UUID         NOT NULL DEFAULT beads.uuidv7() PRIMARY KEY,
    issue_id          VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    rig               VARCHAR(64)  NOT NULL,
    snapshot_time     TIMESTAMPTZ  NOT NULL,
    compaction_level  INTEGER      NOT NULL,
    original_size     INTEGER      NOT NULL,
    compressed_size   INTEGER      NOT NULL,
    original_content  TEXT         NOT NULL,
    archived_events   TEXT,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
ALTER TABLE beads.issue_snapshots REPLICA IDENTITY FULL;
CREATE INDEX idx_snapshots_rig_issue ON beads.issue_snapshots (rig, issue_id);
CREATE INDEX idx_snapshots_level     ON beads.issue_snapshots (compaction_level);

CREATE TABLE beads.compaction_snapshots (
    id                UUID         NOT NULL DEFAULT beads.uuidv7() PRIMARY KEY,
    issue_id          VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    rig               VARCHAR(64)  NOT NULL,
    compaction_level  INTEGER      NOT NULL,
    snapshot_json     JSONB        NOT NULL,  -- was BLOB in MySQL — JSONB is more useful
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
ALTER TABLE beads.compaction_snapshots REPLICA IDENTITY FULL;
CREATE INDEX idx_comp_snap_rig_issue ON beads.compaction_snapshots (rig, issue_id, compaction_level, created_at);

-- ════════════════════════════════════════════════════════════════════════
-- Repo mtime tracking
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE beads.repo_mtimes (
    rig           VARCHAR(64)  NOT NULL,
    repo_path     VARCHAR(512) NOT NULL,
    jsonl_path    VARCHAR(512) NOT NULL,
    mtime_ns      BIGINT       NOT NULL,
    last_checked  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rig, repo_path)
);
ALTER TABLE beads.repo_mtimes REPLICA IDENTITY FULL;
CREATE INDEX idx_repo_mtimes_checked ON beads.repo_mtimes (last_checked);

-- ════════════════════════════════════════════════════════════════════════
-- Federation
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE beads.routes (
    rig         VARCHAR(64)  NOT NULL,
    prefix      VARCHAR(32)  NOT NULL,
    path        VARCHAR(512) NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rig, prefix)
);
ALTER TABLE beads.routes REPLICA IDENTITY FULL;
CREATE TRIGGER tr_routes_updated_at BEFORE UPDATE ON beads.routes
    FOR EACH ROW EXECUTE FUNCTION beads.fn_set_updated_at();

CREATE TABLE beads.federation_peers (
    rig                 VARCHAR(64)   NOT NULL,
    name                VARCHAR(255)  NOT NULL,
    remote_url          VARCHAR(1024) NOT NULL,
    username            VARCHAR(255),
    password_encrypted  BYTEA,
    sovereignty         VARCHAR(8)    NOT NULL DEFAULT '',
    last_sync           TIMESTAMPTZ,
    deleted_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rig, name)
);
ALTER TABLE beads.federation_peers REPLICA IDENTITY FULL;
CREATE TRIGGER tr_federation_peers_updated_at BEFORE UPDATE ON beads.federation_peers
    FOR EACH ROW EXECUTE FUNCTION beads.fn_set_updated_at();
CREATE INDEX idx_federation_peers_sovereignty ON beads.federation_peers (sovereignty) WHERE deleted_at IS NULL;

-- ════════════════════════════════════════════════════════════════════════
-- Agent interactions log
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE beads.interactions (
    id          VARCHAR(32)  NOT NULL PRIMARY KEY,
    rig         VARCHAR(64)  NOT NULL,
    kind        VARCHAR(64)  NOT NULL,
    actor       VARCHAR(255),
    issue_id    VARCHAR(255),
    model       VARCHAR(255),
    prompt      TEXT,
    response    TEXT,
    error       TEXT,
    tool_name   VARCHAR(255),
    exit_code   INTEGER,
    parent_id   VARCHAR(32),
    label       VARCHAR(64),
    reason      TEXT,
    extra       JSONB,
    created_at  TIMESTAMPTZ  NOT NULL
);
ALTER TABLE beads.interactions REPLICA IDENTITY FULL;
CREATE INDEX idx_interactions_rig_created_at ON beads.interactions (rig, created_at);
CREATE INDEX idx_interactions_rig_issue_id   ON beads.interactions (rig, issue_id) WHERE issue_id IS NOT NULL;
CREATE INDEX idx_interactions_kind           ON beads.interactions (kind);
CREATE INDEX idx_interactions_parent_id      ON beads.interactions (parent_id) WHERE parent_id IS NOT NULL;

-- ════════════════════════════════════════════════════════════════════════
-- Wisps (ephemeral helper issues — separate table from issues by design)
-- ════════════════════════════════════════════════════════════════════════
-- Wisps mirror issues structurally but live in their own table because
-- they have different lifecycle/retention rules. The schema is largely
-- identical to issues; we apply the same pattern (rig, uuid, soft-delete,
-- REPLICA IDENTITY FULL).

CREATE TABLE beads.wisps (
    id                    VARCHAR(255) PRIMARY KEY,
    rig                   VARCHAR(64)  NOT NULL,
    uuid                  UUID         NOT NULL DEFAULT beads.uuidv7() UNIQUE,
    content_hash          VARCHAR(64),
    external_ref          VARCHAR(255),
    title                 VARCHAR(500) NOT NULL,
    description           TEXT NOT NULL DEFAULT '',
    design                TEXT NOT NULL DEFAULT '',
    acceptance_criteria   TEXT NOT NULL DEFAULT '',
    notes                 TEXT NOT NULL DEFAULT '',
    status                VARCHAR(32) NOT NULL DEFAULT 'open',
    priority              INTEGER NOT NULL DEFAULT 2,
    issue_type            VARCHAR(32) NOT NULL DEFAULT 'task',
    assignee              VARCHAR(255),
    created_by            VARCHAR(255) NOT NULL DEFAULT '',
    owner                 VARCHAR(255) NOT NULL DEFAULT '',
    sender                VARCHAR(255) NOT NULL DEFAULT '',
    estimated_minutes     INTEGER,
    started_at            TIMESTAMPTZ,
    closed_at             TIMESTAMPTZ,
    closed_by_session     VARCHAR(255) NOT NULL DEFAULT '',
    due_at                TIMESTAMPTZ,
    defer_until           TIMESTAMPTZ,
    compaction_level      INTEGER NOT NULL DEFAULT 0,
    compacted_at          TIMESTAMPTZ,
    compacted_at_commit   VARCHAR(64),
    original_size         INTEGER,
    spec_id               VARCHAR(1024),
    source_system         VARCHAR(255) NOT NULL DEFAULT '',
    source_repo           VARCHAR(512) NOT NULL DEFAULT '',
    ephemeral             BOOLEAN NOT NULL DEFAULT FALSE,
    no_history            BOOLEAN NOT NULL DEFAULT FALSE,
    pinned                BOOLEAN NOT NULL DEFAULT FALSE,
    is_template           BOOLEAN NOT NULL DEFAULT FALSE,
    wisp_type             VARCHAR(32) NOT NULL DEFAULT '',
    mol_type              VARCHAR(32) NOT NULL DEFAULT '',
    work_type             VARCHAR(32) NOT NULL DEFAULT 'mutex',
    close_reason          TEXT NOT NULL DEFAULT '',
    event_kind            VARCHAR(32) NOT NULL DEFAULT '',
    actor                 VARCHAR(255) NOT NULL DEFAULT '',
    target                VARCHAR(255) NOT NULL DEFAULT '',
    payload               TEXT NOT NULL DEFAULT '',
    await_type            VARCHAR(32) NOT NULL DEFAULT '',
    await_id              VARCHAR(255) NOT NULL DEFAULT '',
    timeout_ns            BIGINT NOT NULL DEFAULT 0,
    waiters               TEXT NOT NULL DEFAULT '',
    hook_bead             VARCHAR(255) NOT NULL DEFAULT '',
    role_bead             VARCHAR(255) NOT NULL DEFAULT '',
    agent_state           VARCHAR(32) NOT NULL DEFAULT '',
    last_activity         TIMESTAMPTZ,
    role_type             VARCHAR(32) NOT NULL DEFAULT '',
    rig_field             VARCHAR(255) NOT NULL DEFAULT '',
    metadata              JSONB NOT NULL DEFAULT '{}'::JSONB,
    deleted_at            TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE beads.wisps REPLICA IDENTITY FULL;
CREATE TRIGGER tr_wisps_updated_at BEFORE UPDATE ON beads.wisps
    FOR EACH ROW EXECUTE FUNCTION beads.fn_set_updated_at();
CREATE INDEX idx_wisps_rig_status     ON beads.wisps (rig, status) WHERE deleted_at IS NULL;
CREATE INDEX idx_wisps_rig_assignee   ON beads.wisps (rig, assignee) WHERE deleted_at IS NULL;
CREATE INDEX idx_wisps_metadata_gin   ON beads.wisps USING GIN (metadata);
CREATE INDEX idx_wisps_uuid           ON beads.wisps (uuid);

CREATE TABLE beads.wisp_comments (
    id          UUID         NOT NULL DEFAULT beads.uuidv7() PRIMARY KEY,
    issue_id    VARCHAR(255) NOT NULL REFERENCES beads.wisps(id) ON DELETE CASCADE,
    rig         VARCHAR(64)  NOT NULL,
    author      VARCHAR(255) NOT NULL DEFAULT '',
    text        TEXT         NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
ALTER TABLE beads.wisp_comments REPLICA IDENTITY FULL;
CREATE INDEX idx_wisp_comments_rig_issue ON beads.wisp_comments (rig, issue_id) WHERE deleted_at IS NULL;

CREATE TABLE beads.wisp_dependencies (
    issue_id        VARCHAR(255) NOT NULL REFERENCES beads.wisps(id) ON DELETE CASCADE,
    depends_on_id   VARCHAR(255) NOT NULL REFERENCES beads.wisps(id) ON DELETE CASCADE,
    rig             VARCHAR(64)  NOT NULL,
    type            VARCHAR(32)  NOT NULL DEFAULT 'blocks',
    created_by      VARCHAR(255) NOT NULL DEFAULT '',
    metadata        JSONB        NOT NULL DEFAULT '{}'::JSONB,
    thread_id       VARCHAR(255) NOT NULL DEFAULT '',
    deleted_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (issue_id, depends_on_id)
);
ALTER TABLE beads.wisp_dependencies REPLICA IDENTITY FULL;
CREATE INDEX idx_wisp_dep_rig_depends      ON beads.wisp_dependencies (rig, depends_on_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_wisp_dep_type             ON beads.wisp_dependencies (type) WHERE deleted_at IS NULL;
CREATE INDEX idx_wisp_dep_type_depends     ON beads.wisp_dependencies (type, depends_on_id) WHERE deleted_at IS NULL;

CREATE TABLE beads.wisp_events (
    id          UUID         NOT NULL DEFAULT beads.uuidv7() PRIMARY KEY,
    issue_id    VARCHAR(255) NOT NULL REFERENCES beads.wisps(id) ON DELETE CASCADE,
    rig         VARCHAR(64)  NOT NULL,
    event_type  VARCHAR(32)  NOT NULL,
    actor       VARCHAR(255) NOT NULL DEFAULT '',
    old_value   TEXT         DEFAULT '',
    new_value   TEXT         DEFAULT '',
    comment     TEXT         DEFAULT '',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
ALTER TABLE beads.wisp_events REPLICA IDENTITY FULL;
CREATE INDEX idx_wisp_events_rig_created_at ON beads.wisp_events (rig, created_at);
CREATE INDEX idx_wisp_events_rig_issue      ON beads.wisp_events (rig, issue_id);

CREATE TABLE beads.wisp_labels (
    issue_id    VARCHAR(255) NOT NULL REFERENCES beads.wisps(id) ON DELETE CASCADE,
    rig         VARCHAR(64)  NOT NULL,
    label       VARCHAR(255) NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (issue_id, label)
);
ALTER TABLE beads.wisp_labels REPLICA IDENTITY FULL;
CREATE INDEX idx_wisp_labels_rig_label ON beads.wisp_labels (rig, label) WHERE deleted_at IS NULL;

-- ════════════════════════════════════════════════════════════════════════
-- Custom statuses + types (user-defined extensions to default workflow)
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE beads.custom_statuses (
    rig         VARCHAR(64) NOT NULL,
    name        VARCHAR(64) NOT NULL,
    category    VARCHAR(32) NOT NULL DEFAULT 'unspecified',  -- 'open' | 'done' | 'frozen' | 'unspecified'
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rig, name)
);
ALTER TABLE beads.custom_statuses REPLICA IDENTITY FULL;
CREATE INDEX idx_custom_statuses_rig_category ON beads.custom_statuses (rig, category) WHERE deleted_at IS NULL;

CREATE TABLE beads.custom_types (
    rig         VARCHAR(64) NOT NULL,
    name        VARCHAR(64) NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rig, name)
);
ALTER TABLE beads.custom_types REPLICA IDENTITY FULL;

-- ════════════════════════════════════════════════════════════════════════
-- Solartown-specific: __restart_integrity__
-- ════════════════════════════════════════════════════════════════════════
-- This table is Solartown-specific (used by restart-integrity test scripts).
-- It conceptually belongs in the solartown.* schema rather than beads.*, but
-- is kept here for now to maintain 1:1 schema parity with current Dolt-state.
-- Move to solartown.* schema in a follow-up migration.

CREATE TABLE beads.__restart_integrity__ (
    test_id      VARCHAR(64) NOT NULL,
    marker       INTEGER     NOT NULL,
    inserted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (test_id, marker)
);

-- ════════════════════════════════════════════════════════════════════════
-- View updates: ready_issues + blocked_issues with custom_statuses awareness
-- (Mirrors Dolt migrations 0025 + 0026)
-- ════════════════════════════════════════════════════════════════════════
-- Views need to know which custom statuses are 'done' or 'frozen' (closed-like)
-- so they exclude blocking by issues that are effectively done in a custom way.

DROP VIEW IF EXISTS beads.ready_issues;
CREATE OR REPLACE VIEW beads.ready_issues AS
WITH RECURSIVE
  closed_status_names AS (
    -- Built-in closed statuses + custom_statuses with category 'done'/'frozen'
    SELECT 'closed' AS name, '' AS rig WHERE TRUE
    UNION ALL
    SELECT 'pinned' AS name, '' AS rig WHERE TRUE
    UNION ALL
    SELECT name, rig FROM beads.custom_statuses
    WHERE category IN ('done', 'frozen')
      AND deleted_at IS NULL
  ),
  blocked_directly AS (
    SELECT DISTINCT d.issue_id, d.rig
    FROM beads.dependencies d
    WHERE d.type = 'blocks'
      AND d.deleted_at IS NULL
      AND EXISTS (
        SELECT 1 FROM beads.issues blocker
        LEFT JOIN closed_status_names csn
          ON csn.name = blocker.status
         AND (csn.rig = '' OR csn.rig = blocker.rig)
        WHERE blocker.id = d.depends_on_id
          AND blocker.deleted_at IS NULL
          AND csn.name IS NULL  -- blocker is NOT in any closed-like status
      )
  ),
  blocked_transitively AS (
    SELECT issue_id, rig, 0 AS depth
    FROM blocked_directly
    UNION ALL
    SELECT d.issue_id, d.rig, bt.depth + 1
    FROM blocked_transitively bt
    JOIN beads.dependencies d ON d.depends_on_id = bt.issue_id
    WHERE d.type = 'parent-child'
      AND d.deleted_at IS NULL
      AND bt.depth < 50
  )
SELECT i.*
FROM beads.issues i
LEFT JOIN blocked_transitively bt ON bt.issue_id = i.id
WHERE i.deleted_at IS NULL
  AND i.status = 'open'
  AND i.ephemeral = FALSE
  AND bt.issue_id IS NULL
  AND (i.defer_until IS NULL OR i.defer_until <= NOW())
  AND NOT EXISTS (
    SELECT 1 FROM beads.dependencies d_parent
    JOIN beads.issues parent ON parent.id = d_parent.depends_on_id
    WHERE d_parent.issue_id = i.id
      AND d_parent.type = 'parent-child'
      AND d_parent.deleted_at IS NULL
      AND parent.deleted_at IS NULL
      AND parent.defer_until IS NOT NULL
      AND parent.defer_until > NOW()
  );

DROP VIEW IF EXISTS beads.blocked_issues;
CREATE OR REPLACE VIEW beads.blocked_issues AS
WITH closed_status_names AS (
    SELECT 'closed' AS name, '' AS rig WHERE TRUE
    UNION ALL
    SELECT 'pinned' AS name, '' AS rig WHERE TRUE
    UNION ALL
    SELECT name, rig FROM beads.custom_statuses
    WHERE category IN ('done', 'frozen')
      AND deleted_at IS NULL
)
SELECT
    i.*,
    (SELECT COUNT(*)
     FROM beads.dependencies d
     WHERE d.issue_id = i.id
       AND d.type = 'blocks'
       AND d.deleted_at IS NULL
       AND EXISTS (
         SELECT 1 FROM beads.issues blocker
         LEFT JOIN closed_status_names csn
           ON csn.name = blocker.status
          AND (csn.rig = '' OR csn.rig = blocker.rig)
         WHERE blocker.id = d.depends_on_id
           AND blocker.deleted_at IS NULL
           AND csn.name IS NULL
       )
    ) AS blocked_by_count
FROM beads.issues i
LEFT JOIN closed_status_names csn_self
  ON csn_self.name = i.status
 AND (csn_self.rig = '' OR csn_self.rig = i.rig)
WHERE i.deleted_at IS NULL
  AND csn_self.name IS NULL  -- self is not in closed-like status
  AND EXISTS (
    SELECT 1 FROM beads.dependencies d
    WHERE d.issue_id = i.id
      AND d.type = 'blocks'
      AND d.deleted_at IS NULL
      AND EXISTS (
        SELECT 1 FROM beads.issues blocker
        LEFT JOIN closed_status_names csn
          ON csn.name = blocker.status
         AND (csn.rig = '' OR csn.rig = blocker.rig)
        WHERE blocker.id = d.depends_on_id
          AND blocker.deleted_at IS NULL
          AND csn.name IS NULL
      )
  );

COMMIT;
