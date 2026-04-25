-- Postgres Backend for Beads — Initial Schema
--
-- This migration creates the canonical Beads schema in Postgres, mirroring
-- the end-state of Dolt migrations 0001-0032 (current as of beads v0.63.3).
--
-- Design choices vs. Dolt schema:
--   - Schema "beads" instead of default — Beads tables coexist with other
--     workloads (Vibekanban etc.) in the same Postgres instance
--   - TIMESTAMPTZ everywhere instead of DATETIME — timezone-safe by default
--   - BOOLEAN instead of TINYINT(1) — Postgres-native
--   - JSONB instead of JSON — indexable, faster queries
--   - UUID type for internal IDs (events.id, comments.id) instead of CHAR(36)
--   - VARCHAR(255) issue.id retained — Beads owns its own ID format (e.g. "hq-001")
--   - Soft-delete column (deleted_at TIMESTAMPTZ) on every row-deletable table
--   - REPLICA IDENTITY FULL on tables intended for ElectricSQL replication
--     (Phase 6) — required for logical replication change-tracking
--
-- Forward-compat for ElectricSQL:
--   - Primary keys stable (no auto-increment INT)
--   - All deletes are soft via deleted_at; tombstone-tables for hard-deletes
--   - updated_at maintained via trigger (Postgres lacks ON UPDATE clause)
--
-- Scope of this migration: the 5 core tables + 2 views + helper functions.
-- The remaining 22 tables (config, metadata, custom_statuses, wisps, etc.)
-- are listed as TODO at the bottom and will be added in 0002_*.sql once the
-- core pattern is reviewed and approved.

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- Schema setup
-- ════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS beads;
SET search_path TO beads, public;

-- Required extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid()

-- ════════════════════════════════════════════════════════════════════════
-- Helper functions
-- ════════════════════════════════════════════════════════════════════════

-- updated_at trigger — replaces MySQL's ON UPDATE CURRENT_TIMESTAMP
CREATE OR REPLACE FUNCTION beads.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- UUID v7 — time-sortable UUIDs, replication-safe, mobile-sync-friendly.
-- Postgres 16 does not have native uuidv7(); this is a portable implementation.
-- (When Postgres 17+ ships native uuidv7(), this function can be replaced.)
CREATE OR REPLACE FUNCTION beads.uuidv7()
RETURNS UUID AS $$
DECLARE
    unix_ts_ms BIGINT;
    uuid_bytes BYTEA;
BEGIN
    unix_ts_ms := (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
    -- 48 bits timestamp + 4 bits version (7) + 12 bits random + 2 bits variant + 62 bits random
    uuid_bytes := overlay(
        overlay(
            gen_random_bytes(16)
            placing substring(int8send(unix_ts_ms) from 3 for 6) from 1 for 6
        )
        placing set_byte(set_byte('\x0000'::bytea, 0,
            (get_byte(gen_random_bytes(1), 0) & 15) | 112  -- version 7 in upper nibble
        ), 1, get_byte(gen_random_bytes(1), 0)) from 7 for 2
    );
    -- variant bits (10xxxxxx in byte 9)
    uuid_bytes := set_byte(uuid_bytes, 8,
        (get_byte(uuid_bytes, 8) & 63) | 128
    );
    RETURN encode(uuid_bytes, 'hex')::UUID;
END;
$$ LANGUAGE plpgsql;

-- ════════════════════════════════════════════════════════════════════════
-- Core tables
-- ════════════════════════════════════════════════════════════════════════

-- ----------------------------------------------------------------------
-- issues — the central Beads entity
-- ----------------------------------------------------------------------
CREATE TABLE beads.issues (
    -- Identity
    id                    VARCHAR(255) PRIMARY KEY,  -- Beads format (e.g. "hq-001")
    content_hash          VARCHAR(64),
    external_ref          VARCHAR(255),

    -- Core fields
    title                 VARCHAR(500) NOT NULL,
    description           TEXT NOT NULL DEFAULT '',
    design                TEXT NOT NULL DEFAULT '',
    acceptance_criteria   TEXT NOT NULL DEFAULT '',
    notes                 TEXT NOT NULL DEFAULT '',

    -- Workflow state
    status                VARCHAR(32) NOT NULL DEFAULT 'open',
    priority              INTEGER NOT NULL DEFAULT 2,
    issue_type            VARCHAR(32) NOT NULL DEFAULT 'task',

    -- Assignment / ownership
    assignee              VARCHAR(255),
    created_by            VARCHAR(255) NOT NULL DEFAULT '',
    owner                 VARCHAR(255) NOT NULL DEFAULT '',
    sender                VARCHAR(255) NOT NULL DEFAULT '',

    -- Time tracking
    estimated_minutes     INTEGER,
    started_at            TIMESTAMPTZ,
    closed_at             TIMESTAMPTZ,
    closed_by_session     VARCHAR(255) NOT NULL DEFAULT '',
    due_at                TIMESTAMPTZ,
    defer_until           TIMESTAMPTZ,

    -- Compaction / history
    compaction_level      INTEGER NOT NULL DEFAULT 0,
    compacted_at          TIMESTAMPTZ,
    compacted_at_commit   VARCHAR(64),
    original_size         INTEGER,

    -- Spec / source tracking
    spec_id               VARCHAR(1024),
    source_system         VARCHAR(255) NOT NULL DEFAULT '',
    source_repo           VARCHAR(512) NOT NULL DEFAULT '',

    -- Behavior flags
    ephemeral             BOOLEAN NOT NULL DEFAULT FALSE,
    no_history            BOOLEAN NOT NULL DEFAULT FALSE,
    pinned                BOOLEAN NOT NULL DEFAULT FALSE,
    is_template           BOOLEAN NOT NULL DEFAULT FALSE,

    -- Type modifiers
    wisp_type             VARCHAR(32) NOT NULL DEFAULT '',
    mol_type              VARCHAR(32) NOT NULL DEFAULT '',
    work_type             VARCHAR(32) NOT NULL DEFAULT 'mutex',

    -- Lifecycle / closing
    close_reason          TEXT NOT NULL DEFAULT '',
    event_kind            VARCHAR(32) NOT NULL DEFAULT '',
    actor                 VARCHAR(255) NOT NULL DEFAULT '',
    target                VARCHAR(255) NOT NULL DEFAULT '',
    payload               TEXT NOT NULL DEFAULT '',

    -- Await/wait protocol
    await_type            VARCHAR(32) NOT NULL DEFAULT '',
    await_id              VARCHAR(255) NOT NULL DEFAULT '',
    timeout_ns            BIGINT NOT NULL DEFAULT 0,
    waiters               TEXT NOT NULL DEFAULT '',

    -- Hook / role / agent (Gas Town extensions)
    hook_bead             VARCHAR(255) NOT NULL DEFAULT '',
    role_bead             VARCHAR(255) NOT NULL DEFAULT '',
    agent_state           VARCHAR(32) NOT NULL DEFAULT '',
    last_activity         TIMESTAMPTZ,
    role_type             VARCHAR(32) NOT NULL DEFAULT '',
    rig                   VARCHAR(255) NOT NULL DEFAULT '',

    -- Free-form metadata + soft-delete
    metadata              JSONB NOT NULL DEFAULT '{}'::JSONB,
    deleted_at            TIMESTAMPTZ,  -- soft-delete

    -- Timestamps
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE beads.issues REPLICA IDENTITY FULL;

CREATE TRIGGER tr_issues_updated_at BEFORE UPDATE ON beads.issues
    FOR EACH ROW EXECUTE FUNCTION beads.set_updated_at();

CREATE INDEX idx_issues_status         ON beads.issues (status) WHERE deleted_at IS NULL;
CREATE INDEX idx_issues_priority       ON beads.issues (priority) WHERE deleted_at IS NULL;
CREATE INDEX idx_issues_issue_type     ON beads.issues (issue_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_issues_assignee       ON beads.issues (assignee) WHERE deleted_at IS NULL;
CREATE INDEX idx_issues_created_at     ON beads.issues (created_at);
CREATE INDEX idx_issues_spec_id        ON beads.issues (spec_id) WHERE spec_id IS NOT NULL;
CREATE INDEX idx_issues_external_ref   ON beads.issues (external_ref) WHERE external_ref IS NOT NULL;
CREATE INDEX idx_issues_metadata_gin   ON beads.issues USING GIN (metadata);

-- ----------------------------------------------------------------------
-- dependencies — issue relationships (blocks, parent-child, etc.)
-- ----------------------------------------------------------------------
CREATE TABLE beads.dependencies (
    issue_id        VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    depends_on_id   VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    type            VARCHAR(32)  NOT NULL DEFAULT 'blocks',
    created_by      VARCHAR(255) NOT NULL,
    metadata        JSONB        NOT NULL DEFAULT '{}'::JSONB,
    thread_id       VARCHAR(255) NOT NULL DEFAULT '',
    deleted_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (issue_id, depends_on_id)
);

ALTER TABLE beads.dependencies REPLICA IDENTITY FULL;

CREATE INDEX idx_dependencies_depends_on      ON beads.dependencies (depends_on_id);
CREATE INDEX idx_dependencies_depends_on_type ON beads.dependencies (depends_on_id, type);
CREATE INDEX idx_dependencies_issue           ON beads.dependencies (issue_id);
CREATE INDEX idx_dependencies_thread          ON beads.dependencies (thread_id) WHERE thread_id <> '';

-- ----------------------------------------------------------------------
-- events — fact log of issue changes (Beads' own audit trail)
-- ----------------------------------------------------------------------
CREATE TABLE beads.events (
    id          UUID         NOT NULL DEFAULT beads.uuidv7() PRIMARY KEY,
    issue_id    VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    event_type  VARCHAR(32)  NOT NULL,
    actor       VARCHAR(255) NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    comment     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE beads.events REPLICA IDENTITY FULL;

CREATE INDEX idx_events_created_at  ON beads.events (created_at);
CREATE INDEX idx_events_issue       ON beads.events (issue_id);
CREATE INDEX idx_events_type        ON beads.events (event_type);

-- ----------------------------------------------------------------------
-- comments — human/agent commentary on issues
-- ----------------------------------------------------------------------
CREATE TABLE beads.comments (
    id          UUID         NOT NULL DEFAULT beads.uuidv7() PRIMARY KEY,
    issue_id    VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    author      VARCHAR(255) NOT NULL,
    text        TEXT         NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE beads.comments REPLICA IDENTITY FULL;

CREATE INDEX idx_comments_created_at  ON beads.comments (created_at) WHERE deleted_at IS NULL;
CREATE INDEX idx_comments_issue       ON beads.comments (issue_id) WHERE deleted_at IS NULL;

-- ----------------------------------------------------------------------
-- labels — many-to-many issue tags
-- ----------------------------------------------------------------------
CREATE TABLE beads.labels (
    issue_id    VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    label       VARCHAR(255) NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (issue_id, label)
);

ALTER TABLE beads.labels REPLICA IDENTITY FULL;

CREATE INDEX idx_labels_label ON beads.labels (label) WHERE deleted_at IS NULL;

-- ════════════════════════════════════════════════════════════════════════
-- Views: ready_issues + blocked_issues
-- ════════════════════════════════════════════════════════════════════════

-- ready_issues — issues that are open, non-deferred, and not blocked
-- Mirrors Dolt migration 0017+0025 (with pre-custom-statuses logic; the
-- custom_statuses-aware variant comes when we add the custom_statuses
-- table in 0002).
CREATE OR REPLACE VIEW beads.ready_issues AS
WITH RECURSIVE
  blocked_directly AS (
    SELECT DISTINCT d.issue_id
    FROM beads.dependencies d
    WHERE d.type = 'blocks'
      AND d.deleted_at IS NULL
      AND EXISTS (
        SELECT 1 FROM beads.issues blocker
        WHERE blocker.id = d.depends_on_id
          AND blocker.deleted_at IS NULL
          AND blocker.status NOT IN ('closed', 'pinned')
      )
  ),
  blocked_transitively AS (
    SELECT issue_id, 0 AS depth
    FROM blocked_directly
    UNION ALL
    SELECT d.issue_id, bt.depth + 1
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
  AND (i.ephemeral = FALSE)
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

-- blocked_issues — issues that have unresolved blocking dependencies
-- Mirrors Dolt migration 0018 (without custom_statuses; same caveat as above).
CREATE OR REPLACE VIEW beads.blocked_issues AS
SELECT
    i.*,
    (SELECT COUNT(*)
     FROM beads.dependencies d
     WHERE d.issue_id = i.id
       AND d.type = 'blocks'
       AND d.deleted_at IS NULL
       AND EXISTS (
         SELECT 1 FROM beads.issues blocker
         WHERE blocker.id = d.depends_on_id
           AND blocker.deleted_at IS NULL
           AND blocker.status NOT IN ('closed', 'pinned')
       )
    ) AS blocked_by_count
FROM beads.issues i
WHERE i.deleted_at IS NULL
  AND i.status NOT IN ('closed', 'pinned')
  AND EXISTS (
    SELECT 1 FROM beads.dependencies d
    WHERE d.issue_id = i.id
      AND d.type = 'blocks'
      AND d.deleted_at IS NULL
      AND EXISTS (
        SELECT 1 FROM beads.issues blocker
        WHERE blocker.id = d.depends_on_id
          AND blocker.deleted_at IS NULL
          AND blocker.status NOT IN ('closed', 'pinned')
      )
  );

-- ════════════════════════════════════════════════════════════════════════
-- TODO: remaining 22 tables to add in 0002_*.sql
-- ════════════════════════════════════════════════════════════════════════
-- These mirror Dolt migrations 0006-0032 and will be added in a follow-up
-- migration once this core pattern is reviewed:
--
--   config                  (key-value rig config)
--   metadata                (project-level metadata)
--   local_metadata          (machine-local metadata, NOT replicated)
--   schema_migrations       (migration tracker, replaces Beads' Dolt-side)
--   child_counters          (per-parent ID counter)
--   issue_counter           (global ID counter)
--   issue_snapshots         (compaction snapshots)
--   compaction_snapshots    (history compaction artifacts)
--   repo_mtimes             (repo mtime tracking)
--   routes                  (federation routing table)
--   interactions            (agent-to-agent interaction log)
--   federation_peers        (federation membership)
--   wisps                   (ephemeral helper issues)
--   wisp_comments           (comments on wisps)
--   wisp_dependencies       (dependencies on/from wisps)
--   wisp_events             (events on wisps)
--   wisp_labels             (labels on wisps)
--   custom_statuses         (user-defined status names)
--   custom_types            (user-defined issue types)
--   __restart_integrity__   (Solartown-specific restart marker — may move to solartown schema)
--
-- After 0002 lands, the views ready_issues + blocked_issues need a
-- update to incorporate custom_statuses (mirrors Dolt migrations 0025+0026).

COMMIT;
