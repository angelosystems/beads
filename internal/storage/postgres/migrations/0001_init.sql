-- Postgres Backend for Beads — Initial Schema (revision 2)
--
-- This migration creates the canonical Beads schema in Postgres, mirroring
-- the end-state of Dolt migrations 0001-0032 (current as of beads v0.63.3),
-- with structural extensions to support a unified beat-system across Beads
-- (Solartown) and Vibekanban (coding tasks).
--
-- Pattern decisions (revision 2):
--
--   1. Multi-rig support via "rig" column
--      Solartown today runs 4 separate Dolt databases as "rigs" (hq,
--      activepieces, angeloos, guild). In Postgres we collapse this to
--      ONE schema "beads" with a "rig VARCHAR(64) NOT NULL" column on
--      every entity table. This enables cross-rig analytics ("all coding
--      tasks across all rigs") while keeping migrations simple. Postgres
--      partitioning by rig can be added later if a single rig grows large.
--
--   2. Dual identity on issues (id + uuid)
--      Beads' "id" stays VARCHAR(255) for human-readable IDs ("hq-001").
--      A new "uuid UUID UNIQUE" column gives every issue a stable,
--      time-sortable, replication-safe identifier suitable for
--      cross-system references and ElectricSQL replication.
--
--   3. Cross-system references via external_refs table
--      A generic "beads.external_refs" table maps Beads issues to
--      external entities (Vibekanban tasks, GitHub issues, etc.) without
--      hard foreign keys across schemas — loosely coupled, replication-
--      friendly, and Vibekanban can join via the UUID side.
--
--   4. Generic audit-trigger pattern
--      A single "beads.fn_audit_changes()" trigger function writes old/new
--      JSONB values + actor (from current_setting('beads.actor', true))
--      into a per-table "<table>_audit" sidecar. Beads' own "events" table
--      remains for business-level audit (status transitions, claims, etc.);
--      the audit trigger is for compliance / replication-safety / forensics.
--
--   5. Forward-compat for ElectricSQL replication
--      - TIMESTAMPTZ everywhere (timezone-safe)
--      - BOOLEAN instead of TINYINT(1)
--      - JSONB instead of JSON (indexable, faster queries)
--      - Soft-deletes via "deleted_at TIMESTAMPTZ NULL" on every row-deletable table
--      - REPLICA IDENTITY FULL on tables intended for ElectricSQL replication
--      - All deletes are soft; tombstone-tables can be added later if hard-delete
--        bookkeeping for replication is needed
--
--   6. schema_migrations included from 0001
--      So the migration runner is bootstrapped from the very first migration.
--
-- Scope of this migration: 5 core tables + 2 views + 1 cross-ref table
-- + 1 audit-sidecar (issues_audit) + helper functions + migration tracker.
-- Remaining 22 tables follow in 0002_*.sql once this pattern is reviewed.

BEGIN;

-- ════════════════════════════════════════════════════════════════════════
-- Schema setup
-- ════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS beads;
SET search_path TO beads, public;

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- gen_random_uuid(), gen_random_bytes()

-- ════════════════════════════════════════════════════════════════════════
-- Migration tracker
-- ════════════════════════════════════════════════════════════════════════

CREATE TABLE beads.schema_migrations (
    version     INTEGER     PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by  VARCHAR(255) NOT NULL DEFAULT current_user
);

-- Self-register this migration (the migration runner does this normally,
-- but for the bootstrap migration we record it inline so a fresh DB has
-- a tracker entry from the start).
INSERT INTO beads.schema_migrations (version) VALUES (1);

-- ════════════════════════════════════════════════════════════════════════
-- Helper functions
-- ════════════════════════════════════════════════════════════════════════

-- updated_at trigger — replaces MySQL's ON UPDATE CURRENT_TIMESTAMP
CREATE OR REPLACE FUNCTION beads.fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- UUID v7 — time-sortable, replication-safe, mobile-sync-friendly.
-- Postgres 16 lacks native uuidv7(); this is a portable implementation.
-- When Postgres 17+ ships native uuidv7(), this function can be replaced
-- without any caller changes (same signature).
-- pgcrypto's gen_random_bytes lives in the beads schema (where we installed
-- the extension). We pin search_path on this function so it resolves the
-- helper regardless of the caller's session search_path. Without this,
-- INSERTs from connections that don't include `beads` in search_path fail
-- with "function gen_random_bytes(integer) does not exist".
CREATE OR REPLACE FUNCTION beads.uuidv7()
RETURNS UUID
SET search_path = beads, pg_catalog
AS $$
DECLARE
    unix_ts_ms BIGINT;
    uuid_bytes BYTEA;
BEGIN
    unix_ts_ms := (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT;
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

-- Generic audit-changes trigger — writes old/new JSONB + actor + change-type
-- into a "<table>_audit" sidecar. Each audited table needs:
--   1. A matching "<table>_audit" table (see beads.issues_audit below)
--   2. A trigger calling this function
--
-- Actor is read from session-local setting "beads.actor" (set by the
-- application layer with `SELECT set_config('beads.actor', 'mayor-1', true)`
-- at transaction start). Falls back to current_user if unset.
CREATE OR REPLACE FUNCTION beads.fn_audit_changes()
RETURNS TRIGGER AS $$
DECLARE
    audit_table TEXT;
    actor       TEXT;
    change_type TEXT;
    old_row     JSONB;
    new_row     JSONB;
BEGIN
    audit_table := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME || '_audit';
    actor := COALESCE(current_setting('beads.actor', true), current_user);

    IF TG_OP = 'INSERT' THEN
        change_type := 'insert';
        old_row := NULL;
        new_row := to_jsonb(NEW);
    ELSIF TG_OP = 'UPDATE' THEN
        change_type := 'update';
        old_row := to_jsonb(OLD);
        new_row := to_jsonb(NEW);
    ELSIF TG_OP = 'DELETE' THEN
        change_type := 'delete';
        old_row := to_jsonb(OLD);
        new_row := NULL;
    END IF;

    EXECUTE format(
        'INSERT INTO %s (change_type, actor, old_row, new_row, changed_at) VALUES ($1, $2, $3, $4, NOW())',
        audit_table
    ) USING change_type, actor, old_row, new_row;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
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
    id                    VARCHAR(255) PRIMARY KEY,  -- Beads format ("hq-001")
    rig                   VARCHAR(64)  NOT NULL,     -- Multi-rig: hq | activepieces | angeloos | guild | ...
    uuid                  UUID         NOT NULL DEFAULT beads.uuidv7() UNIQUE,
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
    rig_field             VARCHAR(255) NOT NULL DEFAULT '',  -- legacy: "rig" string in some Beads workflows
                                                              -- (kept distinct from the multi-rig "rig" column above)

    -- Free-form metadata + soft-delete
    metadata              JSONB NOT NULL DEFAULT '{}'::JSONB,
    deleted_at            TIMESTAMPTZ,

    -- Timestamps
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE beads.issues REPLICA IDENTITY FULL;

CREATE TRIGGER tr_issues_updated_at BEFORE UPDATE ON beads.issues
    FOR EACH ROW EXECUTE FUNCTION beads.fn_set_updated_at();

-- All "active-rows" indexes filter by rig + deleted_at for typical query patterns
CREATE INDEX idx_issues_rig_status      ON beads.issues (rig, status) WHERE deleted_at IS NULL;
CREATE INDEX idx_issues_rig_priority    ON beads.issues (rig, priority) WHERE deleted_at IS NULL;
CREATE INDEX idx_issues_rig_type        ON beads.issues (rig, issue_type) WHERE deleted_at IS NULL;
CREATE INDEX idx_issues_rig_assignee    ON beads.issues (rig, assignee) WHERE deleted_at IS NULL;
CREATE INDEX idx_issues_rig_created_at  ON beads.issues (rig, created_at);
CREATE INDEX idx_issues_spec_id         ON beads.issues (spec_id) WHERE spec_id IS NOT NULL;
CREATE INDEX idx_issues_external_ref    ON beads.issues (external_ref) WHERE external_ref IS NOT NULL;
CREATE INDEX idx_issues_metadata_gin    ON beads.issues USING GIN (metadata);
CREATE INDEX idx_issues_uuid            ON beads.issues (uuid);  -- already UNIQUE-indexed but explicit name

-- ----------------------------------------------------------------------
-- issues_audit — generic audit-sidecar (driven by fn_audit_changes())
-- ----------------------------------------------------------------------
CREATE TABLE beads.issues_audit (
    audit_id    BIGSERIAL    PRIMARY KEY,
    change_type VARCHAR(16)  NOT NULL,  -- insert | update | delete
    actor       VARCHAR(255) NOT NULL,
    old_row     JSONB,
    new_row     JSONB,
    changed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_issues_audit_changed_at ON beads.issues_audit (changed_at);
CREATE INDEX idx_issues_audit_issue_id ON beads.issues_audit
    ((COALESCE(new_row->>'id', old_row->>'id')));
CREATE INDEX idx_issues_audit_rig ON beads.issues_audit
    ((COALESCE(new_row->>'rig', old_row->>'rig')));

CREATE TRIGGER tr_issues_audit
    AFTER INSERT OR UPDATE OR DELETE ON beads.issues
    FOR EACH ROW EXECUTE FUNCTION beads.fn_audit_changes();

-- ----------------------------------------------------------------------
-- dependencies — issue relationships (blocks, parent-child, etc.)
-- ----------------------------------------------------------------------
CREATE TABLE beads.dependencies (
    issue_id        VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    depends_on_id   VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    rig             VARCHAR(64)  NOT NULL,  -- denormalized for query speed
    type            VARCHAR(32)  NOT NULL DEFAULT 'blocks',
    created_by      VARCHAR(255) NOT NULL,
    metadata        JSONB        NOT NULL DEFAULT '{}'::JSONB,
    thread_id       VARCHAR(255) NOT NULL DEFAULT '',
    deleted_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (issue_id, depends_on_id)
);

ALTER TABLE beads.dependencies REPLICA IDENTITY FULL;

CREATE INDEX idx_dependencies_rig_depends_on ON beads.dependencies (rig, depends_on_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_dependencies_rig_issue      ON beads.dependencies (rig, issue_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_dependencies_thread         ON beads.dependencies (thread_id) WHERE thread_id <> '' AND deleted_at IS NULL;

-- ----------------------------------------------------------------------
-- events — fact log of issue changes (Beads' own business audit trail)
-- Distinct from issues_audit (which is row-level technical audit).
-- ----------------------------------------------------------------------
CREATE TABLE beads.events (
    id          UUID         NOT NULL DEFAULT beads.uuidv7() PRIMARY KEY,
    issue_id    VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    rig         VARCHAR(64)  NOT NULL,
    event_type  VARCHAR(32)  NOT NULL,
    actor       VARCHAR(255) NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    comment     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE beads.events REPLICA IDENTITY FULL;

CREATE INDEX idx_events_rig_created_at  ON beads.events (rig, created_at);
CREATE INDEX idx_events_rig_issue       ON beads.events (rig, issue_id);
CREATE INDEX idx_events_type            ON beads.events (event_type);

-- ----------------------------------------------------------------------
-- comments — human/agent commentary on issues
-- ----------------------------------------------------------------------
CREATE TABLE beads.comments (
    id          UUID         NOT NULL DEFAULT beads.uuidv7() PRIMARY KEY,
    issue_id    VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    rig         VARCHAR(64)  NOT NULL,
    author      VARCHAR(255) NOT NULL,
    text        TEXT         NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE beads.comments REPLICA IDENTITY FULL;

CREATE INDEX idx_comments_rig_created_at ON beads.comments (rig, created_at) WHERE deleted_at IS NULL;
CREATE INDEX idx_comments_rig_issue      ON beads.comments (rig, issue_id) WHERE deleted_at IS NULL;

-- ----------------------------------------------------------------------
-- labels — many-to-many issue tags
-- ----------------------------------------------------------------------
CREATE TABLE beads.labels (
    issue_id    VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    rig         VARCHAR(64)  NOT NULL,
    label       VARCHAR(255) NOT NULL,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (issue_id, label)
);

ALTER TABLE beads.labels REPLICA IDENTITY FULL;

CREATE INDEX idx_labels_rig_label ON beads.labels (rig, label) WHERE deleted_at IS NULL;

-- ----------------------------------------------------------------------
-- external_refs — cross-system mapping (Beads issue ↔ Vibekanban task / GitHub
-- issue / Linear issue / etc.) without hard cross-schema FKs.
-- ----------------------------------------------------------------------
CREATE TABLE beads.external_refs (
    id              UUID         NOT NULL DEFAULT beads.uuidv7() PRIMARY KEY,
    issue_id        VARCHAR(255) NOT NULL REFERENCES beads.issues(id) ON DELETE CASCADE,
    issue_uuid      UUID         NOT NULL,  -- denormalized, allows reverse-join from external systems
    rig             VARCHAR(64)  NOT NULL,
    external_system VARCHAR(64)  NOT NULL,  -- 'vibekanban' | 'github' | 'linear' | ...
    external_id     VARCHAR(255) NOT NULL,  -- the ID in the external system
    external_url    TEXT,                   -- optional canonical URL
    metadata        JSONB        NOT NULL DEFAULT '{}'::JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (external_system, external_id)
);

ALTER TABLE beads.external_refs REPLICA IDENTITY FULL;

CREATE INDEX idx_external_refs_issue       ON beads.external_refs (issue_id);
CREATE INDEX idx_external_refs_issue_uuid  ON beads.external_refs (issue_uuid);
CREATE INDEX idx_external_refs_rig_system  ON beads.external_refs (rig, external_system);

-- ════════════════════════════════════════════════════════════════════════
-- Views: ready_issues + blocked_issues
-- ════════════════════════════════════════════════════════════════════════
-- Both views are now rig-aware via i.rig included in SELECT *.
-- Callers filter by rig in WHERE: SELECT * FROM ready_issues WHERE rig = 'hq';
--
-- Note: pre-custom_statuses logic. Once 0002 adds custom_statuses,
-- these views get updated to account for category='done'/'frozen' (mirrors
-- Dolt migrations 0025+0026).

CREATE OR REPLACE VIEW beads.ready_issues AS
WITH RECURSIVE
  blocked_directly AS (
    SELECT DISTINCT d.issue_id, d.rig
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
-- These mirror Dolt migrations 0006-0032 and need the same pattern applied:
--   - rig VARCHAR(64) NOT NULL where applicable
--   - deleted_at TIMESTAMPTZ NULL for soft-delete
--   - REPLICA IDENTITY FULL for replication-tauglich tables
--   - Audit-trigger via fn_audit_changes() where the table holds canonical state
--
-- Tables to add:
--   config                  (key-value rig config) — needs rig
--   metadata                (project-level metadata) — needs rig
--   local_metadata          (machine-local metadata, NOT replicated)
--   child_counters          (per-parent ID counter) — needs rig
--   issue_counter           (global ID counter) — needs rig
--   issue_snapshots         (compaction snapshots) — needs rig
--   compaction_snapshots    (history compaction artifacts) — needs rig
--   repo_mtimes             (repo mtime tracking) — needs rig
--   routes                  (federation routing table) — needs rig
--   interactions            (agent-to-agent interaction log) — needs rig
--   federation_peers        (federation membership) — needs rig
--   wisps                   (ephemeral helper issues) — needs rig
--   wisp_comments           (comments on wisps) — needs rig
--   wisp_dependencies       (dependencies on/from wisps) — needs rig
--   wisp_events             (events on wisps) — needs rig
--   wisp_labels             (labels on wisps) — needs rig
--   custom_statuses         (user-defined status names) — needs rig
--   custom_types            (user-defined issue types) — needs rig
--   __restart_integrity__   (Solartown-specific restart marker — may move
--                            to solartown.* schema instead)
--
-- After 0002 lands, ready_issues + blocked_issues need updates to
-- incorporate custom_statuses (mirrors Dolt migrations 0025+0026).

COMMIT;
