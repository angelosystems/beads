package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// AddDependency creates a directional relationship between two issues:
// "issue_id depends on depends_on_id" with the given type (default 'blocks').
// Both issues must exist in the current rig and not be soft-deleted.
//
// If the dependency already exists, returns nil (idempotent).
func (s *PostgresStore) AddDependency(ctx context.Context, dep *types.Dependency, actor string) error {
	if dep == nil {
		return errors.New("postgres: AddDependency: nil dependency")
	}
	if dep.IssueID == "" || dep.DependsOnID == "" {
		return errors.New("postgres: AddDependency: empty issue_id or depends_on_id")
	}

	depType := stringOrDefault(string(dep.Type), "blocks")
	metadata := dep.Metadata
	if metadata == "" {
		metadata = "{}"
	}

	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const q = `
			INSERT INTO beads.dependencies (issue_id, depends_on_id, rig, type, created_by, metadata, thread_id)
			VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
			ON CONFLICT (issue_id, depends_on_id) DO NOTHING
		`
		_, err := tx.ExecContext(ctx, q,
			dep.IssueID, dep.DependsOnID, s.rig, depType,
			actor, metadata, dep.ThreadID,
		)
		if err != nil {
			return fmt.Errorf("postgres: insert dependency: %w", err)
		}
		return nil
	})
}

// RemoveDependency soft-deletes the dependency between two issues. Returns
// nil if the dependency does not exist (idempotent).
func (s *PostgresStore) RemoveDependency(ctx context.Context, issueID, dependsOnID string, actor string) error {
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const q = `
			UPDATE beads.dependencies SET deleted_at = NOW()
			WHERE issue_id = $1 AND depends_on_id = $2 AND rig = $3 AND deleted_at IS NULL
		`
		_, err := tx.ExecContext(ctx, q, issueID, dependsOnID, s.rig)
		if err != nil {
			return fmt.Errorf("postgres: remove dependency: %w", err)
		}
		return nil
	})
}

// GetDependencies returns the issues that issueID depends on (i.e., its
// blockers / parents / etc., the "outgoing" side of dependency edges).
func (s *PostgresStore) GetDependencies(ctx context.Context, issueID string) ([]*types.Issue, error) {
	const q = `
		SELECT
			i.id, i.title, i.description, i.design, i.acceptance_criteria, i.notes,
			i.status, i.priority, i.issue_type,
			i.assignee, i.created_by, i.owner,
			i.estimated_minutes, i.started_at, i.closed_at, i.due_at, i.defer_until,
			i.external_ref, i.spec_id, i.source_system,
			i.created_at, i.updated_at, i.close_reason, i.closed_by_session,
			i.compaction_level, i.compacted_at, i.compacted_at_commit, i.original_size,
			i.metadata
		FROM beads.dependencies d
		JOIN beads.issues i ON i.id = d.depends_on_id
		WHERE d.issue_id = $1
		  AND d.rig = $2
		  AND d.deleted_at IS NULL
		  AND i.deleted_at IS NULL
		ORDER BY i.id
	`
	return s.queryIssues(ctx, q, issueID, s.rig)
}

// GetDependents returns the issues that depend on issueID (i.e., the issues
// that this one blocks, the "incoming" side of dependency edges).
func (s *PostgresStore) GetDependents(ctx context.Context, issueID string) ([]*types.Issue, error) {
	const q = `
		SELECT
			i.id, i.title, i.description, i.design, i.acceptance_criteria, i.notes,
			i.status, i.priority, i.issue_type,
			i.assignee, i.created_by, i.owner,
			i.estimated_minutes, i.started_at, i.closed_at, i.due_at, i.defer_until,
			i.external_ref, i.spec_id, i.source_system,
			i.created_at, i.updated_at, i.close_reason, i.closed_by_session,
			i.compaction_level, i.compacted_at, i.compacted_at_commit, i.original_size,
			i.metadata
		FROM beads.dependencies d
		JOIN beads.issues i ON i.id = d.issue_id
		WHERE d.depends_on_id = $1
		  AND d.rig = $2
		  AND d.deleted_at IS NULL
		  AND i.deleted_at IS NULL
		ORDER BY i.id
	`
	return s.queryIssues(ctx, q, issueID, s.rig)
}

// queryIssues executes a SELECT producing the standard issue projection
// (see scanIssue) and returns the issues. Returns an empty slice if no rows.
func (s *PostgresStore) queryIssues(ctx context.Context, q string, args ...interface{}) ([]*types.Issue, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: query issues: %w", err)
	}
	defer rows.Close()

	var issues []*types.Issue
	for rows.Next() {
		issue, err := scanIssue(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan issue: %w", err)
		}
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}
	return issues, nil
}

// keep storage import alive (used by callers via ErrNotFound / Storage interface)
var _ = storage.ErrNotFound
