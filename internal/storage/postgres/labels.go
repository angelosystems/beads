package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/steveyegge/beads/internal/types"
)

// AddLabel attaches a label to an issue. Idempotent: re-adding an existing
// label is a no-op.
func (s *PostgresStore) AddLabel(ctx context.Context, issueID, label, actor string) error {
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const q = `
			INSERT INTO beads.labels (issue_id, rig, label)
			VALUES ($1, $2, $3)
			ON CONFLICT (issue_id, label) DO UPDATE SET deleted_at = NULL
		`
		_, err := tx.ExecContext(ctx, q, issueID, s.rig, label)
		if err != nil {
			return fmt.Errorf("postgres: add label: %w", err)
		}
		return nil
	})
}

// RemoveLabel soft-deletes the label association.
func (s *PostgresStore) RemoveLabel(ctx context.Context, issueID, label, actor string) error {
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const q = `
			UPDATE beads.labels SET deleted_at = NOW()
			WHERE issue_id = $1 AND rig = $2 AND label = $3 AND deleted_at IS NULL
		`
		_, err := tx.ExecContext(ctx, q, issueID, s.rig, label)
		if err != nil {
			return fmt.Errorf("postgres: remove label: %w", err)
		}
		return nil
	})
}

// GetLabels returns the active labels attached to an issue.
func (s *PostgresStore) GetLabels(ctx context.Context, issueID string) ([]string, error) {
	const q = `
		SELECT label FROM beads.labels
		WHERE issue_id = $1 AND rig = $2 AND deleted_at IS NULL
		ORDER BY label
	`
	rows, err := s.db.QueryContext(ctx, q, issueID, s.rig)
	if err != nil {
		return nil, fmt.Errorf("postgres: get labels: %w", err)
	}
	defer rows.Close()

	var labels []string
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			return nil, fmt.Errorf("postgres: scan label: %w", err)
		}
		labels = append(labels, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}
	return labels, nil
}

// GetIssuesByLabel returns all (non-deleted) issues that carry the given label.
func (s *PostgresStore) GetIssuesByLabel(ctx context.Context, label string) ([]*types.Issue, error) {
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
		FROM beads.labels l
		JOIN beads.issues i ON i.id = l.issue_id
		WHERE l.label = $1
		  AND l.rig = $2
		  AND l.deleted_at IS NULL
		  AND i.deleted_at IS NULL
		ORDER BY i.id
	`
	return s.queryIssues(ctx, q, label, s.rig)
}
