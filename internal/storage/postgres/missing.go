package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// AddComment is the legacy event-shaped comment API used by Dolt's storage
// layer. We implement it on top of AddIssueComment for parity; the comment is
// persisted in beads.comments and audited via the standard event trigger.
func (s *PostgresStore) AddComment(ctx context.Context, issueID, actor, comment string) error {
	_, err := s.AddIssueComment(ctx, issueID, actor, comment)
	return err
}

// ImportIssueComment persists a comment with an explicit createdAt timestamp,
// preserving timestamps across import/export cycles. Used by bd import.
func (s *PostgresStore) ImportIssueComment(ctx context.Context, issueID, author, text string, createdAt time.Time) (*types.Comment, error) {
	var comment types.Comment
	err := s.withTx(ctx, author, func(tx *sql.Tx) error {
		const q = `
			INSERT INTO beads.comments (issue_id, rig, author, text, created_at)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id::text, issue_id, author, text, created_at
		`
		var idStr string
		var ts sql.NullTime
		err := tx.QueryRowContext(ctx, q, issueID, s.rig, author, text, createdAt).Scan(
			&idStr, &comment.IssueID, &comment.Author, &comment.Text, &ts,
		)
		if err != nil {
			return fmt.Errorf("postgres: import comment: %w", err)
		}
		comment.ID = idStr
		if ts.Valid {
			comment.CreatedAt = ts.Time
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &comment, nil
}

// GetDependencyRecords returns the raw dependency rows for an issue, including
// metadata and thread_id, oldest first. This is the lower-level companion to
// GetDependencies (which returns hydrated Issues).
func (s *PostgresStore) GetDependencyRecords(ctx context.Context, issueID string) ([]*types.Dependency, error) {
	const q = `
		SELECT issue_id, depends_on_id, type, created_at, created_by,
		       COALESCE(metadata::text, ''), COALESCE(thread_id, '')
		FROM beads.dependencies
		WHERE issue_id = $1 AND rig = $2 AND deleted_at IS NULL
		ORDER BY created_at ASC
	`
	rows, err := s.db.QueryContext(ctx, q, issueID, s.rig)
	if err != nil {
		return nil, fmt.Errorf("postgres: get dependency records: %w", err)
	}
	defer rows.Close()

	var out []*types.Dependency
	for rows.Next() {
		var d types.Dependency
		var createdAt sql.NullTime
		var metadata, threadID string
		if err := rows.Scan(&d.IssueID, &d.DependsOnID, &d.Type, &createdAt, &d.CreatedBy, &metadata, &threadID); err != nil {
			return nil, fmt.Errorf("postgres: scan dependency: %w", err)
		}
		if createdAt.Valid {
			d.CreatedAt = createdAt.Time
		}
		if metadata != "" {
			d.Metadata = metadata
		}
		if threadID != "" {
			d.ThreadID = threadID
		}
		out = append(out, &d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}
	return out, nil
}

// SetMetadata writes to the rig-scoped metadata table. Distinct from
// SetLocalMetadata (machine-local, not rig-scoped) and SetConfig (operator
// config, also rig-scoped). The metadata table stores import hashes and other
// internal-state key/value pairs.
func (s *PostgresStore) SetMetadata(ctx context.Context, key, value string) error {
	if key == "" {
		return errors.New("postgres: SetMetadata: empty key")
	}
	return s.withTx(ctx, "system", func(tx *sql.Tx) error {
		const q = `
			INSERT INTO beads.metadata (rig, key, value)
			VALUES ($1, $2, $3)
			ON CONFLICT (rig, key) DO UPDATE
			   SET value = EXCLUDED.value,
			       deleted_at = NULL
		`
		_, err := tx.ExecContext(ctx, q, s.rig, key, value)
		if err != nil {
			return fmt.Errorf("postgres: set metadata %q: %w", key, err)
		}
		return nil
	})
}

// GetMetadata reads from the rig-scoped metadata table. Returns empty string
// (not ErrNotFound) when the key is not present, matching the Dolt
// implementation's contract.
func (s *PostgresStore) GetMetadata(ctx context.Context, key string) (string, error) {
	const q = `
		SELECT value FROM beads.metadata
		WHERE rig = $1 AND key = $2 AND deleted_at IS NULL
	`
	var v string
	err := s.db.QueryRowContext(ctx, q, s.rig, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("postgres: get metadata %q: %w", key, err)
	}
	return v, nil
}

// _ ensures the storage import is used; the actual interface assertion lives
// in store.go where it can see all methods on the type.
var _ = storage.ErrNotFound
