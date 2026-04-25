package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/steveyegge/beads/internal/types"
)

// AddIssueComment appends a comment to an issue. Returns the persisted comment
// with its server-assigned UUID and timestamp.
func (s *PostgresStore) AddIssueComment(ctx context.Context, issueID, author, text string) (*types.Comment, error) {
	var comment types.Comment
	err := s.withTx(ctx, author, func(tx *sql.Tx) error {
		const q = `
			INSERT INTO beads.comments (issue_id, rig, author, text)
			VALUES ($1, $2, $3, $4)
			RETURNING id::text, issue_id, author, text, created_at
		`
		var idStr string
		var createdAt sql.NullTime
		err := tx.QueryRowContext(ctx, q, issueID, s.rig, author, text).Scan(
			&idStr, &comment.IssueID, &comment.Author, &comment.Text, &createdAt,
		)
		if err != nil {
			return fmt.Errorf("postgres: insert comment: %w", err)
		}
		comment.ID = idStr
		if createdAt.Valid {
			comment.CreatedAt = createdAt.Time
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &comment, nil
}

// GetIssueComments returns all non-deleted comments on an issue, oldest first.
func (s *PostgresStore) GetIssueComments(ctx context.Context, issueID string) ([]*types.Comment, error) {
	const q = `
		SELECT id::text, issue_id, author, text, created_at
		FROM beads.comments
		WHERE issue_id = $1 AND rig = $2 AND deleted_at IS NULL
		ORDER BY created_at ASC, id ASC
	`
	rows, err := s.db.QueryContext(ctx, q, issueID, s.rig)
	if err != nil {
		return nil, fmt.Errorf("postgres: get comments: %w", err)
	}
	defer rows.Close()

	var out []*types.Comment
	for rows.Next() {
		var c types.Comment
		var idStr string
		var createdAt sql.NullTime
		if err := rows.Scan(&idStr, &c.IssueID, &c.Author, &c.Text, &createdAt); err != nil {
			return nil, fmt.Errorf("postgres: scan comment: %w", err)
		}
		c.ID = idStr
		if createdAt.Valid {
			c.CreatedAt = createdAt.Time
		}
		out = append(out, &c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}
	return out, nil
}
