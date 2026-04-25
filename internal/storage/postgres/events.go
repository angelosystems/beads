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

// GetEvents returns the audit-trail events for issueID, scoped to the store's
// rig. Events are ordered newest-first (created_at DESC, id DESC). When limit
// is greater than zero, at most that many rows are returned.
func (s *PostgresStore) GetEvents(ctx context.Context, issueID string, limit int) ([]*types.Event, error) {
	q := `
		SELECT id::text, issue_id, event_type, actor, old_value, new_value, comment, created_at
		FROM beads.events
		WHERE issue_id = $1 AND rig = $2
		ORDER BY created_at DESC, id DESC
	`
	args := []interface{}{issueID, s.rig}
	if limit > 0 {
		q += " LIMIT $3"
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: get events: %w", err)
	}
	defer rows.Close()

	return scanEvents(rows)
}

// GetAllEventsSince returns every event in the current rig with
// created_at >= since, ordered chronologically (created_at ASC, id ASC).
func (s *PostgresStore) GetAllEventsSince(ctx context.Context, since time.Time) ([]*types.Event, error) {
	const q = `
		SELECT id::text, issue_id, event_type, actor, old_value, new_value, comment, created_at
		FROM beads.events
		WHERE rig = $1 AND created_at >= $2
		ORDER BY created_at ASC, id ASC
	`
	rows, err := s.db.QueryContext(ctx, q, s.rig, since)
	if err != nil {
		return nil, fmt.Errorf("postgres: get all events since: %w", err)
	}
	defer rows.Close()

	return scanEvents(rows)
}

// ReopenIssue flips a closed issue back to open and clears the close metadata.
// A 'reopened' event row is inserted in the same transaction. Returns
// storage.ErrNotFound if the issue does not exist or is not currently closed.
func (s *PostgresStore) ReopenIssue(ctx context.Context, id string, reason string, actor string) error {
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const updateQ = `
			UPDATE beads.issues
			   SET status = 'open',
			       closed_at = NULL,
			       close_reason = ''
			 WHERE id = $1 AND rig = $2 AND deleted_at IS NULL AND status = 'closed'
		`
		res, err := tx.ExecContext(ctx, updateQ, id, s.rig)
		if err != nil {
			return fmt.Errorf("postgres: reopen issue %q: %w", id, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("postgres: reopen issue %q rows: %w", id, err)
		}
		if n == 0 {
			return storage.ErrNotFound
		}

		const eventQ = `
			INSERT INTO beads.events (issue_id, rig, event_type, actor, comment)
			VALUES ($1, $2, 'reopened', $3, $4)
		`
		if _, err := tx.ExecContext(ctx, eventQ, id, s.rig, actor, nullString(reason)); err != nil {
			return fmt.Errorf("postgres: insert reopen event for %q: %w", id, err)
		}
		return nil
	})
}

// UpdateIssueType changes the issue_type of an issue and records a
// 'type_changed' event with the previous and new values. Returns
// storage.ErrNotFound if the issue does not exist (or is soft-deleted).
func (s *PostgresStore) UpdateIssueType(ctx context.Context, id string, issueType string, actor string) error {
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		var oldType string
		const selectQ = `
			SELECT issue_type FROM beads.issues
			WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
		`
		err := tx.QueryRowContext(ctx, selectQ, id, s.rig).Scan(&oldType)
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("postgres: read issue type %q: %w", id, err)
		}

		const updateQ = `
			UPDATE beads.issues
			   SET issue_type = $1
			 WHERE id = $2 AND rig = $3 AND deleted_at IS NULL
		`
		if _, err := tx.ExecContext(ctx, updateQ, issueType, id, s.rig); err != nil {
			return fmt.Errorf("postgres: update issue type %q: %w", id, err)
		}

		const eventQ = `
			INSERT INTO beads.events (issue_id, rig, event_type, actor, old_value, new_value)
			VALUES ($1, $2, 'type_changed', $3, $4, $5)
		`
		if _, err := tx.ExecContext(ctx, eventQ, id, s.rig, actor, oldType, issueType); err != nil {
			return fmt.Errorf("postgres: insert type_changed event for %q: %w", id, err)
		}
		return nil
	})
}

// scanEvents turns a *sql.Rows produced by the standard event projection into
// a slice of *types.Event.
func scanEvents(rows *sql.Rows) ([]*types.Event, error) {
	var out []*types.Event
	for rows.Next() {
		var (
			ev          types.Event
			eventType   string
			oldValue    sql.NullString
			newValue    sql.NullString
			comment     sql.NullString
			createdAt   sql.NullTime
		)
		if err := rows.Scan(
			&ev.ID, &ev.IssueID, &eventType, &ev.Actor,
			&oldValue, &newValue, &comment, &createdAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: scan event: %w", err)
		}
		ev.EventType = types.EventType(eventType)
		if oldValue.Valid {
			v := oldValue.String
			ev.OldValue = &v
		}
		if newValue.Valid {
			v := newValue.String
			ev.NewValue = &v
		}
		if comment.Valid {
			v := comment.String
			ev.Comment = &v
		}
		if createdAt.Valid {
			ev.CreatedAt = createdAt.Time
		}
		out = append(out, &ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}
	return out, nil
}
