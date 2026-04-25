package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// CreateIssue inserts a new issue scoped to the store's rig. The actor is
// recorded in the audit-trigger via session-local "beads.actor".
func (s *PostgresStore) CreateIssue(ctx context.Context, issue *types.Issue, actor string) error {
	if issue == nil {
		return errors.New("postgres: CreateIssue: nil issue")
	}

	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		if err := ensureIssueID(ctx, tx, s.rig, "issues", issue, actor); err != nil {
			return err
		}
		metadata := normalizeMetadata(issue.Metadata)
		const q = `
			INSERT INTO beads.issues (
				id, rig, title, description, design, acceptance_criteria, notes,
				status, priority, issue_type,
				assignee, created_by, owner,
				estimated_minutes, started_at, due_at, defer_until,
				external_ref, spec_id, source_system,
				metadata
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7,
				$8, $9, $10,
				$11, $12, $13,
				$14, $15, $16, $17,
				$18, $19, $20,
				$21::jsonb
			)
		`
		_, err := tx.ExecContext(ctx, q,
			issue.ID, s.rig, issue.Title, issue.Description, issue.Design,
			issue.AcceptanceCriteria, issue.Notes,
			stringOrDefault(string(issue.Status), "open"), issue.Priority,
			stringOrDefault(string(issue.IssueType), "task"),
			nullString(issue.Assignee), issue.CreatedBy, issue.Owner,
			intPtrToNull(issue.EstimatedMinutes),
			timePtrToNull(issue.StartedAt), timePtrToNull(issue.DueAt),
			timePtrToNull(issue.DeferUntil),
			stringPtrToNull(issue.ExternalRef), nullString(issue.SpecID),
			issue.SourceSystem,
			metadata,
		)
		if err != nil {
			return fmt.Errorf("postgres: insert issue: %w", err)
		}
		return nil
	})
}

// GetIssue returns the issue with the given ID, scoped to the store's rig.
// Returns storage.ErrNotFound if the issue does not exist or is soft-deleted.
func (s *PostgresStore) GetIssue(ctx context.Context, id string) (*types.Issue, error) {
	const q = `
		SELECT
			id, title, description, design, acceptance_criteria, notes,
			status, priority, issue_type,
			assignee, created_by, owner,
			estimated_minutes, started_at, closed_at, due_at, defer_until,
			external_ref, spec_id, source_system,
			created_at, updated_at, close_reason, closed_by_session,
			compaction_level, compacted_at, compacted_at_commit, original_size,
			metadata
		FROM beads.issues
		WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
	`
	row := s.db.QueryRowContext(ctx, q, id, s.rig)
	issue, err := scanIssue(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get issue %q: %w", id, err)
	}
	return issue, nil
}

// UpdateIssue applies the supplied updates map to the issue, recording an
// event for each changed field. Returns ErrNotFound if the issue is missing
// or soft-deleted.
//
// Supported keys in updates: title, description, design, acceptance_criteria,
// notes, status, priority, issue_type, assignee, owner, estimated_minutes,
// started_at, due_at, defer_until, metadata. Unknown keys are ignored.
func (s *PostgresStore) UpdateIssue(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
	if len(updates) == 0 {
		return nil
	}

	allowed := map[string]string{
		"title":               "title",
		"description":         "description",
		"design":              "design",
		"acceptance_criteria": "acceptance_criteria",
		"notes":               "notes",
		"status":              "status",
		"priority":            "priority",
		"issue_type":          "issue_type",
		"assignee":            "assignee",
		"owner":               "owner",
		"estimated_minutes":   "estimated_minutes",
		"started_at":          "started_at",
		"due_at":              "due_at",
		"defer_until":         "defer_until",
		"metadata":            "metadata",
	}

	var sets []string
	var args []interface{}
	i := 1
	for key, value := range updates {
		col, ok := allowed[key]
		if !ok {
			continue
		}
		if col == "metadata" {
			sets = append(sets, fmt.Sprintf("%s = $%d::jsonb", col, i))
		} else {
			sets = append(sets, fmt.Sprintf("%s = $%d", col, i))
		}
		args = append(args, value)
		i++
	}
	if len(sets) == 0 {
		return nil
	}

	args = append(args, id, s.rig)
	q := fmt.Sprintf(
		`UPDATE beads.issues SET %s WHERE id = $%d AND rig = $%d AND deleted_at IS NULL`,
		strings.Join(sets, ", "), i, i+1,
	)

	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("postgres: update issue %q: %w", id, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("postgres: update issue %q rows: %w", id, err)
		}
		if n == 0 {
			return storage.ErrNotFound
		}
		return nil
	})
}

// CloseIssue sets status='closed' and records close_reason + closed_by_session.
// If the issue is already closed, returns nil (idempotent).
func (s *PostgresStore) CloseIssue(ctx context.Context, id string, reason string, actor string, session string) error {
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const q = `
			UPDATE beads.issues
			   SET status = 'closed',
			       closed_at = NOW(),
			       close_reason = $1,
			       closed_by_session = $2
			 WHERE id = $3 AND rig = $4 AND deleted_at IS NULL AND status <> 'closed'
		`
		_, err := tx.ExecContext(ctx, q, reason, session, id, s.rig)
		if err != nil {
			return fmt.Errorf("postgres: close issue %q: %w", id, err)
		}
		return nil
	})
}

// DeleteIssue performs a soft-delete (sets deleted_at) so the row remains
// for replication and audit purposes. To purge entirely, use a hard-delete
// admin path (not part of the Storage interface).
func (s *PostgresStore) DeleteIssue(ctx context.Context, id string) error {
	return s.withTx(ctx, "system", func(tx *sql.Tx) error {
		const q = `
			UPDATE beads.issues SET deleted_at = NOW()
			WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
		`
		res, err := tx.ExecContext(ctx, q, id, s.rig)
		if err != nil {
			return fmt.Errorf("postgres: delete issue %q: %w", id, err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return storage.ErrNotFound
		}
		return nil
	})
}

// ════════════════════════════════════════════════════════════════════════
// helpers
// ════════════════════════════════════════════════════════════════════════

// scanIssue reads a row produced by the GetIssue projection into *types.Issue.
type scanner interface {
	Scan(dest ...interface{}) error
}

func scanIssue(row scanner) (*types.Issue, error) {
	var (
		issue              types.Issue
		assignee           sql.NullString
		startedAt          sql.NullTime
		closedAt           sql.NullTime
		dueAt              sql.NullTime
		deferUntil         sql.NullTime
		externalRef        sql.NullString
		specID             sql.NullString
		closeReason        sql.NullString
		closedBySession    sql.NullString
		compactedAt        sql.NullTime
		compactedAtCommit  sql.NullString
		estimatedMinutes   sql.NullInt32
		originalSize       sql.NullInt32
		metadata           []byte
		statusStr          string
		issueTypeStr       string
	)
	err := row.Scan(
		&issue.ID, &issue.Title, &issue.Description, &issue.Design,
		&issue.AcceptanceCriteria, &issue.Notes,
		&statusStr, &issue.Priority, &issueTypeStr,
		&assignee, &issue.CreatedBy, &issue.Owner,
		&estimatedMinutes, &startedAt, &closedAt, &dueAt, &deferUntil,
		&externalRef, &specID, &issue.SourceSystem,
		&issue.CreatedAt, &issue.UpdatedAt, &closeReason, &closedBySession,
		&issue.CompactionLevel, &compactedAt, &compactedAtCommit, &originalSize,
		&metadata,
	)
	if err != nil {
		return nil, err
	}
	issue.Status = types.Status(statusStr)
	issue.IssueType = types.IssueType(issueTypeStr)
	if assignee.Valid {
		issue.Assignee = assignee.String
	}
	if startedAt.Valid {
		t := startedAt.Time
		issue.StartedAt = &t
	}
	if closedAt.Valid {
		t := closedAt.Time
		issue.ClosedAt = &t
	}
	if dueAt.Valid {
		t := dueAt.Time
		issue.DueAt = &t
	}
	if deferUntil.Valid {
		t := deferUntil.Time
		issue.DeferUntil = &t
	}
	if externalRef.Valid {
		s := externalRef.String
		issue.ExternalRef = &s
	}
	if specID.Valid {
		issue.SpecID = specID.String
	}
	if closeReason.Valid {
		issue.CloseReason = closeReason.String
	}
	if closedBySession.Valid {
		issue.ClosedBySession = closedBySession.String
	}
	if compactedAt.Valid {
		t := compactedAt.Time
		issue.CompactedAt = &t
	}
	if compactedAtCommit.Valid {
		s := compactedAtCommit.String
		issue.CompactedAtCommit = &s
	}
	if estimatedMinutes.Valid {
		v := int(estimatedMinutes.Int32)
		issue.EstimatedMinutes = &v
	}
	if originalSize.Valid {
		issue.OriginalSize = int(originalSize.Int32)
	}
	if len(metadata) > 0 {
		issue.Metadata = json.RawMessage(metadata)
	}
	return &issue, nil
}

// normalizeMetadata returns a non-empty JSON value suitable for inserting
// into a JSONB column. Empty/nil input becomes "{}".
func normalizeMetadata(m json.RawMessage) string {
	if len(m) == 0 {
		return "{}"
	}
	return string(m)
}

// stringOrDefault returns def when s is empty, else s.
func stringOrDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// nullString converts a Go string to sql.NullString (NULL when empty).
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// stringPtrToNull converts *string to sql.NullString (NULL when nil or empty).
func stringPtrToNull(s *string) sql.NullString {
	if s == nil || *s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}

// intPtrToNull converts *int to sql.NullInt32 (NULL when nil).
func intPtrToNull(i *int) sql.NullInt32 {
	if i == nil {
		return sql.NullInt32{}
	}
	return sql.NullInt32{Int32: int32(*i), Valid: true}
}

// timePtrToNull converts *time.Time to sql.NullTime (NULL when nil).
func timePtrToNull(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *t, Valid: true}
}
