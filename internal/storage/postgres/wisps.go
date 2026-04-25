package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// wispProjection is the standard SELECT projection used to scan a wisp row
// into *types.Issue. Mirrors the scanIssue projection on beads.issues, since
// the wisps table is structurally identical to issues for our needs.
const wispProjection = `
	id, title, description, design, acceptance_criteria, notes,
	status, priority, issue_type,
	assignee, created_by, owner,
	estimated_minutes, started_at, closed_at, due_at, defer_until,
	external_ref, spec_id, source_system,
	created_at, updated_at, close_reason, closed_by_session,
	compaction_level, compacted_at, compacted_at_commit, original_size,
	metadata
`

// CreateWisp inserts a new wisp scoped to the store's rig. Mirrors
// CreateIssue but writes to beads.wisps instead of beads.issues.
//
// Note: not part of the Storage interface — internal callers reach this via
// type-assertion on *PostgresStore.
func (s *PostgresStore) CreateWisp(ctx context.Context, wisp *types.Issue, actor string) error {
	if wisp == nil {
		return errors.New("postgres: CreateWisp: nil wisp")
	}

	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		if err := ensureIssueID(ctx, tx, s.rig, "wisps", wisp, actor); err != nil {
			return err
		}
		metadata := normalizeMetadata(wisp.Metadata)
		const q = `
			INSERT INTO beads.wisps (
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
			wisp.ID, s.rig, wisp.Title, wisp.Description, wisp.Design,
			wisp.AcceptanceCriteria, wisp.Notes,
			stringOrDefault(string(wisp.Status), "open"), wisp.Priority,
			stringOrDefault(string(wisp.IssueType), "task"),
			nullString(wisp.Assignee), wisp.CreatedBy, wisp.Owner,
			intPtrToNull(wisp.EstimatedMinutes),
			timePtrToNull(wisp.StartedAt), timePtrToNull(wisp.DueAt),
			timePtrToNull(wisp.DeferUntil),
			stringPtrToNull(wisp.ExternalRef), nullString(wisp.SpecID),
			wisp.SourceSystem,
			metadata,
		)
		if err != nil {
			return fmt.Errorf("postgres: insert wisp: %w", err)
		}
		return nil
	})
}

// GetWisp returns the wisp with the given ID, scoped to the store's rig.
// Returns storage.ErrNotFound if the wisp does not exist or is soft-deleted.
func (s *PostgresStore) GetWisp(ctx context.Context, id string) (*types.Issue, error) {
	q := `
		SELECT ` + wispProjection + `
		FROM beads.wisps
		WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
	`
	row := s.db.QueryRowContext(ctx, q, id, s.rig)
	wisp, err := scanWisp(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get wisp %q: %w", id, err)
	}
	return wisp, nil
}

// UpdateWisp applies the supplied updates map to the wisp. Returns ErrNotFound
// if the wisp is missing or soft-deleted.
//
// Supported keys mirror UpdateIssue's allowed set (the wisps table mirrors the
// issues schema). Unknown keys are ignored.
func (s *PostgresStore) UpdateWisp(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
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
		`UPDATE beads.wisps SET %s WHERE id = $%d AND rig = $%d AND deleted_at IS NULL`,
		strings.Join(sets, ", "), i, i+1,
	)

	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("postgres: update wisp %q: %w", id, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("postgres: update wisp %q rows: %w", id, err)
		}
		if n == 0 {
			return storage.ErrNotFound
		}
		return nil
	})
}

// CloseWisp sets status='closed' and records close_reason + closed_by_session
// on the wisp. Idempotent: closing an already-closed wisp is a no-op.
func (s *PostgresStore) CloseWisp(ctx context.Context, id, reason, actor, session string) error {
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const q = `
			UPDATE beads.wisps
			   SET status = 'closed',
			       closed_at = NOW(),
			       close_reason = $1,
			       closed_by_session = $2
			 WHERE id = $3 AND rig = $4 AND deleted_at IS NULL AND status <> 'closed'
		`
		_, err := tx.ExecContext(ctx, q, reason, session, id, s.rig)
		if err != nil {
			return fmt.Errorf("postgres: close wisp %q: %w", id, err)
		}
		return nil
	})
}

// ListWisps returns wisps matching the filter, scoped to the store's rig.
//
// Behavior:
//   - Always restricts to the current rig and filters out soft-deleted rows.
//   - When filter.Status is nil and filter.IncludeClosed is false, excludes
//     closed wisps (default: only non-closed wisps are returned).
//   - filter.Type / Status / UpdatedAfter / UpdatedBefore / Limit are applied
//     when set.
//
// Returns []*types.Issue: the wisps table mirrors the issues schema, so the
// projection maps cleanly into Issue values.
func (s *PostgresStore) ListWisps(ctx context.Context, filter types.WispFilter) ([]*types.Issue, error) {
	var (
		conds []string
		args  []interface{}
	)

	args = append(args, s.rig)
	conds = append(conds, fmt.Sprintf("rig = $%d", len(args)))
	conds = append(conds, "deleted_at IS NULL")

	if filter.Type != nil && *filter.Type != "" {
		args = append(args, string(*filter.Type))
		conds = append(conds, fmt.Sprintf("issue_type = $%d", len(args)))
	}

	if filter.Status != nil {
		args = append(args, string(*filter.Status))
		conds = append(conds, fmt.Sprintf("status = $%d", len(args)))
	} else if !filter.IncludeClosed {
		conds = append(conds, "status <> 'closed'")
	}

	if filter.UpdatedAfter != nil {
		args = append(args, *filter.UpdatedAfter)
		conds = append(conds, fmt.Sprintf("updated_at > $%d", len(args)))
	}
	if filter.UpdatedBefore != nil {
		args = append(args, *filter.UpdatedBefore)
		conds = append(conds, fmt.Sprintf("updated_at < $%d", len(args)))
	}

	limitClause := ""
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		limitClause = fmt.Sprintf("LIMIT $%d", len(args))
	}

	q := fmt.Sprintf(`
		SELECT %s
		FROM beads.wisps
		WHERE %s
		ORDER BY priority ASC, created_at ASC
		%s
	`, wispProjection, strings.Join(conds, " AND "), limitClause)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: query wisps: %w", err)
	}
	defer rows.Close()

	var out []*types.Issue
	for rows.Next() {
		w, err := scanWisp(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan wisp: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}
	return out, nil
}

// scanWisp reads a row produced by the wispProjection into *types.Issue.
// Mirrors scanIssue; kept as a sibling function so future divergence between
// the two tables (e.g. wisp-specific columns) can be handled cleanly here.
func scanWisp(row scanner) (*types.Issue, error) {
	var (
		issue             types.Issue
		assignee          sql.NullString
		startedAt         sql.NullTime
		closedAt          sql.NullTime
		dueAt             sql.NullTime
		deferUntil        sql.NullTime
		externalRef       sql.NullString
		specID            sql.NullString
		closeReason       sql.NullString
		closedBySession   sql.NullString
		compactedAt       sql.NullTime
		compactedAtCommit sql.NullString
		estimatedMinutes  sql.NullInt32
		originalSize      sql.NullInt32
		metadata          []byte
		statusStr         string
		issueTypeStr      string
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
