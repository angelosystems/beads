package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/steveyegge/beads/internal/types"
)

// GetReadyWork returns issues that are ready to be worked on:
// open, non-deferred, and not blocked by anything (transitively).
//
// Implementation note: leverages the beads.ready_issues view from the schema.
// The view already encapsulates the "blocked-transitively" recursion logic
// and respects custom_statuses (done/frozen are treated as closed-like).
//
// WorkFilter fields supported in this initial implementation:
//   - Status (specific status; default 'open' which is what the view returns)
//   - Type (issue_type filter)
//   - Priority (specific priority filter)
//   - Assignee (specific assignee, or NULL when Unassigned=true)
//   - Unassigned (mutually exclusive with Assignee)
//   - Limit
//
// Not yet supported (TODO): Labels/LabelsAny/ExcludeLabels, LabelPattern,
// LabelRegex, ParentID, SortPolicy. These will be added when the upstream
// label-pattern semantics are needed by the SQLx layer in production.
func (s *PostgresStore) GetReadyWork(ctx context.Context, filter types.WorkFilter) ([]*types.Issue, error) {
	var (
		conds []string
		args  []interface{}
	)

	// Always restrict to the current rig.
	args = append(args, s.rig)
	conds = append(conds, fmt.Sprintf("rig = $%d", len(args)))

	if filter.Status != "" {
		args = append(args, string(filter.Status))
		conds = append(conds, fmt.Sprintf("status = $%d", len(args)))
	}
	if filter.Type != "" {
		args = append(args, filter.Type)
		conds = append(conds, fmt.Sprintf("issue_type = $%d", len(args)))
	}
	if filter.Priority != nil {
		args = append(args, *filter.Priority)
		conds = append(conds, fmt.Sprintf("priority = $%d", len(args)))
	}
	if filter.Unassigned {
		conds = append(conds, "(assignee IS NULL OR assignee = '')")
	} else if filter.Assignee != nil && *filter.Assignee != "" {
		args = append(args, *filter.Assignee)
		conds = append(conds, fmt.Sprintf("assignee = $%d", len(args)))
	}

	limitClause := ""
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		limitClause = fmt.Sprintf("LIMIT $%d", len(args))
	}

	q := fmt.Sprintf(`
		SELECT
			id, title, description, design, acceptance_criteria, notes,
			status, priority, issue_type,
			assignee, created_by, owner,
			estimated_minutes, started_at, closed_at, due_at, defer_until,
			external_ref, spec_id, source_system,
			created_at, updated_at, close_reason, closed_by_session,
			compaction_level, compacted_at, compacted_at_commit, original_size,
			metadata
		FROM beads.ready_issues
		WHERE %s
		ORDER BY priority ASC, created_at ASC
		%s
	`, strings.Join(conds, " AND "), limitClause)

	return s.queryIssues(ctx, q, args...)
}

// GetBlockedIssues returns issues that have one or more unresolved blocking
// dependencies, with a count of how many blockers exist.
//
// Filter behavior mirrors GetReadyWork: rig is always applied; the supported
// fields are the same subset (status/type/priority/assignee/unassigned/limit).
func (s *PostgresStore) GetBlockedIssues(ctx context.Context, filter types.WorkFilter) ([]*types.BlockedIssue, error) {
	var (
		conds []string
		args  []interface{}
	)

	args = append(args, s.rig)
	conds = append(conds, fmt.Sprintf("rig = $%d", len(args)))

	if filter.Status != "" {
		args = append(args, string(filter.Status))
		conds = append(conds, fmt.Sprintf("status = $%d", len(args)))
	}
	if filter.Type != "" {
		args = append(args, filter.Type)
		conds = append(conds, fmt.Sprintf("issue_type = $%d", len(args)))
	}
	if filter.Priority != nil {
		args = append(args, *filter.Priority)
		conds = append(conds, fmt.Sprintf("priority = $%d", len(args)))
	}
	if filter.Unassigned {
		conds = append(conds, "(assignee IS NULL OR assignee = '')")
	} else if filter.Assignee != nil && *filter.Assignee != "" {
		args = append(args, *filter.Assignee)
		conds = append(conds, fmt.Sprintf("assignee = $%d", len(args)))
	}

	limitClause := ""
	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		limitClause = fmt.Sprintf("LIMIT $%d", len(args))
	}

	q := fmt.Sprintf(`
		SELECT
			id, title, description, design, acceptance_criteria, notes,
			status, priority, issue_type,
			assignee, created_by, owner,
			estimated_minutes, started_at, closed_at, due_at, defer_until,
			external_ref, spec_id, source_system,
			created_at, updated_at, close_reason, closed_by_session,
			compaction_level, compacted_at, compacted_at_commit, original_size,
			metadata,
			blocked_by_count
		FROM beads.blocked_issues
		WHERE %s
		ORDER BY priority ASC, created_at ASC
		%s
	`, strings.Join(conds, " AND "), limitClause)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: query blocked issues: %w", err)
	}
	defer rows.Close()

	var out []*types.BlockedIssue
	for rows.Next() {
		bi, err := scanBlockedIssue(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan blocked issue: %w", err)
		}
		out = append(out, bi)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}
	return out, nil
}

// scanBlockedIssue extends scanIssue with the blocked_by_count column.
// (We can't reuse scanIssue directly because of the extra trailing column.)
func scanBlockedIssue(row scanner) (*types.BlockedIssue, error) {
	var (
		bi                 types.BlockedIssue
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
		blockedByCount     sql.NullInt64
	)
	err := row.Scan(
		&bi.ID, &bi.Title, &bi.Description, &bi.Design,
		&bi.AcceptanceCriteria, &bi.Notes,
		&statusStr, &bi.Priority, &issueTypeStr,
		&assignee, &bi.CreatedBy, &bi.Owner,
		&estimatedMinutes, &startedAt, &closedAt, &dueAt, &deferUntil,
		&externalRef, &specID, &bi.SourceSystem,
		&bi.CreatedAt, &bi.UpdatedAt, &closeReason, &closedBySession,
		&bi.CompactionLevel, &compactedAt, &compactedAtCommit, &originalSize,
		&metadata,
		&blockedByCount,
	)
	if err != nil {
		return nil, err
	}
	bi.Status = types.Status(statusStr)
	bi.IssueType = types.IssueType(issueTypeStr)
	if assignee.Valid {
		bi.Assignee = assignee.String
	}
	if startedAt.Valid {
		t := startedAt.Time
		bi.StartedAt = &t
	}
	if closedAt.Valid {
		t := closedAt.Time
		bi.ClosedAt = &t
	}
	if dueAt.Valid {
		t := dueAt.Time
		bi.DueAt = &t
	}
	if deferUntil.Valid {
		t := deferUntil.Time
		bi.DeferUntil = &t
	}
	if externalRef.Valid {
		s := externalRef.String
		bi.ExternalRef = &s
	}
	if specID.Valid {
		bi.SpecID = specID.String
	}
	if closeReason.Valid {
		bi.CloseReason = closeReason.String
	}
	if closedBySession.Valid {
		bi.ClosedBySession = closedBySession.String
	}
	if compactedAt.Valid {
		t := compactedAt.Time
		bi.CompactedAt = &t
	}
	if compactedAtCommit.Valid {
		s := compactedAtCommit.String
		bi.CompactedAtCommit = &s
	}
	if estimatedMinutes.Valid {
		v := int(estimatedMinutes.Int32)
		bi.EstimatedMinutes = &v
	}
	if originalSize.Valid {
		bi.OriginalSize = int(originalSize.Int32)
	}
	if len(metadata) > 0 {
		bi.Metadata = metadata
	}
	if blockedByCount.Valid {
		bi.BlockedByCount = int(blockedByCount.Int64)
	}
	return &bi, nil
}
