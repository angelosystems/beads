package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// GetStatistics returns aggregate counts for the current rig.
//
// Most counters are derived from beads.issues (filtered by rig + not deleted).
// blocked_issues / ready_issues use the dedicated views (which encapsulate
// transitive blocking and custom-status awareness).
//
// EpicsEligibleForClosure counts epics where every parent-child child is closed.
// AverageLeadTime is the average (closed_at - created_at) in hours over closed
// issues with a closed_at timestamp.
func (s *PostgresStore) GetStatistics(ctx context.Context) (*types.Statistics, error) {
	stats := &types.Statistics{}

	// 1) Aggregate the simple counters from beads.issues in one shot.
	const issuesQ = `
		SELECT
			COUNT(*)                                            AS total,
			COUNT(*) FILTER (WHERE status = 'open')             AS open_,
			COUNT(*) FILTER (WHERE status = 'in_progress')      AS in_progress_,
			COUNT(*) FILTER (WHERE status = 'closed')           AS closed_,
			COUNT(*) FILTER (WHERE status = 'deferred')         AS deferred_,
			COUNT(*) FILTER (WHERE status = 'pinned')           AS pinned_,
			COALESCE(AVG(EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600.0)
			         FILTER (WHERE status = 'closed' AND closed_at IS NOT NULL), 0)::float8
			                                                     AS avg_lead_time
		FROM beads.issues
		WHERE rig = $1 AND deleted_at IS NULL
	`
	if err := s.db.QueryRowContext(ctx, issuesQ, s.rig).Scan(
		&stats.TotalIssues,
		&stats.OpenIssues,
		&stats.InProgressIssues,
		&stats.ClosedIssues,
		&stats.DeferredIssues,
		&stats.PinnedIssues,
		&stats.AverageLeadTime,
	); err != nil {
		return nil, fmt.Errorf("postgres: statistics base counts: %w", err)
	}

	// 2) Blocked count from the view (encapsulates the transitive logic).
	const blockedQ = `SELECT COUNT(*) FROM beads.blocked_issues WHERE rig = $1`
	if err := s.db.QueryRowContext(ctx, blockedQ, s.rig).Scan(&stats.BlockedIssues); err != nil {
		return nil, fmt.Errorf("postgres: statistics blocked count: %w", err)
	}

	// 3) Ready count from the view.
	const readyQ = `SELECT COUNT(*) FROM beads.ready_issues WHERE rig = $1`
	if err := s.db.QueryRowContext(ctx, readyQ, s.rig).Scan(&stats.ReadyIssues); err != nil {
		return nil, fmt.Errorf("postgres: statistics ready count: %w", err)
	}

	// 4) Epics eligible for closure: epics where every parent-child child is closed.
	const epicsQ = `
		SELECT COUNT(*)
		FROM beads.issues e
		WHERE e.rig = $1
		  AND e.deleted_at IS NULL
		  AND e.issue_type = 'epic'
		  AND e.status <> 'closed'
		  AND EXISTS (
		      SELECT 1
		      FROM beads.dependencies d
		      JOIN beads.issues c ON c.id = d.issue_id
		      WHERE d.depends_on_id = e.id
		        AND d.rig = e.rig
		        AND d.type = 'parent-child'
		        AND d.deleted_at IS NULL
		        AND c.deleted_at IS NULL
		  )
		  AND NOT EXISTS (
		      SELECT 1
		      FROM beads.dependencies d
		      JOIN beads.issues c ON c.id = d.issue_id
		      WHERE d.depends_on_id = e.id
		        AND d.rig = e.rig
		        AND d.type = 'parent-child'
		        AND d.deleted_at IS NULL
		        AND c.deleted_at IS NULL
		        AND c.status <> 'closed'
		  )
	`
	if err := s.db.QueryRowContext(ctx, epicsQ, s.rig).Scan(&stats.EpicsEligibleForClosure); err != nil {
		return nil, fmt.Errorf("postgres: statistics epics-eligible count: %w", err)
	}

	return stats, nil
}

// GetEpicsEligibleForClosure returns epics whose parent-child children are all
// closed (and which themselves are not yet closed). Epics with zero children
// are excluded — there is nothing to "wrap up".
func (s *PostgresStore) GetEpicsEligibleForClosure(ctx context.Context) ([]*types.EpicStatus, error) {
	const q = `
		SELECT
			e.id, e.title, e.description, e.design, e.acceptance_criteria, e.notes,
			e.status, e.priority, e.issue_type,
			e.assignee, e.created_by, e.owner,
			e.estimated_minutes, e.started_at, e.closed_at, e.due_at, e.defer_until,
			e.external_ref, e.spec_id, e.source_system,
			e.created_at, e.updated_at, e.close_reason, e.closed_by_session,
			e.compaction_level, e.compacted_at, e.compacted_at_commit, e.original_size,
			e.metadata,
			(SELECT COUNT(*)
			   FROM beads.dependencies d
			   JOIN beads.issues c ON c.id = d.issue_id
			  WHERE d.depends_on_id = e.id
			    AND d.rig = e.rig
			    AND d.type = 'parent-child'
			    AND d.deleted_at IS NULL
			    AND c.deleted_at IS NULL)            AS total_children,
			(SELECT COUNT(*)
			   FROM beads.dependencies d
			   JOIN beads.issues c ON c.id = d.issue_id
			  WHERE d.depends_on_id = e.id
			    AND d.rig = e.rig
			    AND d.type = 'parent-child'
			    AND d.deleted_at IS NULL
			    AND c.deleted_at IS NULL
			    AND c.status = 'closed')             AS closed_children
		FROM beads.issues e
		WHERE e.rig = $1
		  AND e.deleted_at IS NULL
		  AND e.issue_type = 'epic'
		  AND e.status <> 'closed'
		  AND EXISTS (
		      SELECT 1
		      FROM beads.dependencies d
		      JOIN beads.issues c ON c.id = d.issue_id
		      WHERE d.depends_on_id = e.id
		        AND d.rig = e.rig
		        AND d.type = 'parent-child'
		        AND d.deleted_at IS NULL
		        AND c.deleted_at IS NULL
		  )
		  AND NOT EXISTS (
		      SELECT 1
		      FROM beads.dependencies d
		      JOIN beads.issues c ON c.id = d.issue_id
		      WHERE d.depends_on_id = e.id
		        AND d.rig = e.rig
		        AND d.type = 'parent-child'
		        AND d.deleted_at IS NULL
		        AND c.deleted_at IS NULL
		        AND c.status <> 'closed'
		  )
		ORDER BY e.id
	`
	rows, err := s.db.QueryContext(ctx, q, s.rig)
	if err != nil {
		return nil, fmt.Errorf("postgres: epics-eligible-for-closure query: %w", err)
	}
	defer rows.Close()

	var out []*types.EpicStatus
	for rows.Next() {
		issue, total, closed, err := scanEpicStatusRow(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan epic-status: %w", err)
		}
		out = append(out, &types.EpicStatus{
			Epic:             issue,
			TotalChildren:    total,
			ClosedChildren:   closed,
			EligibleForClose: true, // by construction of the query
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: epics-eligible rows error: %w", err)
	}
	return out, nil
}

// GetDependenciesWithMetadata returns the issues that issueID depends on,
// each annotated with the dependency edge's `type` (DependencyType).
func (s *PostgresStore) GetDependenciesWithMetadata(ctx context.Context, issueID string) ([]*types.IssueWithDependencyMetadata, error) {
	const q = `
		SELECT
			i.id, i.title, i.description, i.design, i.acceptance_criteria, i.notes,
			i.status, i.priority, i.issue_type,
			i.assignee, i.created_by, i.owner,
			i.estimated_minutes, i.started_at, i.closed_at, i.due_at, i.defer_until,
			i.external_ref, i.spec_id, i.source_system,
			i.created_at, i.updated_at, i.close_reason, i.closed_by_session,
			i.compaction_level, i.compacted_at, i.compacted_at_commit, i.original_size,
			i.metadata,
			d.type
		FROM beads.dependencies d
		JOIN beads.issues i ON i.id = d.depends_on_id
		WHERE d.issue_id = $1
		  AND d.rig = $2
		  AND d.deleted_at IS NULL
		  AND i.deleted_at IS NULL
		ORDER BY i.id
	`
	return s.queryIssuesWithDependencyMetadata(ctx, q, issueID, s.rig)
}

// GetDependentsWithMetadata returns the issues that depend on issueID,
// each annotated with the dependency edge's `type` (DependencyType).
func (s *PostgresStore) GetDependentsWithMetadata(ctx context.Context, issueID string) ([]*types.IssueWithDependencyMetadata, error) {
	const q = `
		SELECT
			i.id, i.title, i.description, i.design, i.acceptance_criteria, i.notes,
			i.status, i.priority, i.issue_type,
			i.assignee, i.created_by, i.owner,
			i.estimated_minutes, i.started_at, i.closed_at, i.due_at, i.defer_until,
			i.external_ref, i.spec_id, i.source_system,
			i.created_at, i.updated_at, i.close_reason, i.closed_by_session,
			i.compaction_level, i.compacted_at, i.compacted_at_commit, i.original_size,
			i.metadata,
			d.type
		FROM beads.dependencies d
		JOIN beads.issues i ON i.id = d.issue_id
		WHERE d.depends_on_id = $1
		  AND d.rig = $2
		  AND d.deleted_at IS NULL
		  AND i.deleted_at IS NULL
		ORDER BY i.id
	`
	return s.queryIssuesWithDependencyMetadata(ctx, q, issueID, s.rig)
}

// queryIssuesWithDependencyMetadata runs a SELECT producing the standard
// issue projection plus a trailing dependencies.type column.
func (s *PostgresStore) queryIssuesWithDependencyMetadata(ctx context.Context, q string, args ...interface{}) ([]*types.IssueWithDependencyMetadata, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: query issues with dep-metadata: %w", err)
	}
	defer rows.Close()

	var out []*types.IssueWithDependencyMetadata
	for rows.Next() {
		item, err := scanIssueWithDependencyMetadata(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan issue with dep-metadata: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}
	return out, nil
}

// scanIssueWithDependencyMetadata extends scanIssue with one trailing column
// (dependencies.type), populating IssueWithDependencyMetadata.DependencyType.
func scanIssueWithDependencyMetadata(row scanner) (*types.IssueWithDependencyMetadata, error) {
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
		depTypeStr        string
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
		&depTypeStr,
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
	return &types.IssueWithDependencyMetadata{
		Issue:          issue,
		DependencyType: types.DependencyType(depTypeStr),
	}, nil
}

// scanEpicStatusRow scans one row produced by GetEpicsEligibleForClosure:
// the standard issue projection followed by total_children and closed_children.
func scanEpicStatusRow(row scanner) (*types.Issue, int, int, error) {
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
		total             sql.NullInt64
		closed            sql.NullInt64
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
		&total, &closed,
	)
	if err != nil {
		return nil, 0, 0, err
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
	return &issue, int(total.Int64), int(closed.Int64), nil
}

// keep imports referenced even when only the public API uses them
var (
	_ = errors.Is
	_ = storage.ErrNotFound
)
