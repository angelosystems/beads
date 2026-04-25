package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// CreateIssues inserts many issues in a single transaction. Used by bulk-import
// paths (e.g., the dolt→postgres migration tool). Each insert reuses the same
// columns as CreateIssue but is batched; on any failure the entire transaction
// rolls back.
//
// If issues is empty, returns nil immediately.
func (s *PostgresStore) CreateIssues(ctx context.Context, issues []*types.Issue, actor string) error {
	if len(issues) == 0 {
		return nil
	}

	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
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
		stmt, err := tx.PrepareContext(ctx, q)
		if err != nil {
			return fmt.Errorf("postgres: prepare bulk insert: %w", err)
		}
		defer stmt.Close()

		for _, issue := range issues {
			if issue == nil {
				return errors.New("postgres: CreateIssues: nil issue")
			}
			if err := ensureIssueID(ctx, tx, s.rig, "issues", issue, actor); err != nil {
				return err
			}
			_, err := stmt.ExecContext(ctx,
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
				normalizeMetadata(issue.Metadata),
			)
			if err != nil {
				return fmt.Errorf("postgres: bulk insert %q: %w", issue.ID, err)
			}
		}
		return nil
	})
}

// GetIssueByExternalRef looks up an issue by its external_ref value within
// the current rig. Returns ErrNotFound if no match.
func (s *PostgresStore) GetIssueByExternalRef(ctx context.Context, externalRef string) (*types.Issue, error) {
	if externalRef == "" {
		return nil, errors.New("postgres: GetIssueByExternalRef: empty externalRef")
	}
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
		WHERE external_ref = $1 AND rig = $2 AND deleted_at IS NULL
		LIMIT 1
	`
	row := s.db.QueryRowContext(ctx, q, externalRef, s.rig)
	issue, err := scanIssue(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get issue by external_ref %q: %w", externalRef, err)
	}
	return issue, nil
}

// GetIssuesByIDs fetches multiple issues at once. Issues that don't exist
// or are soft-deleted are silently omitted from the result. Order of the
// returned slice matches the row order from Postgres (sorted by ID).
//
// Empty input returns an empty (non-nil) slice without a roundtrip.
func (s *PostgresStore) GetIssuesByIDs(ctx context.Context, ids []string) ([]*types.Issue, error) {
	if len(ids) == 0 {
		return []*types.Issue{}, nil
	}

	// Build a parameterized IN-list — pgx supports array binds, but for
	// portability with database/sql we expand to $1, $2, ...
	placeholders := make([]string, len(ids))
	args := make([]interface{}, 0, len(ids)+1)
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args = append(args, id)
	}
	args = append(args, s.rig)

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
		FROM beads.issues
		WHERE id IN (%s) AND rig = $%d AND deleted_at IS NULL
		ORDER BY id
	`, strings.Join(placeholders, ","), len(ids)+1)

	return s.queryIssues(ctx, q, args...)
}
