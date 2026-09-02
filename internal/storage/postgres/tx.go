package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// pgTransaction is the *sql.Tx-backed implementation of storage.Transaction.
//
// It mirrors the SQL of PostgresStore methods but routes execution through
// tx.ExecContext / tx.QueryContext so reads see the in-progress writes
// (read-your-writes) and the whole batch commits/rolls back atomically.
type pgTransaction struct {
	tx  *sql.Tx
	s   *PostgresStore // for s.rig and ancillary helpers
}

// Compile-time check that pgTransaction satisfies the Transaction interface.
var _ storage.Transaction = (*pgTransaction)(nil)

// RunInTransaction begins a Postgres transaction, sets the audit-trigger
// actor, wraps the *sql.Tx in a pgTransaction, calls fn, then commits or
// rolls back depending on fn's return value.
//
// commitMsg is a Dolt-specific commit message; for Postgres it has no
// equivalent and is intentionally ignored (the parameter exists only so
// the storage.Storage interface is the same across backends).
func (s *PostgresStore) RunInTransaction(ctx context.Context, commitMsg string, fn func(tx storage.Transaction) error) error {
	_ = commitMsg // not applicable to Postgres
	if s.readOnly {
		return errors.New("postgres: store is read-only")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Best-effort: set actor for the audit trigger. We don't know the actor
	// up front (callers pass it per-method), but each Transaction method
	// re-asserts the actor it receives via setActor, so any in-flight value
	// here is fine. Use "system" as the default placeholder.
	if err := s.setActor(ctx, tx, "system"); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}

	pgtx := &pgTransaction{tx: tx, s: s}
	if err := fn(pgtx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	committed = true
	return nil
}

// setActor sets beads.actor for the current transaction (audit trigger).
func (t *pgTransaction) setActor(ctx context.Context, actor string) error {
	if actor == "" {
		return nil
	}
	_, err := t.tx.ExecContext(ctx, `SELECT set_config('beads.actor', $1, true)`, actor)
	return err
}

// ════════════════════════════════════════════════════════════════════════
// Issue operations
// ════════════════════════════════════════════════════════════════════════

func (t *pgTransaction) CreateIssue(ctx context.Context, issue *types.Issue, actor string) error {
	if issue == nil {
		return errors.New("postgres: CreateIssue: nil issue")
	}
	if err := t.setActor(ctx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}
	if err := ensureIssueID(ctx, t.tx, t.s.rig, "issues", issue, actor); err != nil {
		return err
	}

	const q = `
		INSERT INTO beads.issues (
			id, rig, title, description, design, acceptance_criteria, notes,
			status, priority, issue_type,
			assignee, created_by, owner,
			estimated_minutes, started_at, due_at, defer_until,
			external_ref, spec_id, source_system,
			ephemeral, metadata
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10,
			$11, $12, $13,
			$14, $15, $16, $17,
			$18, $19, $20,
			$21, $22::jsonb
		)
	`
	_, err := t.tx.ExecContext(ctx, q,
		issue.ID, t.s.rig, issue.Title, issue.Description, issue.Design,
		issue.AcceptanceCriteria, issue.Notes,
		stringOrDefault(string(issue.Status), "open"), issue.Priority,
		stringOrDefault(string(issue.IssueType), "task"),
		nullString(issue.Assignee), issue.CreatedBy, issue.Owner,
		intPtrToNull(issue.EstimatedMinutes),
		timePtrToNull(issue.StartedAt), timePtrToNull(issue.DueAt),
		timePtrToNull(issue.DeferUntil),
		stringPtrToNull(issue.ExternalRef), nullString(issue.SpecID),
		issue.SourceSystem, issue.Ephemeral,
		normalizeMetadata(issue.Metadata),
	)
	if err != nil {
		return fmt.Errorf("postgres: tx insert issue: %w", err)
	}
	return nil
}

func (t *pgTransaction) CreateIssues(ctx context.Context, issues []*types.Issue, actor string) error {
	if len(issues) == 0 {
		return nil
	}
	if err := t.setActor(ctx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}

	const q = `
		INSERT INTO beads.issues (
			id, rig, title, description, design, acceptance_criteria, notes,
			status, priority, issue_type,
			assignee, created_by, owner,
			estimated_minutes, started_at, due_at, defer_until,
			external_ref, spec_id, source_system,
			ephemeral, metadata
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10,
			$11, $12, $13,
			$14, $15, $16, $17,
			$18, $19, $20,
			$21, $22::jsonb
		)
	`
	stmt, err := t.tx.PrepareContext(ctx, q)
	if err != nil {
		return fmt.Errorf("postgres: tx prepare bulk insert: %w", err)
	}
	defer stmt.Close()

	for _, issue := range issues {
		if issue == nil {
			return errors.New("postgres: CreateIssues: nil issue")
		}
		if err := ensureIssueID(ctx, t.tx, t.s.rig, "issues", issue, actor); err != nil {
			return err
		}
		_, err := stmt.ExecContext(ctx,
			issue.ID, t.s.rig, issue.Title, issue.Description, issue.Design,
			issue.AcceptanceCriteria, issue.Notes,
			stringOrDefault(string(issue.Status), "open"), issue.Priority,
			stringOrDefault(string(issue.IssueType), "task"),
			nullString(issue.Assignee), issue.CreatedBy, issue.Owner,
			intPtrToNull(issue.EstimatedMinutes),
			timePtrToNull(issue.StartedAt), timePtrToNull(issue.DueAt),
			timePtrToNull(issue.DeferUntil),
			stringPtrToNull(issue.ExternalRef), nullString(issue.SpecID),
			issue.SourceSystem, issue.Ephemeral,
			normalizeMetadata(issue.Metadata),
		)
		if err != nil {
			return fmt.Errorf("postgres: tx bulk insert %q: %w", issue.ID, err)
		}
	}
	return nil
}

func (t *pgTransaction) UpdateIssue(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
	if len(updates) == 0 {
		return nil
	}
	if err := t.setActor(ctx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
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

	args = append(args, id, t.s.rig)
	q := fmt.Sprintf(
		`UPDATE beads.issues SET %s WHERE id = $%d AND rig = $%d AND deleted_at IS NULL`,
		strings.Join(sets, ", "), i, i+1,
	)

	res, err := t.tx.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("postgres: tx update issue %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: tx update issue %q rows: %w", id, err)
	}
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (t *pgTransaction) CloseIssue(ctx context.Context, id, reason, actor, session string) error {
	if err := t.setActor(ctx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}
	const q = `
		UPDATE beads.issues
		   SET status = 'closed',
		       closed_at = NOW(),
		       close_reason = $1,
		       closed_by_session = $2
		 WHERE id = $3 AND rig = $4 AND deleted_at IS NULL AND status <> 'closed'
	`
	if _, err := t.tx.ExecContext(ctx, q, reason, session, id, t.s.rig); err != nil {
		return fmt.Errorf("postgres: tx close issue %q: %w", id, err)
	}
	return nil
}

func (t *pgTransaction) DeleteIssue(ctx context.Context, id string) error {
	const q = `
		UPDATE beads.issues SET deleted_at = NOW()
		WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
	`
	res, err := t.tx.ExecContext(ctx, q, id, t.s.rig)
	if err != nil {
		return fmt.Errorf("postgres: tx delete issue %q: %w", id, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (t *pgTransaction) GetIssue(ctx context.Context, id string) (*types.Issue, error) {
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
	row := t.tx.QueryRowContext(ctx, q, id, t.s.rig)
	issue, err := scanIssue(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: tx get issue %q: %w", id, err)
	}
	return issue, nil
}

// SearchIssues runs the same search SQL as PostgresStore.SearchIssues but
// against the in-progress transaction.
func (t *pgTransaction) SearchIssues(ctx context.Context, query string, filter types.IssueFilter) ([]*types.Issue, error) {
	q, args := buildSearchSQL(t.s.rig, query, filter)
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: tx search issues: %w", err)
	}
	defer rows.Close()

	var issues []*types.Issue
	for rows.Next() {
		issue, err := scanIssue(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: tx scan issue: %w", err)
		}
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: tx rows error: %w", err)
	}
	return issues, nil
}

// ════════════════════════════════════════════════════════════════════════
// Dependency operations
// ════════════════════════════════════════════════════════════════════════

func (t *pgTransaction) AddDependency(ctx context.Context, dep *types.Dependency, actor string) error {
	if dep == nil {
		return errors.New("postgres: AddDependency: nil dependency")
	}
	if dep.IssueID == "" || dep.DependsOnID == "" {
		return errors.New("postgres: AddDependency: empty issue_id or depends_on_id")
	}
	if err := t.setActor(ctx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}

	depType := stringOrDefault(string(dep.Type), "blocks")
	metadata := dep.Metadata
	if metadata == "" {
		metadata = "{}"
	}

	const q = `
		INSERT INTO beads.dependencies (issue_id, depends_on_id, rig, type, created_by, metadata, thread_id)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
		ON CONFLICT (issue_id, depends_on_id) DO NOTHING
	`
	_, err := t.tx.ExecContext(ctx, q,
		dep.IssueID, dep.DependsOnID, t.s.rig, depType,
		actor, metadata, dep.ThreadID,
	)
	if err != nil {
		return fmt.Errorf("postgres: tx insert dependency: %w", err)
	}
	return nil
}

func (t *pgTransaction) RemoveDependency(ctx context.Context, issueID, dependsOnID, actor string) error {
	if err := t.setActor(ctx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}
	const q = `
		UPDATE beads.dependencies SET deleted_at = NOW()
		WHERE issue_id = $1 AND depends_on_id = $2 AND rig = $3 AND deleted_at IS NULL
	`
	if _, err := t.tx.ExecContext(ctx, q, issueID, dependsOnID, t.s.rig); err != nil {
		return fmt.Errorf("postgres: tx remove dependency: %w", err)
	}
	return nil
}

// GetDependencyRecords returns the raw dependency rows for issueID
// (i.e., the edges where this issue appears as the dependent), as
// []*types.Dependency rather than the issue projection. Soft-deleted
// edges are excluded.
func (t *pgTransaction) GetDependencyRecords(ctx context.Context, issueID string) ([]*types.Dependency, error) {
	const q = `
		SELECT issue_id, depends_on_id, type, created_at,
		       COALESCE(created_by, ''),
		       COALESCE(metadata::text, '{}'),
		       COALESCE(thread_id, '')
		FROM beads.dependencies
		WHERE issue_id = $1 AND rig = $2 AND deleted_at IS NULL
		ORDER BY depends_on_id
	`
	rows, err := t.tx.QueryContext(ctx, q, issueID, t.s.rig)
	if err != nil {
		return nil, fmt.Errorf("postgres: tx get dependency records: %w", err)
	}
	defer rows.Close()

	var out []*types.Dependency
	for rows.Next() {
		var (
			d        types.Dependency
			depType  string
		)
		if err := rows.Scan(
			&d.IssueID, &d.DependsOnID, &depType, &d.CreatedAt,
			&d.CreatedBy, &d.Metadata, &d.ThreadID,
		); err != nil {
			return nil, fmt.Errorf("postgres: tx scan dependency: %w", err)
		}
		d.Type = types.DependencyType(depType)
		out = append(out, &d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: tx rows error: %w", err)
	}
	return out, nil
}

// ════════════════════════════════════════════════════════════════════════
// Label operations
// ════════════════════════════════════════════════════════════════════════

func (t *pgTransaction) AddLabel(ctx context.Context, issueID, label, actor string) error {
	if err := t.setActor(ctx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}
	const q = `
		INSERT INTO beads.labels (issue_id, rig, label)
		VALUES ($1, $2, $3)
		ON CONFLICT (issue_id, label) DO UPDATE SET deleted_at = NULL
	`
	if _, err := t.tx.ExecContext(ctx, q, issueID, t.s.rig, label); err != nil {
		return fmt.Errorf("postgres: tx add label: %w", err)
	}
	return nil
}

func (t *pgTransaction) RemoveLabel(ctx context.Context, issueID, label, actor string) error {
	if err := t.setActor(ctx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}
	const q = `
		UPDATE beads.labels SET deleted_at = NOW()
		WHERE issue_id = $1 AND rig = $2 AND label = $3 AND deleted_at IS NULL
	`
	if _, err := t.tx.ExecContext(ctx, q, issueID, t.s.rig, label); err != nil {
		return fmt.Errorf("postgres: tx remove label: %w", err)
	}
	return nil
}

func (t *pgTransaction) GetLabels(ctx context.Context, issueID string) ([]string, error) {
	const q = `
		SELECT label FROM beads.labels
		WHERE issue_id = $1 AND rig = $2 AND deleted_at IS NULL
		ORDER BY label
	`
	rows, err := t.tx.QueryContext(ctx, q, issueID, t.s.rig)
	if err != nil {
		return nil, fmt.Errorf("postgres: tx get labels: %w", err)
	}
	defer rows.Close()

	var labels []string
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			return nil, fmt.Errorf("postgres: tx scan label: %w", err)
		}
		labels = append(labels, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: tx rows error: %w", err)
	}
	return labels, nil
}

// ════════════════════════════════════════════════════════════════════════
// Config / Metadata / LocalMetadata
// ════════════════════════════════════════════════════════════════════════

func (t *pgTransaction) SetConfig(ctx context.Context, key, value string) error {
	if key == "" {
		return errors.New("postgres: SetConfig: empty key")
	}
	const q = `
		INSERT INTO beads.config (rig, key, value)
		VALUES ($1, $2, $3)
		ON CONFLICT (rig, key) DO UPDATE
		   SET value = EXCLUDED.value,
		       deleted_at = NULL
	`
	if _, err := t.tx.ExecContext(ctx, q, t.s.rig, key, value); err != nil {
		return fmt.Errorf("postgres: tx set config %q: %w", key, err)
	}
	return nil
}

func (t *pgTransaction) GetConfig(ctx context.Context, key string) (string, error) {
	const q = `
		SELECT value FROM beads.config
		WHERE rig = $1 AND key = $2 AND deleted_at IS NULL
	`
	var v string
	err := t.tx.QueryRowContext(ctx, q, t.s.rig, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("postgres: tx get config %q: %w", key, err)
	}
	return v, nil
}

// SetMetadata writes a key/value pair into beads.metadata (rig-scoped).
// Mirrors SetConfig but for the project-level metadata table.
func (t *pgTransaction) SetMetadata(ctx context.Context, key, value string) error {
	if key == "" {
		return errors.New("postgres: SetMetadata: empty key")
	}
	const q = `
		INSERT INTO beads.metadata (rig, key, value)
		VALUES ($1, $2, $3)
		ON CONFLICT (rig, key) DO UPDATE
		   SET value = EXCLUDED.value,
		       deleted_at = NULL
	`
	if _, err := t.tx.ExecContext(ctx, q, t.s.rig, key, value); err != nil {
		return fmt.Errorf("postgres: tx set metadata %q: %w", key, err)
	}
	return nil
}

// GetMetadata returns the value for key from beads.metadata (rig-scoped).
// Returns storage.ErrNotFound if not present (or soft-deleted).
func (t *pgTransaction) GetMetadata(ctx context.Context, key string) (string, error) {
	const q = `
		SELECT value FROM beads.metadata
		WHERE rig = $1 AND key = $2 AND deleted_at IS NULL
	`
	var v string
	err := t.tx.QueryRowContext(ctx, q, t.s.rig, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("postgres: tx get metadata %q: %w", key, err)
	}
	return v, nil
}

func (t *pgTransaction) SetLocalMetadata(ctx context.Context, key, value string) error {
	if key == "" {
		return errors.New("postgres: SetLocalMetadata: empty key")
	}
	const q = `
		INSERT INTO beads.local_metadata (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
	`
	if _, err := t.tx.ExecContext(ctx, q, key, value); err != nil {
		return fmt.Errorf("postgres: tx set local_metadata %q: %w", key, err)
	}
	return nil
}

// GetLocalMetadata returns the value for key from beads.local_metadata.
// Per the storage.Transaction contract, missing keys yield ("", nil) — not
// ErrNotFound — because callers treat ephemeral state as optional.
func (t *pgTransaction) GetLocalMetadata(ctx context.Context, key string) (string, error) {
	const q = `SELECT value FROM beads.local_metadata WHERE key = $1`
	var v string
	err := t.tx.QueryRowContext(ctx, q, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("postgres: tx get local_metadata %q: %w", key, err)
	}
	return v, nil
}

// ════════════════════════════════════════════════════════════════════════
// Comment operations
// ════════════════════════════════════════════════════════════════════════

// AddComment records a 'commented' event. Distinct from ImportIssueComment,
// which inserts into beads.comments. AddComment matches the dolt semantics:
// it adds an event row, not a comments-table row.
func (t *pgTransaction) AddComment(ctx context.Context, issueID, actor, comment string) error {
	if err := t.setActor(ctx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}
	const q = `
		INSERT INTO beads.events (issue_id, rig, event_type, actor, comment)
		VALUES ($1, $2, 'commented', $3, $4)
	`
	if _, err := t.tx.ExecContext(ctx, q, issueID, t.s.rig, actor, comment); err != nil {
		return fmt.Errorf("postgres: tx add comment event: %w", err)
	}
	return nil
}

// ImportIssueComment inserts a comment row with an explicit createdAt timestamp.
// Used by import paths that need to preserve original timestamps.
func (t *pgTransaction) ImportIssueComment(ctx context.Context, issueID, author, text string, createdAt time.Time) (*types.Comment, error) {
	if err := t.setActor(ctx, author); err != nil {
		return nil, fmt.Errorf("postgres: set actor: %w", err)
	}
	const q = `
		INSERT INTO beads.comments (issue_id, rig, author, text, created_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id::text, issue_id, author, text, created_at
	`
	var (
		c      types.Comment
		idStr  string
		ts     sql.NullTime
	)
	err := t.tx.QueryRowContext(ctx, q, issueID, t.s.rig, author, text, createdAt.UTC()).Scan(
		&idStr, &c.IssueID, &c.Author, &c.Text, &ts,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: tx import comment: %w", err)
	}
	c.ID = idStr
	if ts.Valid {
		c.CreatedAt = ts.Time
	}
	return &c, nil
}

func (t *pgTransaction) GetIssueComments(ctx context.Context, issueID string) ([]*types.Comment, error) {
	const q = `
		SELECT id::text, issue_id, author, text, created_at
		FROM beads.comments
		WHERE issue_id = $1 AND rig = $2 AND deleted_at IS NULL
		ORDER BY created_at ASC, id ASC
	`
	rows, err := t.tx.QueryContext(ctx, q, issueID, t.s.rig)
	if err != nil {
		return nil, fmt.Errorf("postgres: tx get comments: %w", err)
	}
	defer rows.Close()

	var out []*types.Comment
	for rows.Next() {
		var (
			c      types.Comment
			idStr  string
			ts     sql.NullTime
		)
		if err := rows.Scan(&idStr, &c.IssueID, &c.Author, &c.Text, &ts); err != nil {
			return nil, fmt.Errorf("postgres: tx scan comment: %w", err)
		}
		c.ID = idStr
		if ts.Valid {
			c.CreatedAt = ts.Time
		}
		out = append(out, &c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: tx rows error: %w", err)
	}
	return out, nil
}
