package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/steveyegge/beads/internal/types"
)

// SearchIssues returns issues matching the supplied free-text query and
// IssueFilter. The query argument matches title and description (ILIKE).
// The filter contributes additional WHERE clauses; label-based filters are
// expressed as EXISTS / NOT EXISTS subqueries against beads.labels so the
// reads-cleaner concern wins over JOIN gymnastics.
//
// Results are ordered by priority ASC, created_at ASC. Limit is applied last
// when filter.Limit > 0.
func (s *PostgresStore) SearchIssues(ctx context.Context, query string, filter types.IssueFilter) ([]*types.Issue, error) {
	q, args := buildSearchSQL(s.rig, query, filter)
	return s.queryIssues(ctx, q, args...)
}

// buildSearchSQL returns the SQL and args for SearchIssues, scoped to the
// given rig. Extracted so both PostgresStore.SearchIssues and the transaction
// implementation share the exact same filter logic.
func buildSearchSQL(rig, query string, filter types.IssueFilter) (string, []interface{}) {
	// Same projection list as scanIssue, table-aliased to "i".
	const projection = `
		i.id, i.title, i.description, i.design, i.acceptance_criteria, i.notes,
		i.status, i.priority, i.issue_type,
		i.assignee, i.created_by, i.owner,
		i.estimated_minutes, i.started_at, i.closed_at, i.due_at, i.defer_until,
		i.external_ref, i.spec_id, i.source_system,
		i.created_at, i.updated_at, i.close_reason, i.closed_by_session,
		i.compaction_level, i.compacted_at, i.compacted_at_commit, i.original_size,
		i.metadata
	`

	var (
		where []string
		args  []interface{}
	)

	// Always: rig + not soft-deleted.
	args = append(args, rig)
	where = append(where, fmt.Sprintf("i.rig = $%d", len(args)))
	where = append(where, "i.deleted_at IS NULL")

	// Free-text query: ILIKE against title or description.
	if q := strings.TrimSpace(query); q != "" {
		args = append(args, "%"+q+"%")
		idx := len(args)
		where = append(where, fmt.Sprintf("(i.title ILIKE $%d OR i.description ILIKE $%d)", idx, idx))
	}

	// Single status.
	if filter.Status != nil {
		args = append(args, string(*filter.Status))
		where = append(where, fmt.Sprintf("i.status = $%d", len(args)))
	}

	// Multiple statuses (OR semantics).
	if len(filter.Statuses) > 0 {
		placeholders := make([]string, 0, len(filter.Statuses))
		for _, st := range filter.Statuses {
			args = append(args, string(st))
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		where = append(where, fmt.Sprintf("i.status IN (%s)", strings.Join(placeholders, ", ")))
	}

	// Priority (exact).
	if filter.Priority != nil {
		args = append(args, *filter.Priority)
		where = append(where, fmt.Sprintf("i.priority = $%d", len(args)))
	}

	// Issue type.
	if filter.IssueType != nil {
		args = append(args, string(*filter.IssueType))
		where = append(where, fmt.Sprintf("i.issue_type = $%d", len(args)))
	}

	// Assignee.
	if filter.Assignee != nil {
		args = append(args, *filter.Assignee)
		where = append(where, fmt.Sprintf("i.assignee = $%d", len(args)))
	}

	// Title contains (case-insensitive).
	if ts := strings.TrimSpace(filter.TitleSearch); ts != "" {
		args = append(args, "%"+ts+"%")
		where = append(where, fmt.Sprintf("i.title ILIKE $%d", len(args)))
	}

	// IDs IN (...)
	if len(filter.IDs) > 0 {
		placeholders := make([]string, 0, len(filter.IDs))
		for _, id := range filter.IDs {
			args = append(args, id)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		where = append(where, fmt.Sprintf("i.id IN (%s)", strings.Join(placeholders, ", ")))
	}

	// IDPrefix (e.g. "bd-").
	if p := filter.IDPrefix; p != "" {
		args = append(args, escapeLike(p)+"%")
		where = append(where, fmt.Sprintf("i.id LIKE $%d ESCAPE '\\'", len(args)))
	}

	// SpecIDPrefix.
	if p := filter.SpecIDPrefix; p != "" {
		args = append(args, escapeLike(p)+"%")
		where = append(where, fmt.Sprintf("i.spec_id LIKE $%d ESCAPE '\\'", len(args)))
	}

	// Labels (AND): every label in filter.Labels must be present.
	for _, lbl := range filter.Labels {
		args = append(args, lbl, rig)
		labelIdx := len(args) - 1
		rigIdx := len(args)
		where = append(where, fmt.Sprintf(
			`EXISTS (SELECT 1 FROM beads.labels l
				WHERE l.issue_id = i.id AND l.rig = $%d
				  AND l.label = $%d AND l.deleted_at IS NULL)`,
			rigIdx, labelIdx))
	}

	// LabelsAny (OR): at least one of the labels must be present.
	if len(filter.LabelsAny) > 0 {
		placeholders := make([]string, 0, len(filter.LabelsAny))
		for _, lbl := range filter.LabelsAny {
			args = append(args, lbl)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		args = append(args, rig)
		rigIdx := len(args)
		where = append(where, fmt.Sprintf(
			`EXISTS (SELECT 1 FROM beads.labels l
				WHERE l.issue_id = i.id AND l.rig = $%d
				  AND l.deleted_at IS NULL
				  AND l.label IN (%s))`,
			rigIdx, strings.Join(placeholders, ", ")))
	}

	// ExcludeLabels: NOT EXISTS — the issue must not have ANY of these labels.
	if len(filter.ExcludeLabels) > 0 {
		placeholders := make([]string, 0, len(filter.ExcludeLabels))
		for _, lbl := range filter.ExcludeLabels {
			args = append(args, lbl)
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		args = append(args, rig)
		rigIdx := len(args)
		where = append(where, fmt.Sprintf(
			`NOT EXISTS (SELECT 1 FROM beads.labels l
				WHERE l.issue_id = i.id AND l.rig = $%d
				  AND l.deleted_at IS NULL
				  AND l.label IN (%s))`,
			rigIdx, strings.Join(placeholders, ", ")))
	}

	// LabelPattern: glob translated to SQL LIKE.
	if pat := filter.LabelPattern; pat != "" {
		args = append(args, globToLike(pat), rig)
		patIdx := len(args) - 1
		rigIdx := len(args)
		where = append(where, fmt.Sprintf(
			`EXISTS (SELECT 1 FROM beads.labels l
				WHERE l.issue_id = i.id AND l.rig = $%d
				  AND l.deleted_at IS NULL
				  AND l.label LIKE $%d)`,
			rigIdx, patIdx))
	}

	// LabelRegex: Postgres POSIX regex (~). Note: regex syntax differs from
	// some flavours (e.g., Go's RE2). Basic patterns work; advanced features
	// like lookarounds are NOT supported by Postgres POSIX.
	if rx := filter.LabelRegex; rx != "" {
		args = append(args, rx, rig)
		rxIdx := len(args) - 1
		rigIdx := len(args)
		where = append(where, fmt.Sprintf(
			`EXISTS (SELECT 1 FROM beads.labels l
				WHERE l.issue_id = i.id AND l.rig = $%d
				  AND l.deleted_at IS NULL
				  AND l.label ~ $%d)`,
			rigIdx, rxIdx))
	}

	q := fmt.Sprintf(`
		SELECT %s
		FROM beads.issues i
		WHERE %s
		ORDER BY i.priority ASC, i.created_at ASC
	`, projection, strings.Join(where, " AND "))

	if filter.Limit > 0 {
		args = append(args, filter.Limit)
		q += fmt.Sprintf(" LIMIT $%d", len(args))
	}

	return q, args
}

// globToLike translates a simple glob pattern into a SQL LIKE pattern.
// Glob metas: '*' -> '%', '?' -> '_'. Existing LIKE metas in the input
// ('%', '_', '\') are escaped so they remain literal.
func globToLike(glob string) string {
	var b strings.Builder
	b.Grow(len(glob) + 4)
	for i := 0; i < len(glob); i++ {
		c := glob[i]
		switch c {
		case '*':
			b.WriteByte('%')
		case '?':
			b.WriteByte('_')
		case '%', '_', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// escapeLike escapes LIKE metacharacters in s so it can be used as a literal
// prefix. Pair with `ESCAPE '\'` in SQL.
func escapeLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}
