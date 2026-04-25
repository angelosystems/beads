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

// treeMaxDepthCap is the hard upper bound on recursion depth for
// GetDependencyTree, applied even when callers request unbounded recursion.
// It prevents runaway queries on pathological graphs.
const treeMaxDepthCap = 50

// GetDependencyTree returns the dependency tree rooted at issueID as a flat
// slice of TreeNode values, ordered by traversal (root first, then a stable
// child-by-child walk).
//
//   - maxDepth: stops recursion once a node's depth equals this value.
//     Pass <= 0 for "unbounded" (the function still caps at treeMaxDepthCap).
//   - showAllPaths: when true, a node may appear multiple times (once per
//     path that reaches it). When false, duplicates are collapsed and only
//     the shallowest depth is kept for each issue ID.
//   - reverse: when false, walks "issueID depends on X depends on Y"
//     (outgoing edges). When true, walks "X depends on issueID, Y depends on
//     X" (incoming edges) — i.e., dependents.
//
// Truncated is set on a node when (a) its depth equals the effective max
// depth and (b) the graph has further children that recursion stopped from
// being included.
func (s *PostgresStore) GetDependencyTree(
	ctx context.Context,
	issueID string,
	maxDepth int,
	showAllPaths bool,
	reverse bool,
) ([]*types.TreeNode, error) {
	if issueID == "" {
		return nil, errors.New("postgres: GetDependencyTree: empty issue id")
	}

	// Resolve the effective recursion depth. <= 0 means unbounded; we still
	// cap at treeMaxDepthCap to avoid runaway queries.
	effectiveMax := maxDepth
	if effectiveMax <= 0 || effectiveMax > treeMaxDepthCap {
		effectiveMax = treeMaxDepthCap
	}

	// Verify the root exists in this rig (and isn't soft-deleted) before
	// running the CTE so we can return a clean ErrNotFound — matches the
	// semantics of the other Get* helpers.
	if _, err := s.GetIssue(ctx, issueID); err != nil {
		return nil, err
	}

	// Edge direction selectors. The recursive arm joins beads.dependencies
	// against the previous level's id; the column we read off d depends on
	// the direction we're walking.
	//
	// Forward (reverse=false):  parent -> child via d.issue_id = parent.id,
	//                           child = d.depends_on_id
	// Reverse (reverse=true) :  parent -> child via d.depends_on_id = parent.id,
	//                           child = d.issue_id
	parentSide := "issue_id"
	childSide := "depends_on_id"
	if reverse {
		parentSide = "depends_on_id"
		childSide = "issue_id"
	}

	// $1 = root id, $2 = rig, $3 = effective max depth.
	q := fmt.Sprintf(`
WITH RECURSIVE tree AS (
    SELECT
        i.id, i.title, i.description, i.design, i.acceptance_criteria, i.notes,
        i.status, i.priority, i.issue_type,
        i.assignee, i.created_by, i.owner,
        i.estimated_minutes, i.started_at, i.closed_at, i.due_at, i.defer_until,
        i.external_ref, i.spec_id, i.source_system,
        i.created_at, i.updated_at, i.close_reason, i.closed_by_session,
        i.compaction_level, i.compacted_at, i.compacted_at_commit, i.original_size,
        i.metadata,
        0::int                              AS depth,
        ''::varchar                         AS parent_id,
        ARRAY[i.id]::varchar[]              AS path
    FROM beads.issues i
    WHERE i.id = $1 AND i.rig = $2 AND i.deleted_at IS NULL

    UNION ALL

    SELECT
        i.id, i.title, i.description, i.design, i.acceptance_criteria, i.notes,
        i.status, i.priority, i.issue_type,
        i.assignee, i.created_by, i.owner,
        i.estimated_minutes, i.started_at, i.closed_at, i.due_at, i.defer_until,
        i.external_ref, i.spec_id, i.source_system,
        i.created_at, i.updated_at, i.close_reason, i.closed_by_session,
        i.compaction_level, i.compacted_at, i.compacted_at_commit, i.original_size,
        i.metadata,
        (t.depth + 1)::int                  AS depth,
        t.id::varchar                       AS parent_id,
        (t.path || i.id)::varchar[]         AS path
    FROM tree t
    JOIN beads.dependencies d
      ON d.%[1]s = t.id
     AND d.rig = $2
     AND d.deleted_at IS NULL
    JOIN beads.issues i
      ON i.id = d.%[2]s
     AND i.rig = $2
     AND i.deleted_at IS NULL
    WHERE t.depth < $3
      AND NOT (i.id = ANY(t.path))
)
SELECT
    tree.id, tree.title, tree.description, tree.design, tree.acceptance_criteria, tree.notes,
    tree.status, tree.priority, tree.issue_type,
    tree.assignee, tree.created_by, tree.owner,
    tree.estimated_minutes, tree.started_at, tree.closed_at, tree.due_at, tree.defer_until,
    tree.external_ref, tree.spec_id, tree.source_system,
    tree.created_at, tree.updated_at, tree.close_reason, tree.closed_by_session,
    tree.compaction_level, tree.compacted_at, tree.compacted_at_commit, tree.original_size,
    tree.metadata,
    tree.depth,
    tree.parent_id,
    (
        tree.depth = $3
        AND EXISTS (
            SELECT 1
              FROM beads.dependencies d2
              JOIN beads.issues i2 ON i2.id = d2.%[2]s
             WHERE d2.%[1]s = tree.id
               AND d2.rig = $2
               AND d2.deleted_at IS NULL
               AND i2.rig = $2
               AND i2.deleted_at IS NULL
        )
    ) AS truncated_flag
FROM tree
ORDER BY depth, id
`, parentSide, childSide)

	rows, err := s.db.QueryContext(ctx, q, issueID, s.rig, effectiveMax)
	if err != nil {
		return nil, fmt.Errorf("postgres: dependency tree query: %w", err)
	}
	defer rows.Close()

	var nodes []*types.TreeNode
	for rows.Next() {
		node, err := scanTreeNode(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan tree node: %w", err)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: tree rows: %w", err)
	}

	if !showAllPaths {
		nodes = dedupeByID(nodes)
	}

	return nodes, nil
}

// scanTreeNode reads a row produced by the GetDependencyTree projection.
// The projection extends the standard issue columns with depth, parent_id,
// and a truncated boolean.
func scanTreeNode(row scanner) (*types.TreeNode, error) {
	var (
		node              types.TreeNode
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
		parentID          sql.NullString
		truncated         sql.NullBool
	)
	err := row.Scan(
		&node.Issue.ID, &node.Issue.Title, &node.Issue.Description, &node.Issue.Design,
		&node.Issue.AcceptanceCriteria, &node.Issue.Notes,
		&statusStr, &node.Issue.Priority, &issueTypeStr,
		&assignee, &node.Issue.CreatedBy, &node.Issue.Owner,
		&estimatedMinutes, &startedAt, &closedAt, &dueAt, &deferUntil,
		&externalRef, &specID, &node.Issue.SourceSystem,
		&node.Issue.CreatedAt, &node.Issue.UpdatedAt, &closeReason, &closedBySession,
		&node.Issue.CompactionLevel, &compactedAt, &compactedAtCommit, &originalSize,
		&metadata,
		&node.Depth,
		&parentID,
		&truncated,
	)
	if err != nil {
		return nil, err
	}
	node.Issue.Status = types.Status(statusStr)
	node.Issue.IssueType = types.IssueType(issueTypeStr)
	if assignee.Valid {
		node.Issue.Assignee = assignee.String
	}
	if startedAt.Valid {
		t := startedAt.Time
		node.Issue.StartedAt = &t
	}
	if closedAt.Valid {
		t := closedAt.Time
		node.Issue.ClosedAt = &t
	}
	if dueAt.Valid {
		t := dueAt.Time
		node.Issue.DueAt = &t
	}
	if deferUntil.Valid {
		t := deferUntil.Time
		node.Issue.DeferUntil = &t
	}
	if externalRef.Valid {
		s := externalRef.String
		node.Issue.ExternalRef = &s
	}
	if specID.Valid {
		node.Issue.SpecID = specID.String
	}
	if closeReason.Valid {
		node.Issue.CloseReason = closeReason.String
	}
	if closedBySession.Valid {
		node.Issue.ClosedBySession = closedBySession.String
	}
	if compactedAt.Valid {
		t := compactedAt.Time
		node.Issue.CompactedAt = &t
	}
	if compactedAtCommit.Valid {
		s := compactedAtCommit.String
		node.Issue.CompactedAtCommit = &s
	}
	if estimatedMinutes.Valid {
		v := int(estimatedMinutes.Int32)
		node.Issue.EstimatedMinutes = &v
	}
	if originalSize.Valid {
		node.Issue.OriginalSize = int(originalSize.Int32)
	}
	if len(metadata) > 0 {
		node.Issue.Metadata = json.RawMessage(metadata)
	}
	if parentID.Valid {
		node.ParentID = parentID.String
	}
	if truncated.Valid {
		node.Truncated = truncated.Bool
	}
	return &node, nil
}

// dedupeByID collapses duplicate TreeNodes (same Issue.ID) keeping the entry
// with the smallest Depth. The relative order of the kept entries is preserved.
func dedupeByID(nodes []*types.TreeNode) []*types.TreeNode {
	if len(nodes) <= 1 {
		return nodes
	}
	bestIdx := make(map[string]int, len(nodes))
	for i, n := range nodes {
		if j, ok := bestIdx[n.Issue.ID]; ok {
			if n.Depth < nodes[j].Depth {
				bestIdx[n.Issue.ID] = i
			}
		} else {
			bestIdx[n.Issue.ID] = i
		}
	}
	keep := make(map[int]bool, len(bestIdx))
	for _, i := range bestIdx {
		keep[i] = true
	}
	out := make([]*types.TreeNode, 0, len(bestIdx))
	for i, n := range nodes {
		if keep[i] {
			out = append(out, n)
		}
	}
	return out
}

// keep storage import alive in case future edits need ErrNotFound here.
var _ = storage.ErrNotFound
