package postgres

import (
	"context"
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

// helper: create an issue with sensible defaults.
func createIssue(t *testing.T, ctx context.Context, store *PostgresStore, id, title string) {
	t.Helper()
	in := &types.Issue{
		ID:        id,
		Title:     title,
		Status:    types.Status("open"),
		Priority:  2,
		IssueType: types.IssueType("task"),
	}
	if err := store.CreateIssue(ctx, in, "tester"); err != nil {
		t.Fatalf("CreateIssue %s: %v", id, err)
	}
}

// ════════════════════════════════════════════════════════════════════════
// Dependencies
// ════════════════════════════════════════════════════════════════════════

func TestDependencies_AddRemoveQuery(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "A", "Issue A")
	createIssue(t, ctx, store, "B", "Issue B")
	createIssue(t, ctx, store, "C", "Issue C")

	// A blocks B (B depends on A); A blocks C (C depends on A).
	// In Beads' model: dependency.issue_id depends_on dependency.depends_on_id,
	// so we record "B depends on A" and "C depends on A".
	if err := store.AddDependency(ctx, &types.Dependency{
		IssueID: "B", DependsOnID: "A", Type: "blocks",
	}, "tester"); err != nil {
		t.Fatalf("AddDependency B->A: %v", err)
	}
	if err := store.AddDependency(ctx, &types.Dependency{
		IssueID: "C", DependsOnID: "A", Type: "blocks",
	}, "tester"); err != nil {
		t.Fatalf("AddDependency C->A: %v", err)
	}

	// B's outgoing deps: [A]
	deps, err := store.GetDependencies(ctx, "B")
	if err != nil {
		t.Fatalf("GetDependencies B: %v", err)
	}
	if len(deps) != 1 || deps[0].ID != "A" {
		t.Errorf("B should depend on [A], got %v", idsOf(deps))
	}

	// A's incoming deps (dependents): [B, C]
	dependents, err := store.GetDependents(ctx, "A")
	if err != nil {
		t.Fatalf("GetDependents A: %v", err)
	}
	if len(dependents) != 2 {
		t.Errorf("A should have 2 dependents, got %v", idsOf(dependents))
	}

	// Idempotent re-add
	if err := store.AddDependency(ctx, &types.Dependency{
		IssueID: "B", DependsOnID: "A", Type: "blocks",
	}, "tester"); err != nil {
		t.Errorf("idempotent re-add failed: %v", err)
	}

	// Remove B->A
	if err := store.RemoveDependency(ctx, "B", "A", "tester"); err != nil {
		t.Fatalf("RemoveDependency: %v", err)
	}
	deps, _ = store.GetDependencies(ctx, "B")
	if len(deps) != 0 {
		t.Errorf("after remove, B should have no deps, got %v", idsOf(deps))
	}

	// Idempotent remove
	if err := store.RemoveDependency(ctx, "B", "A", "tester"); err != nil {
		t.Errorf("idempotent remove failed: %v", err)
	}
}

// ════════════════════════════════════════════════════════════════════════
// Labels
// ════════════════════════════════════════════════════════════════════════

func TestLabels_AddRemoveQuery(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "X", "Labeled issue")
	createIssue(t, ctx, store, "Y", "Other labeled issue")

	if err := store.AddLabel(ctx, "X", "urgent", "tester"); err != nil {
		t.Fatalf("AddLabel: %v", err)
	}
	if err := store.AddLabel(ctx, "X", "frontend", "tester"); err != nil {
		t.Fatalf("AddLabel: %v", err)
	}
	if err := store.AddLabel(ctx, "Y", "urgent", "tester"); err != nil {
		t.Fatalf("AddLabel: %v", err)
	}

	labels, err := store.GetLabels(ctx, "X")
	if err != nil {
		t.Fatalf("GetLabels: %v", err)
	}
	if len(labels) != 2 {
		t.Errorf("X should have 2 labels, got %v", labels)
	}

	urgentIssues, err := store.GetIssuesByLabel(ctx, "urgent")
	if err != nil {
		t.Fatalf("GetIssuesByLabel: %v", err)
	}
	if len(urgentIssues) != 2 {
		t.Errorf("'urgent' label should match 2 issues, got %v", idsOf(urgentIssues))
	}

	// Idempotent re-add of an existing label is a no-op
	if err := store.AddLabel(ctx, "X", "urgent", "tester"); err != nil {
		t.Errorf("idempotent re-add: %v", err)
	}

	// Remove
	if err := store.RemoveLabel(ctx, "X", "urgent", "tester"); err != nil {
		t.Fatalf("RemoveLabel: %v", err)
	}
	labels, _ = store.GetLabels(ctx, "X")
	if len(labels) != 1 || labels[0] != "frontend" {
		t.Errorf("after remove, X should have ['frontend'], got %v", labels)
	}

	// Re-add a soft-deleted label should resurrect it
	if err := store.AddLabel(ctx, "X", "urgent", "tester"); err != nil {
		t.Fatalf("re-add after remove: %v", err)
	}
	labels, _ = store.GetLabels(ctx, "X")
	if len(labels) != 2 {
		t.Errorf("after re-add, X should have 2 labels, got %v", labels)
	}
}

// ════════════════════════════════════════════════════════════════════════
// GetReadyWork + GetBlockedIssues (the bd ready / bd blocked entrypoint)
// ════════════════════════════════════════════════════════════════════════

func TestGetReadyWork(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// Setup:
	//   alpha (open, no blockers) → ready
	//   beta  (open, blocked by alpha) → NOT ready
	//   gamma (closed) → not in ready
	//   delta (open, no blockers, but assigned) → ready
	createIssue(t, ctx, store, "alpha", "alpha")
	createIssue(t, ctx, store, "beta", "beta")
	createIssue(t, ctx, store, "gamma", "gamma")
	createIssue(t, ctx, store, "delta", "delta")

	if err := store.AddDependency(ctx, &types.Dependency{
		IssueID: "beta", DependsOnID: "alpha", Type: "blocks",
	}, "tester"); err != nil {
		t.Fatalf("AddDependency: %v", err)
	}
	if err := store.CloseIssue(ctx, "gamma", "done", "tester", "session"); err != nil {
		t.Fatalf("CloseIssue: %v", err)
	}
	assignee := "polecat-1"
	if err := store.UpdateIssue(ctx, "delta",
		map[string]interface{}{"assignee": assignee}, "tester"); err != nil {
		t.Fatalf("UpdateIssue: %v", err)
	}

	// All-ready (no filter except rig)
	ready, err := store.GetReadyWork(ctx, types.WorkFilter{})
	if err != nil {
		t.Fatalf("GetReadyWork: %v", err)
	}
	got := idsOf(ready)
	want := map[string]bool{"alpha": true, "delta": true}
	if len(got) != 2 {
		t.Errorf("expected 2 ready issues, got %v", got)
	}
	for _, id := range got {
		if !want[id] {
			t.Errorf("unexpected issue %q in ready set", id)
		}
	}

	// Filter by unassigned
	readyUnassigned, err := store.GetReadyWork(ctx, types.WorkFilter{Unassigned: true})
	if err != nil {
		t.Fatalf("GetReadyWork unassigned: %v", err)
	}
	if got = idsOf(readyUnassigned); len(got) != 1 || got[0] != "alpha" {
		t.Errorf("unassigned ready expected [alpha], got %v", got)
	}

	// Filter by specific assignee
	readyAssigned, err := store.GetReadyWork(ctx, types.WorkFilter{Assignee: &assignee})
	if err != nil {
		t.Fatalf("GetReadyWork assigned: %v", err)
	}
	if got = idsOf(readyAssigned); len(got) != 1 || got[0] != "delta" {
		t.Errorf("assigned ready expected [delta], got %v", got)
	}
}

func TestGetBlockedIssues(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "blocker-1", "blocker 1")
	createIssue(t, ctx, store, "blocker-2", "blocker 2")
	createIssue(t, ctx, store, "blocked-1", "blocked by 1")
	createIssue(t, ctx, store, "blocked-2", "blocked by 2")
	createIssue(t, ctx, store, "free-1", "no blockers")

	for _, dep := range []*types.Dependency{
		{IssueID: "blocked-1", DependsOnID: "blocker-1", Type: "blocks"},
		{IssueID: "blocked-2", DependsOnID: "blocker-1", Type: "blocks"},
		{IssueID: "blocked-2", DependsOnID: "blocker-2", Type: "blocks"},
	} {
		if err := store.AddDependency(ctx, dep, "tester"); err != nil {
			t.Fatalf("AddDependency: %v", err)
		}
	}

	blocked, err := store.GetBlockedIssues(ctx, types.WorkFilter{})
	if err != nil {
		t.Fatalf("GetBlockedIssues: %v", err)
	}

	// Expect blocked-1 (1 blocker) and blocked-2 (2 blockers).
	wantCounts := map[string]int{"blocked-1": 1, "blocked-2": 2}
	if len(blocked) != 2 {
		t.Fatalf("expected 2 blocked, got %d (%v)", len(blocked), blocked)
	}
	for _, b := range blocked {
		want, ok := wantCounts[b.ID]
		if !ok {
			t.Errorf("unexpected blocked issue: %s", b.ID)
			continue
		}
		if b.BlockedByCount != want {
			t.Errorf("%s blocked_by_count: got %d, want %d", b.ID, b.BlockedByCount, want)
		}
	}
}

// helper: extract IDs from a slice of *types.Issue / *types.BlockedIssue.
func idsOf(items interface{}) []string {
	switch v := items.(type) {
	case []*types.Issue:
		out := make([]string, len(v))
		for i, x := range v {
			out[i] = x.ID
		}
		return out
	case []*types.BlockedIssue:
		out := make([]string, len(v))
		for i, x := range v {
			out[i] = x.ID
		}
		return out
	default:
		return nil
	}
}
