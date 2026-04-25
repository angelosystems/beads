package postgres

import (
	"context"
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

// ════════════════════════════════════════════════════════════════════════
// GetStatistics
// ════════════════════════════════════════════════════════════════════════

func TestStatistics_MixedSetup(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// Setup:
	//   open-1, open-2, open-3 — open
	//   wip-1                  — in_progress
	//   closed-1               — closed
	//   blocked-1              — open but blocked by open-1
	//   epic-done              — epic, with all children closed -> eligible for closure
	//   epic-open              — epic, with one open child -> NOT eligible
	createIssue(t, ctx, store, "open-1", "open 1")
	createIssue(t, ctx, store, "open-2", "open 2")
	createIssue(t, ctx, store, "open-3", "open 3")
	createIssue(t, ctx, store, "wip-1", "wip")
	createIssue(t, ctx, store, "closed-1", "closed")
	createIssue(t, ctx, store, "blocked-1", "blocked")

	// in_progress for wip-1
	if err := store.UpdateIssue(ctx, "wip-1",
		map[string]interface{}{"status": "in_progress"}, "tester"); err != nil {
		t.Fatalf("UpdateIssue wip-1: %v", err)
	}

	// close closed-1
	if err := store.CloseIssue(ctx, "closed-1", "done", "tester", "session-x"); err != nil {
		t.Fatalf("CloseIssue closed-1: %v", err)
	}

	// blocked-1 depends on open-1 (a non-closed blocker -> blocked)
	if err := store.AddDependency(ctx, &types.Dependency{
		IssueID: "blocked-1", DependsOnID: "open-1", Type: "blocks",
	}, "tester"); err != nil {
		t.Fatalf("AddDependency blocked-1: %v", err)
	}

	// Epics + parent-child children
	createIssue(t, ctx, store, "epic-done", "Epic with all closed children")
	createIssue(t, ctx, store, "epic-open", "Epic with one open child")
	if err := store.UpdateIssue(ctx, "epic-done",
		map[string]interface{}{"issue_type": "epic"}, "tester"); err != nil {
		t.Fatalf("set epic-done type: %v", err)
	}
	if err := store.UpdateIssue(ctx, "epic-open",
		map[string]interface{}{"issue_type": "epic"}, "tester"); err != nil {
		t.Fatalf("set epic-open type: %v", err)
	}

	createIssue(t, ctx, store, "child-done-1", "child of epic-done #1")
	createIssue(t, ctx, store, "child-done-2", "child of epic-done #2")
	createIssue(t, ctx, store, "child-open-1", "child of epic-open #1 (closed)")
	createIssue(t, ctx, store, "child-open-2", "child of epic-open #2 (open)")

	for _, dep := range []*types.Dependency{
		{IssueID: "child-done-1", DependsOnID: "epic-done", Type: "parent-child"},
		{IssueID: "child-done-2", DependsOnID: "epic-done", Type: "parent-child"},
		{IssueID: "child-open-1", DependsOnID: "epic-open", Type: "parent-child"},
		{IssueID: "child-open-2", DependsOnID: "epic-open", Type: "parent-child"},
	} {
		if err := store.AddDependency(ctx, dep, "tester"); err != nil {
			t.Fatalf("AddDependency parent-child: %v", err)
		}
	}

	// Close all of epic-done's children + only one of epic-open's children.
	for _, id := range []string{"child-done-1", "child-done-2", "child-open-1"} {
		if err := store.CloseIssue(ctx, id, "ok", "tester", "session-y"); err != nil {
			t.Fatalf("CloseIssue %s: %v", id, err)
		}
	}

	stats, err := store.GetStatistics(ctx)
	if err != nil {
		t.Fatalf("GetStatistics: %v", err)
	}

	// Total = 12 (open-1..3, wip-1, closed-1, blocked-1, epic-done, epic-open,
	// child-done-1..2, child-open-1..2)
	if stats.TotalIssues != 12 {
		t.Errorf("TotalIssues: got %d, want 12", stats.TotalIssues)
	}
	// Closed = 4 (closed-1, child-done-1..2, child-open-1)
	if stats.ClosedIssues != 4 {
		t.Errorf("ClosedIssues: got %d, want 4", stats.ClosedIssues)
	}
	// In progress = 1 (wip-1)
	if stats.InProgressIssues != 1 {
		t.Errorf("InProgressIssues: got %d, want 1", stats.InProgressIssues)
	}
	// Open status = total - closed - in_progress = 12 - 4 - 1 = 7
	if stats.OpenIssues != 7 {
		t.Errorf("OpenIssues: got %d, want 7", stats.OpenIssues)
	}
	// blocked-1 is the only blocked-by-open-deps issue
	if stats.BlockedIssues != 1 {
		t.Errorf("BlockedIssues: got %d, want 1", stats.BlockedIssues)
	}
	// EpicsEligibleForClosure = 1 (epic-done)
	if stats.EpicsEligibleForClosure != 1 {
		t.Errorf("EpicsEligibleForClosure: got %d, want 1", stats.EpicsEligibleForClosure)
	}
	// Sanity: ready_issues view returns >= 1 (open-1..3 etc. are ready)
	if stats.ReadyIssues < 1 {
		t.Errorf("ReadyIssues: got %d, want >= 1", stats.ReadyIssues)
	}
}

// ════════════════════════════════════════════════════════════════════════
// GetEpicsEligibleForClosure
// ════════════════════════════════════════════════════════════════════════

func TestEpicsEligibleForClosure(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "epic-A", "epic A — all closed")
	createIssue(t, ctx, store, "epic-B", "epic B — one open")
	createIssue(t, ctx, store, "epic-C", "epic C — no children")
	if err := store.UpdateIssue(ctx, "epic-A",
		map[string]interface{}{"issue_type": "epic"}, "tester"); err != nil {
		t.Fatalf("set epic-A type: %v", err)
	}
	if err := store.UpdateIssue(ctx, "epic-B",
		map[string]interface{}{"issue_type": "epic"}, "tester"); err != nil {
		t.Fatalf("set epic-B type: %v", err)
	}
	if err := store.UpdateIssue(ctx, "epic-C",
		map[string]interface{}{"issue_type": "epic"}, "tester"); err != nil {
		t.Fatalf("set epic-C type: %v", err)
	}

	createIssue(t, ctx, store, "A-c1", "A child 1")
	createIssue(t, ctx, store, "A-c2", "A child 2")
	createIssue(t, ctx, store, "B-c1", "B child 1 (closed)")
	createIssue(t, ctx, store, "B-c2", "B child 2 (open)")

	for _, dep := range []*types.Dependency{
		{IssueID: "A-c1", DependsOnID: "epic-A", Type: "parent-child"},
		{IssueID: "A-c2", DependsOnID: "epic-A", Type: "parent-child"},
		{IssueID: "B-c1", DependsOnID: "epic-B", Type: "parent-child"},
		{IssueID: "B-c2", DependsOnID: "epic-B", Type: "parent-child"},
	} {
		if err := store.AddDependency(ctx, dep, "tester"); err != nil {
			t.Fatalf("AddDependency: %v", err)
		}
	}

	// Close all of A's children, only one of B's.
	for _, id := range []string{"A-c1", "A-c2", "B-c1"} {
		if err := store.CloseIssue(ctx, id, "ok", "tester", "session"); err != nil {
			t.Fatalf("CloseIssue %s: %v", id, err)
		}
	}

	out, err := store.GetEpicsEligibleForClosure(ctx)
	if err != nil {
		t.Fatalf("GetEpicsEligibleForClosure: %v", err)
	}

	if len(out) != 1 {
		t.Fatalf("expected exactly 1 eligible epic, got %d (%v)", len(out), epicIDs(out))
	}
	if out[0].Epic.ID != "epic-A" {
		t.Errorf("expected epic-A eligible, got %s", out[0].Epic.ID)
	}
	if out[0].TotalChildren != 2 {
		t.Errorf("epic-A TotalChildren: got %d, want 2", out[0].TotalChildren)
	}
	if out[0].ClosedChildren != 2 {
		t.Errorf("epic-A ClosedChildren: got %d, want 2", out[0].ClosedChildren)
	}
	if !out[0].EligibleForClose {
		t.Errorf("epic-A: EligibleForClose should be true")
	}

	// Now close B-c2 — epic-B should also become eligible.
	if err := store.CloseIssue(ctx, "B-c2", "ok", "tester", "session"); err != nil {
		t.Fatalf("CloseIssue B-c2: %v", err)
	}
	out, err = store.GetEpicsEligibleForClosure(ctx)
	if err != nil {
		t.Fatalf("GetEpicsEligibleForClosure (after closing B-c2): %v", err)
	}
	if len(out) != 2 {
		t.Errorf("expected 2 eligible epics after closing B-c2, got %d (%v)",
			len(out), epicIDs(out))
	}
	// epic-C has no children → must NOT be in the list either before or after.
	for _, e := range out {
		if e.Epic.ID == "epic-C" {
			t.Errorf("epic-C has no children — must not be eligible")
		}
	}
}

// ════════════════════════════════════════════════════════════════════════
// GetDependenciesWithMetadata + GetDependentsWithMetadata
// ════════════════════════════════════════════════════════════════════════

func TestDependenciesWithMetadata(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "P", "parent epic")
	createIssue(t, ctx, store, "C", "child")
	createIssue(t, ctx, store, "X", "blocker of C")

	// C depends on P (parent-child) and on X (blocks).
	if err := store.AddDependency(ctx, &types.Dependency{
		IssueID: "C", DependsOnID: "P", Type: "parent-child",
	}, "tester"); err != nil {
		t.Fatalf("AddDependency C->P: %v", err)
	}
	if err := store.AddDependency(ctx, &types.Dependency{
		IssueID: "C", DependsOnID: "X", Type: "blocks",
	}, "tester"); err != nil {
		t.Fatalf("AddDependency C->X: %v", err)
	}

	deps, err := store.GetDependenciesWithMetadata(ctx, "C")
	if err != nil {
		t.Fatalf("GetDependenciesWithMetadata: %v", err)
	}
	if len(deps) != 2 {
		t.Fatalf("expected 2 deps for C, got %d", len(deps))
	}
	got := map[string]types.DependencyType{}
	for _, d := range deps {
		got[d.ID] = d.DependencyType
	}
	if got["P"] != types.DepParentChild {
		t.Errorf("C->P dependency type: got %q, want %q", got["P"], types.DepParentChild)
	}
	if got["X"] != types.DepBlocks {
		t.Errorf("C->X dependency type: got %q, want %q", got["X"], types.DepBlocks)
	}
}

func TestDependentsWithMetadata(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "T", "the target")
	createIssue(t, ctx, store, "U", "depends on T (blocks)")
	createIssue(t, ctx, store, "V", "depends on T (parent-child)")

	if err := store.AddDependency(ctx, &types.Dependency{
		IssueID: "U", DependsOnID: "T", Type: "blocks",
	}, "tester"); err != nil {
		t.Fatalf("AddDependency U->T: %v", err)
	}
	if err := store.AddDependency(ctx, &types.Dependency{
		IssueID: "V", DependsOnID: "T", Type: "parent-child",
	}, "tester"); err != nil {
		t.Fatalf("AddDependency V->T: %v", err)
	}

	dependents, err := store.GetDependentsWithMetadata(ctx, "T")
	if err != nil {
		t.Fatalf("GetDependentsWithMetadata: %v", err)
	}
	if len(dependents) != 2 {
		t.Fatalf("expected 2 dependents for T, got %d", len(dependents))
	}
	got := map[string]types.DependencyType{}
	for _, d := range dependents {
		got[d.ID] = d.DependencyType
	}
	if got["U"] != types.DepBlocks {
		t.Errorf("U->T dependency type: got %q, want %q", got["U"], types.DepBlocks)
	}
	if got["V"] != types.DepParentChild {
		t.Errorf("V->T dependency type: got %q, want %q", got["V"], types.DepParentChild)
	}
}

// helper: extract epic IDs from a slice of *types.EpicStatus
func epicIDs(items []*types.EpicStatus) []string {
	out := make([]string, len(items))
	for i, e := range items {
		if e.Epic != nil {
			out[i] = e.Epic.ID
		}
	}
	return out
}
