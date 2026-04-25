package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// addDep is a small helper to record "issueID depends on dependsOn" in tests.
func addDep(t *testing.T, ctx context.Context, store *PostgresStore, issueID, dependsOn string) {
	t.Helper()
	dep := &types.Dependency{
		IssueID:     issueID,
		DependsOnID: dependsOn,
		Type:        types.DepBlocks,
	}
	if err := store.AddDependency(ctx, dep, "tester"); err != nil {
		t.Fatalf("AddDependency %s -> %s: %v", issueID, dependsOn, err)
	}
}

// findNode locates the first TreeNode with the given issue id. Returns nil
// when missing — callers use this for both presence and absence assertions.
func findNode(nodes []*types.TreeNode, id string) *types.TreeNode {
	for _, n := range nodes {
		if n.Issue.ID == id {
			return n
		}
	}
	return nil
}

// countNodes counts how many TreeNodes carry the given issue id.
func countNodes(nodes []*types.TreeNode, id string) int {
	n := 0
	for _, x := range nodes {
		if x.Issue.ID == id {
			n++
		}
	}
	return n
}

// ════════════════════════════════════════════════════════════════════════
// GetDependencyTree tests
// ════════════════════════════════════════════════════════════════════════

// TestDependencyTree_NotFound checks that a missing root returns ErrNotFound.
func TestDependencyTree_NotFound(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	_, err := store.GetDependencyTree(ctx, "no-such-issue", 5, false, false)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// TestDependencyTree_LinearChain walks A -> B -> C -> D and asserts depth +
// parent_id are correctly populated for every node.
func TestDependencyTree_LinearChain(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "A", "A")
	createIssue(t, ctx, store, "B", "B")
	createIssue(t, ctx, store, "C", "C")
	createIssue(t, ctx, store, "D", "D")

	// A depends on B, B depends on C, C depends on D
	addDep(t, ctx, store, "A", "B")
	addDep(t, ctx, store, "B", "C")
	addDep(t, ctx, store, "C", "D")

	tree, err := store.GetDependencyTree(ctx, "A", 10, false, false)
	if err != nil {
		t.Fatalf("GetDependencyTree: %v", err)
	}
	if len(tree) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(tree))
	}

	// Depth + parent_id for each known issue.
	want := map[string]struct {
		depth  int
		parent string
	}{
		"A": {0, ""},
		"B": {1, "A"},
		"C": {2, "B"},
		"D": {3, "C"},
	}
	for id, w := range want {
		n := findNode(tree, id)
		if n == nil {
			t.Errorf("missing node %s", id)
			continue
		}
		if n.Depth != w.depth {
			t.Errorf("%s: depth=%d, want %d", id, n.Depth, w.depth)
		}
		if n.ParentID != w.parent {
			t.Errorf("%s: parent_id=%q, want %q", id, n.ParentID, w.parent)
		}
		if n.Truncated {
			t.Errorf("%s: truncated=true unexpectedly", id)
		}
	}
}

// TestDependencyTree_MaxDepthTruncated stops a deep chain at depth 2 and
// verifies the boundary node carries truncated=true.
func TestDependencyTree_MaxDepthTruncated(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	for _, id := range []string{"r0", "r1", "r2", "r3", "r4"} {
		createIssue(t, ctx, store, id, id)
	}
	// r0 -> r1 -> r2 -> r3 -> r4
	addDep(t, ctx, store, "r0", "r1")
	addDep(t, ctx, store, "r1", "r2")
	addDep(t, ctx, store, "r2", "r3")
	addDep(t, ctx, store, "r3", "r4")

	tree, err := store.GetDependencyTree(ctx, "r0", 2, false, false)
	if err != nil {
		t.Fatalf("GetDependencyTree: %v", err)
	}

	// Expect r0 (depth 0), r1 (depth 1), r2 (depth 2). r3 must NOT appear.
	if findNode(tree, "r3") != nil {
		t.Error("r3 should be truncated out of the result")
	}
	r2 := findNode(tree, "r2")
	if r2 == nil {
		t.Fatal("r2 missing from tree")
	}
	if r2.Depth != 2 {
		t.Errorf("r2 depth=%d, want 2", r2.Depth)
	}
	if !r2.Truncated {
		t.Error("r2 should be marked truncated (r3 was reachable)")
	}

	// r1 is below the depth limit and has further children, but it's *not*
	// at the boundary, so it must NOT be truncated.
	r1 := findNode(tree, "r1")
	if r1 == nil {
		t.Fatal("r1 missing from tree")
	}
	if r1.Truncated {
		t.Error("r1 should not be marked truncated (depth < limit)")
	}
}

// TestDependencyTree_BranchingNoDepth verifies that an unbounded request
// (maxDepth <= 0) walks all branches.
func TestDependencyTree_BranchingNoDepth(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// A has two children B and C; B has a grandchild D.
	for _, id := range []string{"A", "B", "C", "D"} {
		createIssue(t, ctx, store, id, id)
	}
	addDep(t, ctx, store, "A", "B")
	addDep(t, ctx, store, "A", "C")
	addDep(t, ctx, store, "B", "D")

	tree, err := store.GetDependencyTree(ctx, "A", 0, false, false)
	if err != nil {
		t.Fatalf("GetDependencyTree: %v", err)
	}
	if len(tree) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(tree))
	}

	// A is at depth 0, B/C at depth 1, D at depth 2.
	depths := map[string]int{}
	for _, n := range tree {
		depths[n.Issue.ID] = n.Depth
	}
	want := map[string]int{"A": 0, "B": 1, "C": 1, "D": 2}
	for id, d := range want {
		if got, ok := depths[id]; !ok || got != d {
			t.Errorf("%s: depth=%d (present=%v), want %d", id, got, ok, d)
		}
	}
}

// TestDependencyTree_Reverse walks the dependents direction.
// Setup: leaf -> mid -> root (mid depends on root, leaf depends on mid).
// Reverse tree from root must include root + mid + leaf.
func TestDependencyTree_Reverse(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "root", "root")
	createIssue(t, ctx, store, "mid", "mid")
	createIssue(t, ctx, store, "leaf", "leaf")

	addDep(t, ctx, store, "mid", "root")
	addDep(t, ctx, store, "leaf", "mid")

	tree, err := store.GetDependencyTree(ctx, "root", 5, false, true)
	if err != nil {
		t.Fatalf("GetDependencyTree reverse: %v", err)
	}
	if len(tree) != 3 {
		t.Fatalf("expected 3 nodes in reverse tree, got %d", len(tree))
	}

	// root -> mid -> leaf in the reverse direction.
	rootN := findNode(tree, "root")
	midN := findNode(tree, "mid")
	leafN := findNode(tree, "leaf")
	if rootN == nil || midN == nil || leafN == nil {
		t.Fatalf("missing nodes: root=%v mid=%v leaf=%v", rootN, midN, leafN)
	}
	if rootN.Depth != 0 || rootN.ParentID != "" {
		t.Errorf("root: depth=%d parent=%q", rootN.Depth, rootN.ParentID)
	}
	if midN.Depth != 1 || midN.ParentID != "root" {
		t.Errorf("mid: depth=%d parent=%q", midN.Depth, midN.ParentID)
	}
	if leafN.Depth != 2 || leafN.ParentID != "mid" {
		t.Errorf("leaf: depth=%d parent=%q", leafN.Depth, leafN.ParentID)
	}
}

// TestDependencyTree_DedupeShowAllPaths constructs a diamond graph and
// verifies that:
//   - showAllPaths=false returns the converging node exactly once.
//   - showAllPaths=true returns it twice (once per path).
func TestDependencyTree_DedupeShowAllPaths(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// Diamond:
	//   top depends on left and right
	//   both left and right depend on bottom
	for _, id := range []string{"top", "left", "right", "bottom"} {
		createIssue(t, ctx, store, id, id)
	}
	addDep(t, ctx, store, "top", "left")
	addDep(t, ctx, store, "top", "right")
	addDep(t, ctx, store, "left", "bottom")
	addDep(t, ctx, store, "right", "bottom")

	// Dedup case: bottom appears once.
	dedup, err := store.GetDependencyTree(ctx, "top", 5, false, false)
	if err != nil {
		t.Fatalf("GetDependencyTree dedup: %v", err)
	}
	if got := countNodes(dedup, "bottom"); got != 1 {
		t.Errorf("dedup: bottom appeared %d times, want 1", got)
	}
	if len(dedup) != 4 {
		t.Errorf("dedup: expected 4 nodes (top, left, right, bottom), got %d", len(dedup))
	}

	// All-paths case: bottom appears twice (one per path through left/right).
	all, err := store.GetDependencyTree(ctx, "top", 5, true, false)
	if err != nil {
		t.Fatalf("GetDependencyTree all paths: %v", err)
	}
	if got := countNodes(all, "bottom"); got != 2 {
		t.Errorf("all-paths: bottom appeared %d times, want 2", got)
	}
}

// TestDependencyTree_RootOnly verifies that a node with no edges returns just
// itself with depth=0 and no truncation.
func TestDependencyTree_RootOnly(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "solo", "solo")

	tree, err := store.GetDependencyTree(ctx, "solo", 5, false, false)
	if err != nil {
		t.Fatalf("GetDependencyTree: %v", err)
	}
	if len(tree) != 1 {
		t.Fatalf("expected 1 node, got %d", len(tree))
	}
	if tree[0].Issue.ID != "solo" || tree[0].Depth != 0 || tree[0].Truncated {
		t.Errorf("solo node wrong: id=%q depth=%d truncated=%v",
			tree[0].Issue.ID, tree[0].Depth, tree[0].Truncated)
	}
}
