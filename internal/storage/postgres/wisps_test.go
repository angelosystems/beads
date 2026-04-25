package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// createWisp inserts a wisp with sensible defaults.
func createWisp(t *testing.T, ctx context.Context, store *PostgresStore, id, title string) {
	t.Helper()
	in := &types.Issue{
		ID:        id,
		Title:     title,
		Status:    types.Status("open"),
		Priority:  2,
		IssueType: types.IssueType("task"),
	}
	if err := store.CreateWisp(ctx, in, "tester"); err != nil {
		t.Fatalf("CreateWisp %s: %v", id, err)
	}
}

func TestWispCreateAndGet(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	in := &types.Issue{
		ID:          "w-001",
		Title:       "First wisp",
		Description: "ephemeral helper",
		Status:      types.Status("open"),
		Priority:    1,
		IssueType:   types.IssueType("task"),
		CreatedBy:   "tester",
	}
	if err := store.CreateWisp(ctx, in, "tester"); err != nil {
		t.Fatalf("CreateWisp: %v", err)
	}

	out, err := store.GetWisp(ctx, "w-001")
	if err != nil {
		t.Fatalf("GetWisp: %v", err)
	}
	if out.ID != in.ID {
		t.Errorf("ID mismatch: got %q, want %q", out.ID, in.ID)
	}
	if out.Title != in.Title {
		t.Errorf("Title mismatch: got %q, want %q", out.Title, in.Title)
	}
	if out.Status != in.Status {
		t.Errorf("Status mismatch: got %q, want %q", out.Status, in.Status)
	}
	if out.Priority != in.Priority {
		t.Errorf("Priority mismatch: got %d, want %d", out.Priority, in.Priority)
	}
	if out.CreatedAt.IsZero() {
		t.Error("CreatedAt should be populated by DB default")
	}
}

func TestWispGet_NotFound(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	_, err := store.GetWisp(ctx, "nope")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestWispUpdate(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createWisp(t, ctx, store, "w-002", "Original wisp")

	updates := map[string]interface{}{
		"title":    "Updated wisp",
		"priority": 0,
		"assignee": "agent-7",
	}
	if err := store.UpdateWisp(ctx, "w-002", updates, "tester"); err != nil {
		t.Fatalf("UpdateWisp: %v", err)
	}

	out, err := store.GetWisp(ctx, "w-002")
	if err != nil {
		t.Fatalf("GetWisp: %v", err)
	}
	if out.Title != "Updated wisp" {
		t.Errorf("Title not updated: %q", out.Title)
	}
	if out.Priority != 0 {
		t.Errorf("Priority not updated: %d", out.Priority)
	}
	if out.Assignee != "agent-7" {
		t.Errorf("Assignee not updated: %q", out.Assignee)
	}
}

func TestWispUpdate_NotFound(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	err := store.UpdateWisp(ctx, "ghost", map[string]interface{}{"title": "x"}, "tester")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestWispClose(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createWisp(t, ctx, store, "w-003", "Will be closed")

	if err := store.CloseWisp(ctx, "w-003", "completed", "tester", "session-abc"); err != nil {
		t.Fatalf("CloseWisp: %v", err)
	}

	out, err := store.GetWisp(ctx, "w-003")
	if err != nil {
		t.Fatalf("GetWisp: %v", err)
	}
	if out.Status != types.Status("closed") {
		t.Errorf("expected status=closed, got %q", out.Status)
	}
	if out.CloseReason != "completed" {
		t.Errorf("expected close_reason=completed, got %q", out.CloseReason)
	}
	if out.ClosedBySession != "session-abc" {
		t.Errorf("expected closed_by_session=session-abc, got %q", out.ClosedBySession)
	}
	if out.ClosedAt == nil {
		t.Error("ClosedAt should be populated")
	}

	// Idempotent re-close is a no-op.
	if err := store.CloseWisp(ctx, "w-003", "again", "tester", "session-xyz"); err != nil {
		t.Errorf("idempotent re-close failed: %v", err)
	}
	out2, _ := store.GetWisp(ctx, "w-003")
	if out2.CloseReason != "completed" {
		t.Errorf("re-close should not overwrite reason; got %q", out2.CloseReason)
	}
}

func TestListWisps_Filters(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// alpha (open, task)
	// beta  (open, bug)
	// gamma (open, task) -> assignee=agent-1
	// delta (closed, task)
	createWisp(t, ctx, store, "alpha", "alpha")
	createWisp(t, ctx, store, "beta", "beta")
	if err := store.UpdateWisp(ctx, "beta",
		map[string]interface{}{"issue_type": "bug"}, "tester"); err != nil {
		t.Fatalf("UpdateWisp beta: %v", err)
	}
	createWisp(t, ctx, store, "gamma", "gamma")
	if err := store.UpdateWisp(ctx, "gamma",
		map[string]interface{}{"assignee": "agent-1"}, "tester"); err != nil {
		t.Fatalf("UpdateWisp gamma: %v", err)
	}
	createWisp(t, ctx, store, "delta", "delta")
	if err := store.CloseWisp(ctx, "delta", "done", "tester", "s"); err != nil {
		t.Fatalf("CloseWisp delta: %v", err)
	}

	// Default: non-closed wisps only.
	all, err := store.ListWisps(ctx, types.WispFilter{})
	if err != nil {
		t.Fatalf("ListWisps: %v", err)
	}
	gotIDs := wispIDs(all)
	if len(gotIDs) != 3 {
		t.Errorf("default ListWisps expected 3 non-closed, got %v", gotIDs)
	}
	for _, id := range gotIDs {
		if id == "delta" {
			t.Errorf("default ListWisps should exclude closed wisps; saw %q", id)
		}
	}

	// IncludeClosed=true: all 4.
	allInc, err := store.ListWisps(ctx, types.WispFilter{IncludeClosed: true})
	if err != nil {
		t.Fatalf("ListWisps inc: %v", err)
	}
	if len(allInc) != 4 {
		t.Errorf("IncludeClosed expected 4, got %d", len(allInc))
	}

	// Filter by Type=bug.
	bugType := types.IssueType("bug")
	bugs, err := store.ListWisps(ctx, types.WispFilter{Type: &bugType})
	if err != nil {
		t.Fatalf("ListWisps bug: %v", err)
	}
	if got := wispIDs(bugs); len(got) != 1 || got[0] != "beta" {
		t.Errorf("Type=bug expected [beta], got %v", got)
	}

	// Filter by Status=closed.
	closedStatus := types.Status("closed")
	closed, err := store.ListWisps(ctx, types.WispFilter{Status: &closedStatus})
	if err != nil {
		t.Fatalf("ListWisps closed: %v", err)
	}
	if got := wispIDs(closed); len(got) != 1 || got[0] != "delta" {
		t.Errorf("Status=closed expected [delta], got %v", got)
	}

	// Limit.
	lim, err := store.ListWisps(ctx, types.WispFilter{Limit: 2})
	if err != nil {
		t.Fatalf("ListWisps limit: %v", err)
	}
	if len(lim) != 2 {
		t.Errorf("Limit=2 expected 2 results, got %d", len(lim))
	}

	// UpdatedAfter in the future excludes everything.
	future := time.Now().Add(1 * time.Hour)
	none, err := store.ListWisps(ctx, types.WispFilter{UpdatedAfter: &future})
	if err != nil {
		t.Fatalf("ListWisps updatedAfter: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("UpdatedAfter=future expected 0, got %d", len(none))
	}
}

// TestListWisps_RigIsolation verifies that ListWisps does not leak across rigs
// or read from the issues table.
func TestListWisps_RigIsolation(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// Insert into beads.issues — must NOT show up in ListWisps.
	createIssue(t, ctx, store, "issue-only", "from issues table")

	// Insert a wisp.
	createWisp(t, ctx, store, "wisp-only", "from wisps table")

	out, err := store.ListWisps(ctx, types.WispFilter{})
	if err != nil {
		t.Fatalf("ListWisps: %v", err)
	}
	got := wispIDs(out)
	if len(got) != 1 || got[0] != "wisp-only" {
		t.Errorf("ListWisps should only return wisps, got %v", got)
	}
}

func wispIDs(items []*types.Issue) []string {
	out := make([]string, len(items))
	for i, x := range items {
		out[i] = x.ID
	}
	return out
}
