package postgres

import (
	"context"
	"sort"
	"testing"

	"github.com/steveyegge/beads/internal/types"
)

// seedSearchCorpus populates a small, predictable corpus used by the search
// tests. Returns a map of id -> issue title for diagnostic messages.
//
// Layout:
//
//	s-alpha    open       p2 task    (assignee=alice, labels=[urgent, frontend])
//	s-bravo    in_progress p1 bug    (assignee=bob, labels=[urgent, backend])
//	s-charlie  open       p0 task    (assignee=alice, labels=[backend, docs])
//	s-delta    closed     p3 task    (no labels)
//	s-echo     open       p1 feature (labels=[tech-debt])
//	bd-foxtrot open       p2 task    (different prefix; labels=[frontend])
func seedSearchCorpus(t *testing.T, ctx context.Context, store *PostgresStore) {
	t.Helper()

	type issueSpec struct {
		id, title, desc string
		status          types.Status
		priority        int
		issueType       types.IssueType
		assignee        string
		labels          []string
	}
	specs := []issueSpec{
		{"s-alpha", "alpha task", "matchneedle in description", types.Status("open"), 2, types.IssueType("task"), "alice", []string{"urgent", "frontend"}},
		{"s-bravo", "bravo running task", "in flight", types.Status("in_progress"), 1, types.IssueType("bug"), "bob", []string{"urgent", "backend"}},
		{"s-charlie", "charlie work matchneedle", "another", types.Status("open"), 0, types.IssueType("task"), "alice", []string{"backend", "docs"}},
		{"s-delta", "delta done", "closed work", types.Status("closed"), 3, types.IssueType("task"), "", nil},
		{"s-echo", "echo feature", "ship it", types.Status("open"), 1, types.IssueType("feature"), "", []string{"tech-debt"}},
		{"bd-foxtrot", "foxtrot bd item", "different prefix", types.Status("open"), 2, types.IssueType("task"), "", []string{"frontend"}},
	}

	for _, sp := range specs {
		in := &types.Issue{
			ID:          sp.id,
			Title:       sp.title,
			Description: sp.desc,
			Status:      sp.status,
			Priority:    sp.priority,
			IssueType:   sp.issueType,
			Assignee:    sp.assignee,
		}
		if err := store.CreateIssue(ctx, in, "tester"); err != nil {
			t.Fatalf("seed CreateIssue %s: %v", sp.id, err)
		}
		for _, lbl := range sp.labels {
			if err := store.AddLabel(ctx, sp.id, lbl, "tester"); err != nil {
				t.Fatalf("seed AddLabel %s/%s: %v", sp.id, lbl, err)
			}
		}
		// Closed issues need a real status transition so the
		// closed_at invariant holds. CreateIssue inserts the status
		// directly; bring s-delta into "closed" via CloseIssue so its
		// closed_at is populated by the DB.
		if sp.status == types.Status("closed") {
			if err := store.CloseIssue(ctx, sp.id, "done", "tester", "session"); err != nil {
				t.Fatalf("seed CloseIssue %s: %v", sp.id, err)
			}
		}
	}
}

func sortedIDs(items []*types.Issue) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.ID
	}
	sort.Strings(out)
	return out
}

func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSearchIssues_FreeText(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	got, err := store.SearchIssues(ctx, "matchneedle", types.IssueFilter{})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"s-alpha", "s-charlie"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("free-text search: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_Status(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	open := types.Status("open")
	got, err := store.SearchIssues(ctx, "", types.IssueFilter{Status: &open})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"bd-foxtrot", "s-alpha", "s-charlie", "s-echo"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("single status: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_Statuses(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	got, err := store.SearchIssues(ctx, "", types.IssueFilter{
		Statuses: []types.Status{types.Status("in_progress"), types.Status("closed")},
	})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"s-bravo", "s-delta"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("multiple statuses: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_Priority(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	p1 := 1
	got, err := store.SearchIssues(ctx, "", types.IssueFilter{Priority: &p1})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"s-bravo", "s-echo"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("priority: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_Assignee(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	alice := "alice"
	got, err := store.SearchIssues(ctx, "", types.IssueFilter{Assignee: &alice})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"s-alpha", "s-charlie"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("assignee: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_LabelsAND(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	// Issues with BOTH "urgent" AND "backend" — only s-bravo qualifies.
	got, err := store.SearchIssues(ctx, "", types.IssueFilter{
		Labels: []string{"urgent", "backend"},
	})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"s-bravo"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("labels AND: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_LabelsAny(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	// Issues with ANY of "frontend", "tech-debt".
	got, err := store.SearchIssues(ctx, "", types.IssueFilter{
		LabelsAny: []string{"frontend", "tech-debt"},
	})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"bd-foxtrot", "s-alpha", "s-echo"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("labels OR: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_ExcludeLabels(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	// Only open issues that do NOT have "urgent".
	open := types.Status("open")
	got, err := store.SearchIssues(ctx, "", types.IssueFilter{
		Status:        &open,
		ExcludeLabels: []string{"urgent"},
	})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"bd-foxtrot", "s-charlie", "s-echo"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("exclude labels: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_LabelPattern(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	// "tech-*" -> "tech-%"
	got, err := store.SearchIssues(ctx, "", types.IssueFilter{
		LabelPattern: "tech-*",
	})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"s-echo"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("label pattern: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_IDs(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	got, err := store.SearchIssues(ctx, "", types.IssueFilter{
		IDs: []string{"s-alpha", "s-delta", "no-such-id"},
	})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"s-alpha", "s-delta"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("IDs filter: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_IDPrefix(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	got, err := store.SearchIssues(ctx, "", types.IssueFilter{IDPrefix: "bd-"})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	gotIDs := sortedIDs(got)
	wantIDs := []string{"bd-foxtrot"}
	if !equalIDs(gotIDs, wantIDs) {
		t.Errorf("IDPrefix: got %v, want %v", gotIDs, wantIDs)
	}
}

func TestSearchIssues_Limit(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()
	seedSearchCorpus(t, ctx, store)

	got, err := store.SearchIssues(ctx, "", types.IssueFilter{Limit: 2})
	if err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("limit: expected 2 results, got %d (%v)", len(got), sortedIDs(got))
	}
	// Results must be ordered by priority ASC, so the first two are P0/P1.
	for _, it := range got {
		if it.Priority > 1 {
			t.Errorf("limit ordering: priority %d should not appear in top-2", it.Priority)
		}
	}
}
