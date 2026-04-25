package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// ════════════════════════════════════════════════════════════════════════
// Bulk + lookup
// ════════════════════════════════════════════════════════════════════════

func TestCreateIssues_Bulk(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	batch := []*types.Issue{
		{ID: "bulk-1", Title: "B1", Status: "open", Priority: 2, IssueType: "task"},
		{ID: "bulk-2", Title: "B2", Status: "open", Priority: 1, IssueType: "bug"},
		{ID: "bulk-3", Title: "B3", Status: "open", Priority: 0, IssueType: "feature"},
	}
	if err := store.CreateIssues(ctx, batch, "tester"); err != nil {
		t.Fatalf("CreateIssues: %v", err)
	}

	got, err := store.GetIssuesByIDs(ctx, []string{"bulk-1", "bulk-2", "bulk-3", "bulk-missing"})
	if err != nil {
		t.Fatalf("GetIssuesByIDs: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("expected 3 issues, got %v", idsOf(got))
	}
}

func TestCreateIssues_Empty(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	if err := store.CreateIssues(ctx, nil, "tester"); err != nil {
		t.Errorf("empty bulk should be a no-op, got %v", err)
	}
	if err := store.CreateIssues(ctx, []*types.Issue{}, "tester"); err != nil {
		t.Errorf("empty slice should be a no-op, got %v", err)
	}
}

func TestGetIssueByExternalRef(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	ref := "gh-42"
	in := &types.Issue{
		ID:          "with-ext",
		Title:       "Has external ref",
		Status:      "open",
		Priority:    2,
		IssueType:   "task",
		ExternalRef: &ref,
	}
	if err := store.CreateIssue(ctx, in, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	out, err := store.GetIssueByExternalRef(ctx, "gh-42")
	if err != nil {
		t.Fatalf("GetIssueByExternalRef: %v", err)
	}
	if out.ID != "with-ext" {
		t.Errorf("expected with-ext, got %s", out.ID)
	}

	_, err = store.GetIssueByExternalRef(ctx, "gh-nonexistent")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// ════════════════════════════════════════════════════════════════════════
// Comments
// ════════════════════════════════════════════════════════════════════════

func TestComments_AddAndGet(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "comm-1", "Issue with comments")

	c1, err := store.AddIssueComment(ctx, "comm-1", "alice", "first comment")
	if err != nil {
		t.Fatalf("AddIssueComment: %v", err)
	}
	if c1.ID == "" {
		t.Error("expected server-assigned UUID for comment")
	}
	if c1.Author != "alice" || c1.Text != "first comment" {
		t.Errorf("wrong fields: author=%q text=%q", c1.Author, c1.Text)
	}
	if c1.CreatedAt.IsZero() {
		t.Error("CreatedAt should be populated")
	}

	c2, err := store.AddIssueComment(ctx, "comm-1", "bob", "second comment")
	if err != nil {
		t.Fatalf("AddIssueComment: %v", err)
	}
	if c2.ID == c1.ID {
		t.Errorf("comments should have unique IDs")
	}

	all, err := store.GetIssueComments(ctx, "comm-1")
	if err != nil {
		t.Fatalf("GetIssueComments: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 comments, got %d", len(all))
	}
	// ordered oldest-first
	if all[0].Author != "alice" || all[1].Author != "bob" {
		t.Errorf("comment order wrong: %v", []string{all[0].Author, all[1].Author})
	}

	// Comments on a different issue should be empty
	createIssue(t, ctx, store, "comm-2", "No comments")
	none, _ := store.GetIssueComments(ctx, "comm-2")
	if len(none) != 0 {
		t.Errorf("expected no comments on comm-2, got %d", len(none))
	}
}

// ════════════════════════════════════════════════════════════════════════
// Config + LocalMetadata
// ════════════════════════════════════════════════════════════════════════

func TestConfig_SetGetAll(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// Initial Get returns ErrNotFound
	_, err := store.GetConfig(ctx, "missing")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	// Set + Get round-trip
	if err := store.SetConfig(ctx, "issue_prefix", "test"); err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	v, err := store.GetConfig(ctx, "issue_prefix")
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if v != "test" {
		t.Errorf("got %q, want %q", v, "test")
	}

	// Update is upsert
	if err := store.SetConfig(ctx, "issue_prefix", "updated"); err != nil {
		t.Fatalf("SetConfig update: %v", err)
	}
	v, _ = store.GetConfig(ctx, "issue_prefix")
	if v != "updated" {
		t.Errorf("after update, got %q, want %q", v, "updated")
	}

	// More keys + GetAllConfig
	_ = store.SetConfig(ctx, "auto_compact", "true")
	all, err := store.GetAllConfig(ctx)
	if err != nil {
		t.Fatalf("GetAllConfig: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("expected 2 keys, got %v", all)
	}
	if all["issue_prefix"] != "updated" {
		t.Errorf("issue_prefix=%q", all["issue_prefix"])
	}
}

func TestLocalMetadata_SetGet(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	_, err := store.GetLocalMetadata(ctx, "machine_id")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	if err := store.SetLocalMetadata(ctx, "machine_id", "angeloos2"); err != nil {
		t.Fatalf("SetLocalMetadata: %v", err)
	}
	v, err := store.GetLocalMetadata(ctx, "machine_id")
	if err != nil {
		t.Fatalf("GetLocalMetadata: %v", err)
	}
	if v != "angeloos2" {
		t.Errorf("got %q, want angeloos2", v)
	}

	// Update is upsert
	if err := store.SetLocalMetadata(ctx, "machine_id", "angeloos3"); err != nil {
		t.Fatalf("SetLocalMetadata update: %v", err)
	}
	v, _ = store.GetLocalMetadata(ctx, "machine_id")
	if v != "angeloos3" {
		t.Errorf("after update, got %q", v)
	}
}
