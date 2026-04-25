package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// ════════════════════════════════════════════════════════════════════════
// GetEvents / GetAllEventsSince
// ════════════════════════════════════════════════════════════════════════

func TestEvents_GetEventsAndSince(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "ev-1", "issue with events")
	createIssue(t, ctx, store, "ev-2", "other issue with events")

	// Generate events on both issues by exercising other store methods.
	// CloseIssue + ReopenIssue + UpdateIssueType all write event rows.
	if err := store.CloseIssue(ctx, "ev-1", "done", "tester", "session-a"); err != nil {
		t.Fatalf("CloseIssue ev-1: %v", err)
	}
	if err := store.ReopenIssue(ctx, "ev-1", "needs more work", "tester"); err != nil {
		t.Fatalf("ReopenIssue ev-1: %v", err)
	}
	if err := store.UpdateIssueType(ctx, "ev-1", "bug", "tester"); err != nil {
		t.Fatalf("UpdateIssueType ev-1: %v", err)
	}

	if err := store.UpdateIssueType(ctx, "ev-2", "feature", "tester"); err != nil {
		t.Fatalf("UpdateIssueType ev-2: %v", err)
	}

	// ── GetEvents (no limit): we wrote two events for ev-1 ──
	events, err := store.GetEvents(ctx, "ev-1", 0)
	if err != nil {
		t.Fatalf("GetEvents ev-1: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected >=2 events on ev-1, got %d", len(events))
	}
	// Every event should belong to ev-1.
	for _, e := range events {
		if e.IssueID != "ev-1" {
			t.Errorf("unexpected event for issue %q in ev-1 results", e.IssueID)
		}
	}

	// ── Ordering: newest first (created_at DESC, id DESC) ──
	for i := 1; i < len(events); i++ {
		prev := events[i-1].CreatedAt
		cur := events[i].CreatedAt
		if cur.After(prev) {
			t.Errorf("events not ordered newest-first: index %d created_at %v > %v",
				i, cur, prev)
		}
	}

	// type_changed event (most recent action) should be the first row.
	if events[0].EventType != types.EventType("type_changed") {
		t.Errorf("expected first event = type_changed, got %q", events[0].EventType)
	}
	if events[0].OldValue == nil || *events[0].OldValue != "task" {
		t.Errorf("expected old_value=task, got %v", events[0].OldValue)
	}
	if events[0].NewValue == nil || *events[0].NewValue != "bug" {
		t.Errorf("expected new_value=bug, got %v", events[0].NewValue)
	}

	// ── GetEvents with limit=1 ──
	limited, err := store.GetEvents(ctx, "ev-1", 1)
	if err != nil {
		t.Fatalf("GetEvents limit=1: %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("expected 1 event with limit=1, got %d", len(limited))
	}
	if limited[0].EventType != events[0].EventType {
		t.Errorf("limit=1 should return the newest event")
	}

	// ── GetAllEventsSince: time-based filter, ASC order, includes both issues ──
	long_ago := time.Now().Add(-1 * time.Hour)
	all, err := store.GetAllEventsSince(ctx, long_ago)
	if err != nil {
		t.Fatalf("GetAllEventsSince: %v", err)
	}
	sawEv1, sawEv2 := false, false
	for _, e := range all {
		if e.IssueID == "ev-1" {
			sawEv1 = true
		}
		if e.IssueID == "ev-2" {
			sawEv2 = true
		}
	}
	if !sawEv1 || !sawEv2 {
		t.Errorf("GetAllEventsSince should return events from both issues; ev1=%v ev2=%v",
			sawEv1, sawEv2)
	}
	// Ordered ascending.
	for i := 1; i < len(all); i++ {
		prev := all[i-1].CreatedAt
		cur := all[i].CreatedAt
		if cur.Before(prev) {
			t.Errorf("GetAllEventsSince not ordered oldest-first at index %d", i)
		}
	}

	// Future cutoff returns nothing.
	future := time.Now().Add(1 * time.Hour)
	none, err := store.GetAllEventsSince(ctx, future)
	if err != nil {
		t.Fatalf("GetAllEventsSince future: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("expected 0 events for future cutoff, got %d", len(none))
	}
}

// ════════════════════════════════════════════════════════════════════════
// ReopenIssue
// ════════════════════════════════════════════════════════════════════════

func TestReopenIssue_StatusFlipsAndEventWritten(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "reopen-1", "to be reopened")

	if err := store.CloseIssue(ctx, "reopen-1", "done", "tester", "session-a"); err != nil {
		t.Fatalf("CloseIssue: %v", err)
	}

	// Sanity check: it's actually closed now.
	closed, err := store.GetIssue(ctx, "reopen-1")
	if err != nil {
		t.Fatalf("GetIssue closed: %v", err)
	}
	if closed.Status != types.Status("closed") {
		t.Fatalf("expected status=closed before reopen, got %q", closed.Status)
	}

	if err := store.ReopenIssue(ctx, "reopen-1", "regression", "tester"); err != nil {
		t.Fatalf("ReopenIssue: %v", err)
	}

	out, err := store.GetIssue(ctx, "reopen-1")
	if err != nil {
		t.Fatalf("GetIssue after reopen: %v", err)
	}
	if out.Status != types.Status("open") {
		t.Errorf("expected status=open, got %q", out.Status)
	}
	if out.ClosedAt != nil {
		t.Errorf("expected ClosedAt to be cleared, got %v", out.ClosedAt)
	}
	if out.CloseReason != "" {
		t.Errorf("expected close_reason cleared, got %q", out.CloseReason)
	}

	// Event with the right shape was written.
	events, err := store.GetEvents(ctx, "reopen-1", 0)
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	var reopened *types.Event
	for _, e := range events {
		if e.EventType == types.EventType("reopened") {
			reopened = e
			break
		}
	}
	if reopened == nil {
		t.Fatalf("expected a 'reopened' event, got %+v", events)
	}
	if reopened.Actor != "tester" {
		t.Errorf("expected actor=tester, got %q", reopened.Actor)
	}
	if reopened.Comment == nil || *reopened.Comment != "regression" {
		t.Errorf("expected comment=regression, got %v", reopened.Comment)
	}
}

func TestReopenIssue_NotClosedReturnsNotFound(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// Issue exists but is open — ReopenIssue must return ErrNotFound.
	createIssue(t, ctx, store, "reopen-2", "still open")
	err := store.ReopenIssue(ctx, "reopen-2", "x", "tester")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound when issue is not closed, got %v", err)
	}

	// Truly missing issue.
	err = store.ReopenIssue(ctx, "does-not-exist", "x", "tester")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing issue, got %v", err)
	}
}

// ════════════════════════════════════════════════════════════════════════
// UpdateIssueType
// ════════════════════════════════════════════════════════════════════════

func TestUpdateIssueType_ChangesTypeAndWritesEvent(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "type-1", "to be retyped")

	if err := store.UpdateIssueType(ctx, "type-1", "feature", "tester"); err != nil {
		t.Fatalf("UpdateIssueType: %v", err)
	}

	out, err := store.GetIssue(ctx, "type-1")
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if out.IssueType != types.IssueType("feature") {
		t.Errorf("expected issue_type=feature, got %q", out.IssueType)
	}

	events, err := store.GetEvents(ctx, "type-1", 0)
	if err != nil {
		t.Fatalf("GetEvents: %v", err)
	}
	var typeChanged *types.Event
	for _, e := range events {
		if e.EventType == types.EventType("type_changed") {
			typeChanged = e
			break
		}
	}
	if typeChanged == nil {
		t.Fatalf("expected a 'type_changed' event, got %+v", events)
	}
	if typeChanged.OldValue == nil || *typeChanged.OldValue != "task" {
		t.Errorf("expected old_value=task, got %v", typeChanged.OldValue)
	}
	if typeChanged.NewValue == nil || *typeChanged.NewValue != "feature" {
		t.Errorf("expected new_value=feature, got %v", typeChanged.NewValue)
	}
	if typeChanged.Actor != "tester" {
		t.Errorf("expected actor=tester, got %q", typeChanged.Actor)
	}
}

func TestUpdateIssueType_MissingIssueReturnsNotFound(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	err := store.UpdateIssueType(ctx, "no-such-issue", "bug", "tester")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing issue, got %v", err)
	}
}
