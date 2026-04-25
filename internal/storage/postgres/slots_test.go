package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
)

func TestSlotSetGetClear_RoundTrip(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "slot-1", "slot test")

	if err := store.SlotSet(ctx, "slot-1", "lease.holder", "agent-7", "tester"); err != nil {
		t.Fatalf("SlotSet: %v", err)
	}

	got, err := store.SlotGet(ctx, "slot-1", "lease.holder")
	if err != nil {
		t.Fatalf("SlotGet: %v", err)
	}
	if got != "agent-7" {
		t.Errorf("SlotGet: got %q, want %q", got, "agent-7")
	}

	// Overwrite same key.
	if err := store.SlotSet(ctx, "slot-1", "lease.holder", "agent-9", "tester"); err != nil {
		t.Fatalf("SlotSet (overwrite): %v", err)
	}
	got, err = store.SlotGet(ctx, "slot-1", "lease.holder")
	if err != nil {
		t.Fatalf("SlotGet after overwrite: %v", err)
	}
	if got != "agent-9" {
		t.Errorf("SlotGet after overwrite: got %q, want %q", got, "agent-9")
	}

	// Empty-string value is preserved (distinguished from "absent").
	if err := store.SlotSet(ctx, "slot-1", "empty", "", "tester"); err != nil {
		t.Fatalf("SlotSet empty: %v", err)
	}
	got, err = store.SlotGet(ctx, "slot-1", "empty")
	if err != nil {
		t.Fatalf("SlotGet empty: %v", err)
	}
	if got != "" {
		t.Errorf("SlotGet empty: expected empty string, got %q", got)
	}

	// Clear the key.
	if err := store.SlotClear(ctx, "slot-1", "lease.holder", "tester"); err != nil {
		t.Fatalf("SlotClear: %v", err)
	}

	_, err = store.SlotGet(ctx, "slot-1", "lease.holder")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("SlotGet after Clear: expected ErrNotFound, got %v", err)
	}

	// The other key still resolves.
	got, err = store.SlotGet(ctx, "slot-1", "empty")
	if err != nil {
		t.Fatalf("SlotGet other key after clear: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestSlotGet_MissingKey(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	createIssue(t, ctx, store, "slot-2", "missing key test")

	_, err := store.SlotGet(ctx, "slot-2", "never-set")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing key, got %v", err)
	}
}

func TestSlotGet_MissingIssue(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	_, err := store.SlotGet(ctx, "no-such-issue", "any")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing issue, got %v", err)
	}
}

func TestSlotSet_MissingIssue(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	err := store.SlotSet(ctx, "no-such-issue", "k", "v", "tester")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing issue, got %v", err)
	}
}

func TestSlotClear_MissingIssue(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	err := store.SlotClear(ctx, "no-such-issue", "k", "tester")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing issue, got %v", err)
	}
}
