package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

func TestMergeSlotCreate_Idempotent(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	first, err := store.MergeSlotCreate(ctx, "tester")
	if err != nil {
		t.Fatalf("MergeSlotCreate (first): %v", err)
	}
	if first == nil || first.ID != mergeSlotID {
		t.Fatalf("expected slot id=%q, got %+v", mergeSlotID, first)
	}

	second, err := store.MergeSlotCreate(ctx, "tester")
	if err != nil {
		t.Fatalf("MergeSlotCreate (second): %v", err)
	}
	if second.ID != first.ID {
		t.Errorf("idempotent create must return same slot, got %q vs %q", second.ID, first.ID)
	}
}

func TestMergeSlotCheck_NotFound(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	_, err := store.MergeSlotCheck(ctx)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound when no slot exists, got %v", err)
	}
}

func TestMergeSlotCheck_AfterCreate(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	if _, err := store.MergeSlotCreate(ctx, "tester"); err != nil {
		t.Fatalf("MergeSlotCreate: %v", err)
	}
	st, err := store.MergeSlotCheck(ctx)
	if err != nil {
		t.Fatalf("MergeSlotCheck: %v", err)
	}
	if !st.Available {
		t.Errorf("fresh slot should be Available=true, got %+v", st)
	}
	if st.Holder != "" {
		t.Errorf("fresh slot Holder should be empty, got %q", st.Holder)
	}
	if len(st.Waiters) != 0 {
		t.Errorf("fresh slot Waiters should be empty, got %v", st.Waiters)
	}
}

func TestMergeSlotAcquireRelease_HappyPath(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	if _, err := store.MergeSlotCreate(ctx, "tester"); err != nil {
		t.Fatalf("MergeSlotCreate: %v", err)
	}

	res, err := store.MergeSlotAcquire(ctx, "alice", "tester", false)
	if err != nil {
		t.Fatalf("MergeSlotAcquire: %v", err)
	}
	if !res.Acquired {
		t.Fatalf("expected Acquired=true, got %+v", res)
	}
	if res.Holder != "alice" {
		t.Errorf("expected Holder=alice, got %q", res.Holder)
	}

	st, err := store.MergeSlotCheck(ctx)
	if err != nil {
		t.Fatalf("MergeSlotCheck post-acquire: %v", err)
	}
	if st.Available {
		t.Errorf("slot should not be Available after acquire, got %+v", st)
	}
	if st.Holder != "alice" {
		t.Errorf("expected Holder=alice, got %q", st.Holder)
	}

	if err := store.MergeSlotRelease(ctx, "alice", "tester"); err != nil {
		t.Fatalf("MergeSlotRelease: %v", err)
	}

	st, err = store.MergeSlotCheck(ctx)
	if err != nil {
		t.Fatalf("MergeSlotCheck post-release: %v", err)
	}
	if !st.Available {
		t.Errorf("slot should be Available after release, got %+v", st)
	}
	if st.Holder != "" {
		t.Errorf("Holder should be empty after release, got %q", st.Holder)
	}
}

func TestMergeSlotAcquire_AlreadyHeld_NoWait(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	if _, err := store.MergeSlotCreate(ctx, "tester"); err != nil {
		t.Fatalf("MergeSlotCreate: %v", err)
	}

	if _, err := store.MergeSlotAcquire(ctx, "alice", "tester", false); err != nil {
		t.Fatalf("first MergeSlotAcquire: %v", err)
	}

	// Bob arrives while Alice holds.
	res, err := store.MergeSlotAcquire(ctx, "bob", "tester", false)
	if err != nil {
		t.Fatalf("second MergeSlotAcquire: %v", err)
	}
	if res.Acquired {
		t.Errorf("expected Acquired=false when slot is held, got %+v", res)
	}
	if res.Waiting {
		t.Errorf("expected Waiting=false when wait=false, got %+v", res)
	}
	if res.Holder != "alice" {
		t.Errorf("expected Holder=alice (current owner), got %q", res.Holder)
	}
}

func TestMergeSlotAcquire_Wait_PromotesOnRelease(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	if _, err := store.MergeSlotCreate(ctx, "tester"); err != nil {
		t.Fatalf("MergeSlotCreate: %v", err)
	}

	// Alice holds.
	if _, err := store.MergeSlotAcquire(ctx, "alice", "tester", false); err != nil {
		t.Fatalf("alice acquire: %v", err)
	}

	// Bob waits in a goroutine; releaser frees the slot shortly after.
	type acqResult struct {
		res *storage.MergeSlotResult
		err error
	}
	done := make(chan acqResult, 1)
	go func() {
		res, err := store.MergeSlotAcquire(ctx, "bob", "tester", true)
		done <- acqResult{res, err}
	}()

	// Give Bob time to enqueue.
	st, err := waitForWaiter(ctx, store, "bob")
	if err != nil {
		t.Fatalf("wait for bob to enqueue: %v", err)
	}
	if st.Holder != "alice" {
		t.Fatalf("expected alice still holder, got %q", st.Holder)
	}

	// Release as Alice — Bob should be promoted via direct meta update.
	if err := store.MergeSlotRelease(ctx, "alice", "tester"); err != nil {
		t.Fatalf("alice release: %v", err)
	}

	out := <-done
	if out.err != nil {
		t.Fatalf("bob acquire returned error: %v", out.err)
	}
	if !out.res.Acquired {
		t.Errorf("bob should have acquired after alice released, got %+v", out.res)
	}
	if out.res.Holder != "bob" {
		t.Errorf("expected bob to be holder, got %q", out.res.Holder)
	}
}

func TestMergeSlotRelease_NonHolder(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	if _, err := store.MergeSlotCreate(ctx, "tester"); err != nil {
		t.Fatalf("MergeSlotCreate: %v", err)
	}
	if _, err := store.MergeSlotAcquire(ctx, "alice", "tester", false); err != nil {
		t.Fatalf("alice acquire: %v", err)
	}

	err := store.MergeSlotRelease(ctx, "bob", "tester")
	if err == nil {
		t.Fatal("expected error releasing as non-holder, got nil")
	}
	if !strings.Contains(err.Error(), "alice") || !strings.Contains(err.Error(), "bob") {
		t.Errorf("expected error to name both alice and bob, got %v", err)
	}
}

// waitForWaiter polls MergeSlotCheck until name appears in Waiters or ctx
// expires. Returns the most recent status.
func waitForWaiter(ctx context.Context, store *PostgresStore, name string) (*storage.MergeSlotStatus, error) {
	var last *storage.MergeSlotStatus
	for i := 0; i < 200; i++ { // up to ~10s at 50ms intervals
		st, err := store.MergeSlotCheck(ctx)
		if err != nil {
			return nil, err
		}
		last = st
		for _, w := range st.Waiters {
			if w == name {
				return st, nil
			}
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	return last, errors.New("timed out waiting for waiter to enqueue")
}
