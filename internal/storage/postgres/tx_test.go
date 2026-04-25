package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// TestRunInTransaction_CommitMultipleWrites verifies the happy path: several
// distinct writes inside one transaction all become visible after Commit.
func TestRunInTransaction_CommitMultipleWrites(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	err := store.RunInTransaction(ctx, "test commit", func(tx storage.Transaction) error {
		if err := tx.CreateIssue(ctx, &types.Issue{
			ID: "tx-1", Title: "first", Status: types.Status("open"),
			Priority: 2, IssueType: types.IssueType("task"),
		}, "tester"); err != nil {
			return err
		}
		if err := tx.CreateIssue(ctx, &types.Issue{
			ID: "tx-2", Title: "second", Status: types.Status("open"),
			Priority: 2, IssueType: types.IssueType("task"),
		}, "tester"); err != nil {
			return err
		}
		if err := tx.AddDependency(ctx, &types.Dependency{
			IssueID: "tx-2", DependsOnID: "tx-1", Type: "blocks",
		}, "tester"); err != nil {
			return err
		}
		if err := tx.AddLabel(ctx, "tx-1", "alpha", "tester"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunInTransaction commit: %v", err)
	}

	// All four writes should be visible on the store after commit.
	if _, err := store.GetIssue(ctx, "tx-1"); err != nil {
		t.Errorf("tx-1 not visible after commit: %v", err)
	}
	if _, err := store.GetIssue(ctx, "tx-2"); err != nil {
		t.Errorf("tx-2 not visible after commit: %v", err)
	}
	deps, err := store.GetDependencies(ctx, "tx-2")
	if err != nil {
		t.Fatalf("GetDependencies: %v", err)
	}
	if len(deps) != 1 || deps[0].ID != "tx-1" {
		t.Errorf("expected tx-2 to depend on tx-1, got %v", deps)
	}
	labels, err := store.GetLabels(ctx, "tx-1")
	if err != nil {
		t.Fatalf("GetLabels: %v", err)
	}
	if len(labels) != 1 || labels[0] != "alpha" {
		t.Errorf("expected tx-1 to have label [alpha], got %v", labels)
	}
}

// TestRunInTransaction_RollbackHidesAllWrites verifies that when the callback
// returns an error, no writes from the failed transaction are visible.
func TestRunInTransaction_RollbackHidesAllWrites(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// Pre-create one issue so we can confirm independent baseline state.
	createIssue(t, ctx, store, "baseline", "baseline issue")

	sentinel := errors.New("intentional rollback")
	err := store.RunInTransaction(ctx, "rollback test", func(tx storage.Transaction) error {
		if err := tx.CreateIssue(ctx, &types.Issue{
			ID: "rb-1", Title: "should disappear",
			Status: types.Status("open"), Priority: 2,
			IssueType: types.IssueType("task"),
		}, "tester"); err != nil {
			return err
		}
		if err := tx.AddLabel(ctx, "baseline", "tx-only", "tester"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}

	// rb-1 must not exist.
	if _, err := store.GetIssue(ctx, "rb-1"); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("rb-1 should not be visible after rollback, got err=%v", err)
	}
	// 'tx-only' label must not be on baseline.
	labels, err := store.GetLabels(ctx, "baseline")
	if err != nil {
		t.Fatalf("GetLabels baseline: %v", err)
	}
	for _, l := range labels {
		if l == "tx-only" {
			t.Errorf("rolled-back label still present: %v", labels)
		}
	}
	// baseline itself still exists (was committed before the tx).
	if _, err := store.GetIssue(ctx, "baseline"); err != nil {
		t.Errorf("baseline must still exist: %v", err)
	}
}

// TestTx_ReadYourWrites verifies that GetIssue inside a transaction sees
// an issue created earlier in the same transaction (read-your-writes).
func TestTx_ReadYourWrites(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	err := store.RunInTransaction(ctx, "ryw", func(tx storage.Transaction) error {
		if err := tx.CreateIssue(ctx, &types.Issue{
			ID: "ryw-1", Title: "read-your-writes",
			Status: types.Status("open"), Priority: 2,
			IssueType: types.IssueType("task"),
		}, "tester"); err != nil {
			return err
		}
		got, err := tx.GetIssue(ctx, "ryw-1")
		if err != nil {
			return err
		}
		if got.Title != "read-your-writes" {
			t.Errorf("read-your-writes title mismatch: %q", got.Title)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunInTransaction: %v", err)
	}
}

// TestTx_GetDependencyRecords verifies the raw-dependencies accessor returns
// the expected edges for an issue.
func TestTx_GetDependencyRecords(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	err := store.RunInTransaction(ctx, "deprecs", func(tx storage.Transaction) error {
		for _, id := range []string{"dep-A", "dep-B", "dep-C"} {
			if err := tx.CreateIssue(ctx, &types.Issue{
				ID: id, Title: id, Status: types.Status("open"),
				Priority: 2, IssueType: types.IssueType("task"),
			}, "tester"); err != nil {
				return err
			}
		}
		// dep-A depends on dep-B (blocks) and dep-C (related).
		if err := tx.AddDependency(ctx, &types.Dependency{
			IssueID: "dep-A", DependsOnID: "dep-B", Type: "blocks",
		}, "tester"); err != nil {
			return err
		}
		if err := tx.AddDependency(ctx, &types.Dependency{
			IssueID: "dep-A", DependsOnID: "dep-C", Type: "related",
		}, "tester"); err != nil {
			return err
		}

		recs, err := tx.GetDependencyRecords(ctx, "dep-A")
		if err != nil {
			return err
		}
		if len(recs) != 2 {
			t.Errorf("expected 2 dependency records for dep-A, got %d", len(recs))
		}
		seen := map[string]types.DependencyType{}
		for _, r := range recs {
			seen[r.DependsOnID] = r.Type
		}
		if seen["dep-B"] != "blocks" {
			t.Errorf("dep-B edge type: got %q want blocks", seen["dep-B"])
		}
		if seen["dep-C"] != "related" {
			t.Errorf("dep-C edge type: got %q want related", seen["dep-C"])
		}

		// And after RemoveDependency, it should drop out.
		if err := tx.RemoveDependency(ctx, "dep-A", "dep-C", "tester"); err != nil {
			return err
		}
		recs2, err := tx.GetDependencyRecords(ctx, "dep-A")
		if err != nil {
			return err
		}
		if len(recs2) != 1 || recs2[0].DependsOnID != "dep-B" {
			t.Errorf("after remove, expected only dep-B; got %d records", len(recs2))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunInTransaction: %v", err)
	}
}

// TestTx_MetadataRoundTrip exercises SetMetadata / GetMetadata both inside
// the transaction (read-your-writes) and after commit via a fresh
// transaction. ErrNotFound is returned for unknown keys.
func TestTx_MetadataRoundTrip(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	// 1) Inside-tx read-your-writes.
	err := store.RunInTransaction(ctx, "metadata-write", func(tx storage.Transaction) error {
		if err := tx.SetMetadata(ctx, "import.cursor", "cursor-1"); err != nil {
			return err
		}
		v, err := tx.GetMetadata(ctx, "import.cursor")
		if err != nil {
			return err
		}
		if v != "cursor-1" {
			t.Errorf("read-your-writes: got %q want cursor-1", v)
		}
		// Unknown key must yield ErrNotFound.
		if _, err := tx.GetMetadata(ctx, "no-such-key"); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("expected ErrNotFound for missing metadata, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunInTransaction set: %v", err)
	}

	// 2) Visible from a fresh transaction after commit.
	err = store.RunInTransaction(ctx, "metadata-read", func(tx storage.Transaction) error {
		v, err := tx.GetMetadata(ctx, "import.cursor")
		if err != nil {
			return err
		}
		if v != "cursor-1" {
			t.Errorf("post-commit read: got %q want cursor-1", v)
		}
		// Update should behave as upsert.
		if err := tx.SetMetadata(ctx, "import.cursor", "cursor-2"); err != nil {
			return err
		}
		v2, err := tx.GetMetadata(ctx, "import.cursor")
		if err != nil {
			return err
		}
		if v2 != "cursor-2" {
			t.Errorf("upsert: got %q want cursor-2", v2)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RunInTransaction read: %v", err)
	}
}
