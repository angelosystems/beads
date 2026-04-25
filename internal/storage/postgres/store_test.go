package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// testEnv is the env var for the Postgres connection string used by tests.
// Set to a connection string pointing at a Postgres instance where the
// test process is allowed to CREATE/DROP databases.
//
// Example:
//   export BEADS_TEST_POSTGRES="postgres://remote:remote@127.0.0.1:5433/postgres?sslmode=disable"
//   go test ./internal/storage/postgres/...
//
// Tests are skipped when this env var is not set, so the test suite stays
// CI-friendly without a hard Postgres dependency.
const testEnv = "BEADS_TEST_POSTGRES"

// connectTest returns a freshly-migrated PostgresStore against an ephemeral
// test database. The database is dropped via t.Cleanup. Skips the test if
// BEADS_TEST_POSTGRES is unset.
func connectTest(t *testing.T) (*PostgresStore, string) {
	t.Helper()
	connStr := os.Getenv(testEnv)
	if connStr == "" {
		t.Skipf("set %s to enable Postgres integration tests", testEnv)
	}

	ctx := context.Background()

	// Use the bootstrap connection to CREATE a unique test DB.
	bootstrap, err := Open(ctx, Config{ConnString: connStr, Rig: "test", AutoMigrate: false})
	if err != nil {
		t.Fatalf("connect bootstrap: %v", err)
	}
	defer bootstrap.Close()

	dbName := fmt.Sprintf("beads_test_%d", time.Now().UnixNano())
	if _, err := bootstrap.db.ExecContext(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create test db: %v", err)
	}

	// Build a connection string pointing at the new DB.
	testConn := rebindDatabase(connStr, dbName)
	store, err := Open(ctx, Config{
		ConnString:  testConn,
		Rig:         "test",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("open test store: %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close()
		// Reconnect to bootstrap and drop the test DB.
		bs, err := Open(context.Background(), Config{ConnString: connStr, Rig: "test"})
		if err != nil {
			return
		}
		defer bs.Close()
		_, _ = bs.db.ExecContext(context.Background(), "DROP DATABASE "+dbName+" WITH (FORCE)")
	})

	return store, dbName
}

// rebindDatabase rewrites the database segment of a libpq URI.
// Quick-and-dirty for tests; assumes URL format.
func rebindDatabase(connStr, dbName string) string {
	// connStr is like: postgres://user:pass@host:port/oldname?args
	// We swap "/oldname" for "/dbName".
	idx := -1
	for i := len(connStr) - 1; i >= 0; i-- {
		if connStr[i] == '/' {
			idx = i
			break
		}
	}
	if idx < 0 {
		return connStr
	}
	rest := ""
	if q := indexAfter(connStr, '?', idx); q >= 0 {
		rest = connStr[q:]
	}
	return connStr[:idx+1] + dbName + rest
}

func indexAfter(s string, c byte, from int) int {
	for i := from; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// ════════════════════════════════════════════════════════════════════════
// Tests
// ════════════════════════════════════════════════════════════════════════

func TestMigrate_FreshDB(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	var version int
	err := store.db.QueryRowContext(ctx, `SELECT MAX(version) FROM beads.schema_migrations`).Scan(&version)
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	if version < 2 {
		t.Errorf("expected migrations through version 2, got %d", version)
	}

	// Sanity: 27 tables in the beads schema.
	var n int
	err = store.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'beads' AND table_type = 'BASE TABLE'
	`).Scan(&n)
	if err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if n != 27 {
		t.Errorf("expected 27 tables in beads schema, got %d", n)
	}
}

func TestCreateAndGetIssue(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	in := &types.Issue{
		ID:          "test-001",
		Title:       "First test issue",
		Description: "smoke test",
		Status:      types.Status("open"),
		Priority:    1,
		IssueType:   types.IssueType("task"),
		CreatedBy:   "tester",
	}

	if err := store.CreateIssue(ctx, in, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	out, err := store.GetIssue(ctx, "test-001")
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
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

func TestGetIssue_NotFound(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	_, err := store.GetIssue(ctx, "does-not-exist")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestUpdateIssue(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	in := &types.Issue{
		ID:        "test-002",
		Title:     "Original",
		Status:    types.Status("open"),
		Priority:  2,
		IssueType: types.IssueType("task"),
	}
	if err := store.CreateIssue(ctx, in, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	updates := map[string]interface{}{
		"title":    "Updated title",
		"priority": 0,
	}
	if err := store.UpdateIssue(ctx, "test-002", updates, "tester"); err != nil {
		t.Fatalf("UpdateIssue: %v", err)
	}

	out, err := store.GetIssue(ctx, "test-002")
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if out.Title != "Updated title" {
		t.Errorf("Title not updated: %q", out.Title)
	}
	if out.Priority != 0 {
		t.Errorf("Priority not updated: %d", out.Priority)
	}
}

func TestCloseIssue(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	in := &types.Issue{
		ID:        "test-003",
		Title:     "Will be closed",
		Status:    types.Status("open"),
		Priority:  2,
		IssueType: types.IssueType("task"),
	}
	if err := store.CreateIssue(ctx, in, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	if err := store.CloseIssue(ctx, "test-003", "completed", "tester", "session-xyz"); err != nil {
		t.Fatalf("CloseIssue: %v", err)
	}

	out, err := store.GetIssue(ctx, "test-003")
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if out.Status != types.Status("closed") {
		t.Errorf("expected status=closed, got %q", out.Status)
	}
	if out.CloseReason != "completed" {
		t.Errorf("expected close_reason=completed, got %q", out.CloseReason)
	}
	if out.ClosedBySession != "session-xyz" {
		t.Errorf("expected closed_by_session=session-xyz, got %q", out.ClosedBySession)
	}
	if out.ClosedAt == nil {
		t.Error("ClosedAt should be populated")
	}
}

func TestDeleteIssue_SoftDelete(t *testing.T) {
	store, _ := connectTest(t)
	ctx := context.Background()

	in := &types.Issue{
		ID:        "test-004",
		Title:     "Will be deleted",
		Status:    types.Status("open"),
		Priority:  2,
		IssueType: types.IssueType("task"),
	}
	if err := store.CreateIssue(ctx, in, "tester"); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	if err := store.DeleteIssue(ctx, "test-004"); err != nil {
		t.Fatalf("DeleteIssue: %v", err)
	}

	// GetIssue must now return ErrNotFound.
	_, err := store.GetIssue(ctx, "test-004")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("after soft-delete, GetIssue should return ErrNotFound, got %v", err)
	}

	// But the row is still present (soft-delete) — verify directly.
	var deletedAt *time.Time
	err = store.db.QueryRowContext(ctx,
		`SELECT deleted_at FROM beads.issues WHERE id = $1`, "test-004",
	).Scan(&deletedAt)
	if err != nil {
		t.Fatalf("direct read after soft-delete: %v", err)
	}
	if deletedAt == nil {
		t.Error("deleted_at should be set after soft-delete")
	}
}
