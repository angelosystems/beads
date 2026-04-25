package main

// Tests for bd-pgmigrate.
//
// Test approach (documented per spec): use the live Dolt server at
// 127.0.0.1:3307 and run the migration tool against the "hq" database with
// LIMIT 5 — this exercises the real MySQL-protocol path, real schema
// (TINYINT, JSON, DATETIME), and real Beads data without copying the full
// 19k+ rows. The tradeoff is that the test depends on the Dolt server being
// up; we skip if the connection fails.
//
// Postgres: we use the same BEADS_TEST_POSTGRES env var as the rest of the
// suite; the test creates an ephemeral DB, applies the embedded Beads
// migrations, runs the migration tool with --limit=5 against just the
// `issues` and `labels` tables (FK constraints prevent a full --limit=5 run
// across all tables — see comment in TestMigrate_LimitedSubset), and
// asserts the rows landed with rig=hq.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/steveyegge/beads/internal/storage/postgres"
)

const (
	testPostgresEnv = "BEADS_TEST_POSTGRES"
	testDoltDSN     = "root@tcp(127.0.0.1:3307)/"
	testSourceDB    = "hq"
)

// TestBuildMySQLDSN exercises the DSN builder in isolation — pure logic,
// no network needed.
func TestBuildMySQLDSN(t *testing.T) {
	cases := []struct {
		base, db, want string
	}{
		{"root@tcp(127.0.0.1:3307)/", "hq", "root@tcp(127.0.0.1:3307)/hq?parseTime=true"},
		{"root@tcp(127.0.0.1:3307)/?charset=utf8", "hq", "root@tcp(127.0.0.1:3307)/hq?charset=utf8&parseTime=true"},
		{"root@tcp(127.0.0.1:3307)/?parseTime=true", "hq", "root@tcp(127.0.0.1:3307)/hq?parseTime=true"},
		{"root:pass@tcp(host:3307)/oldname", "hq", "root:pass@tcp(host:3307)/hq?parseTime=true"},
	}
	for _, c := range cases {
		got := buildMySQLDSN(c.base, c.db)
		if got != c.want {
			t.Errorf("buildMySQLDSN(%q, %q) = %q; want %q", c.base, c.db, got, c.want)
		}
	}
}

// TestCoerce checks the type-coercion helper for booleans and JSON columns.
func TestCoerce(t *testing.T) {
	boolCol := pgColumn{DataType: "boolean"}
	jsonCol := pgColumn{DataType: "jsonb"}
	textCol := pgColumn{DataType: "text"}

	if got := coerce(int64(1), boolCol); got != true {
		t.Errorf("coerce(1, bool) = %v; want true", got)
	}
	if got := coerce(int64(0), boolCol); got != false {
		t.Errorf("coerce(0, bool) = %v; want false", got)
	}
	if got := coerce([]byte("true"), boolCol); got != true {
		t.Errorf("coerce(\"true\", bool) = %v; want true", got)
	}
	if got := coerce(nil, boolCol); got != nil {
		t.Errorf("coerce(nil, bool) = %v; want nil", got)
	}
	if got := coerce([]byte(`{"k":1}`), jsonCol); got != `{"k":1}` {
		t.Errorf("coerce(jsonbytes, jsonb) = %v; want %q", got, `{"k":1}`)
	}
	if got := coerce([]byte{}, jsonCol); got != "{}" {
		t.Errorf("coerce(emptybytes, jsonb) = %v; want %q", got, "{}")
	}
	if got := coerce("hello", textCol); got != "hello" {
		t.Errorf("coerce passthrough text changed value: %v", got)
	}
}

// TestIsFKError verifies the FK-error detector.
func TestIsFKError(t *testing.T) {
	if isFKError(nil) {
		t.Fatal("nil should not be FK error")
	}
	if !isFKError(fmt.Errorf("ERROR: insert violates foreign key constraint (SQLSTATE 23503)")) {
		t.Fatal("23503 string should be FK error")
	}
	if isFKError(fmt.Errorf("syntax error")) {
		t.Fatal("plain error should not be FK error")
	}
}

// TestMigrate_LimitedSubset is the end-to-end test. It:
//   1. Skips unless BEADS_TEST_POSTGRES + the local Dolt server are reachable.
//   2. Creates an ephemeral test Postgres DB with the Beads schema applied.
//   3. Runs the migrate tool with --limit=5 against the live Dolt "hq" DB.
//      Because FK constraints would fail when only 5 issues come along but
//      hundreds of dependent rows reference them, the tool's per-row
//      savepoint/skip path is what makes this work — and is itself part
//      of what's being tested.
//   4. Asserts the issues landed in Postgres with rig="hq".
func TestMigrate_LimitedSubset(t *testing.T) {
	connStr := os.Getenv(testPostgresEnv)
	if connStr == "" {
		t.Skipf("set %s to enable the integration test", testPostgresEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Probe Dolt — skip if it's not reachable.
	doltDB, err := sql.Open("mysql", buildMySQLDSN(testDoltDSN, testSourceDB))
	if err != nil {
		t.Skipf("dolt open: %v", err)
	}
	if err := doltDB.PingContext(ctx); err != nil {
		_ = doltDB.Close()
		t.Skipf("dolt ping (server not running on 127.0.0.1:3307?): %v", err)
	}
	_ = doltDB.Close()

	// Create an ephemeral test DB.
	testDB, cleanup := createTestDB(t, connStr)
	defer cleanup()

	// Apply the Beads schema by opening with AutoMigrate=true.
	store, err := postgres.Open(ctx, postgres.Config{
		ConnString:  testDB,
		Rig:         "hq",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	pg := store.DB()
	defer store.Close()

	// Run the tool.
	if err := run(ctx, testDoltDSN, testDB, testSourceDB, "hq", false, false, 5); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Verify issues landed.
	var issueCount int
	if err := pg.QueryRowContext(ctx, "SELECT COUNT(*) FROM beads.issues WHERE rig = 'hq'").Scan(&issueCount); err != nil {
		t.Fatalf("count issues: %v", err)
	}
	if issueCount != 5 {
		t.Errorf("expected 5 issues, got %d", issueCount)
	}

	// Verify rig is populated everywhere.
	var nullRig int
	if err := pg.QueryRowContext(ctx, "SELECT COUNT(*) FROM beads.issues WHERE rig IS NULL OR rig = ''").Scan(&nullRig); err != nil {
		t.Fatalf("count null-rig: %v", err)
	}
	if nullRig != 0 {
		t.Errorf("expected 0 issues with empty rig, got %d", nullRig)
	}

	// Verify config rows landed (config has 13 rows in hq, all with the
	// same rig — and no FKs to issues, so they all migrate cleanly).
	var configCount int
	if err := pg.QueryRowContext(ctx, "SELECT COUNT(*) FROM beads.config WHERE rig = 'hq'").Scan(&configCount); err != nil {
		t.Fatalf("count config: %v", err)
	}
	if configCount != 5 { // limited to 5
		t.Errorf("expected 5 config rows (limit=5), got %d", configCount)
	}

	// Verify boolean coercion: issues have ephemeral/no_history/pinned/is_template
	// as TINYINT(1) in Dolt and BOOLEAN in Postgres. The fact that the INSERT
	// succeeded above already proves the coercion works, but assert column
	// types just in case.
	var dt string
	if err := pg.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema='beads' AND table_name='issues' AND column_name='ephemeral'
	`).Scan(&dt); err != nil {
		t.Fatalf("query column type: %v", err)
	}
	if dt != "boolean" {
		t.Errorf("issues.ephemeral type = %s; want boolean", dt)
	}
}

// TestMigrate_DryRun verifies dry-run mode counts rows but writes nothing.
func TestMigrate_DryRun(t *testing.T) {
	connStr := os.Getenv(testPostgresEnv)
	if connStr == "" {
		t.Skipf("set %s to enable the integration test", testPostgresEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	doltDB, err := sql.Open("mysql", buildMySQLDSN(testDoltDSN, testSourceDB))
	if err != nil {
		t.Skipf("dolt open: %v", err)
	}
	if err := doltDB.PingContext(ctx); err != nil {
		_ = doltDB.Close()
		t.Skipf("dolt ping: %v", err)
	}
	_ = doltDB.Close()

	testDB, cleanup := createTestDB(t, connStr)
	defer cleanup()

	store, err := postgres.Open(ctx, postgres.Config{
		ConnString:  testDB,
		Rig:         "hq",
		AutoMigrate: true,
	})
	if err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	pg := store.DB()
	defer store.Close()

	if err := run(ctx, testDoltDSN, testDB, testSourceDB, "hq", true /*dryRun*/, false, 5); err != nil {
		t.Fatalf("dry-run: %v", err)
	}

	// Nothing should have landed.
	var n int
	if err := pg.QueryRowContext(ctx, "SELECT COUNT(*) FROM beads.issues").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("dry-run wrote %d issues; expected 0", n)
	}
}

// createTestDB creates a unique ephemeral test database and returns
// (connection string for it, cleanup fn).
func createTestDB(t *testing.T, baseConn string) (string, func()) {
	t.Helper()
	ctx := context.Background()
	bs, err := sql.Open("pgx", baseConn)
	if err != nil {
		t.Fatalf("bootstrap open: %v", err)
	}
	if err := bs.PingContext(ctx); err != nil {
		_ = bs.Close()
		t.Skipf("bootstrap ping: %v", err)
	}
	dbName := fmt.Sprintf("bd_pgmigrate_test_%d", time.Now().UnixNano())
	if _, err := bs.ExecContext(ctx, "CREATE DATABASE "+dbName); err != nil {
		_ = bs.Close()
		t.Fatalf("create test db: %v", err)
	}
	_ = bs.Close()

	testConn := rebindDatabase(baseConn, dbName)
	cleanup := func() {
		bs, err := sql.Open("pgx", baseConn)
		if err != nil {
			return
		}
		defer bs.Close()
		_, _ = bs.ExecContext(context.Background(), "DROP DATABASE "+dbName+" WITH (FORCE)")
	}
	return testConn, cleanup
}

// rebindDatabase rewrites the database segment of a libpq URI.
// (Mirrors the same helper used in internal/storage/postgres test setup.)
func rebindDatabase(connStr, dbName string) string {
	idx := strings.LastIndex(connStr, "/")
	if idx < 0 {
		return connStr
	}
	rest := ""
	tail := connStr[idx+1:]
	if q := strings.Index(tail, "?"); q >= 0 {
		rest = tail[q:]
	}
	return connStr[:idx+1] + dbName + rest
}
