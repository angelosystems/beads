// Command bd-pgmigrate is a one-shot data migration tool that copies a Beads
// rig from a Dolt SQL server (MySQL wire protocol) into a Postgres database
// that has the canonical Beads schema (migrations 0001+0002) already applied.
//
// Source: a Dolt SQL server reachable via TCP (default 127.0.0.1:3307).
//         Each Dolt database is one rig (hq, activepieces, angeloos, guild, ...).
// Target: a Postgres database with the "beads" schema present and migrated.
//
// The tool walks a fixed list of Beads tables, SELECTs all rows from Dolt,
// and INSERTs them into Postgres beads.<table>, setting the rig column
// explicitly per row.
//
// MySQL → Postgres type mapping is handled by the driver layer plus a small
// amount of fixup:
//
//   - TINYINT(1)  → BOOL   (the MySQL driver returns int64; we coerce to bool
//                          when the destination column is boolean)
//   - DATETIME    → TIMESTAMPTZ (time.Time round-trips natively)
//   - JSON        → JSONB  (we pass the raw bytes and use ::jsonb cast)
//
// Example:
//
//	bd-pgmigrate \
//	  --dolt-uri="root@tcp(127.0.0.1:3307)/" \
//	  --postgres-uri="postgres://remote:remote@127.0.0.1:5433/beads?sslmode=disable" \
//	  --source-database=hq \
//	  --target-rig=hq \
//	  --validate
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// migrationTables lists the Beads tables to copy from Dolt to Postgres,
// in dependency-safe order (parents before children, FK-wise).
//
// Skipped on purpose:
//   - schema_migrations  (Postgres has its own migration tracker)
//   - __restart_integrity__  (Solartown-specific test marker)
//   - blocked_issues / ready_issues  (these are views in Dolt, not tables)
var migrationTables = []string{
	// Issues comes first — many tables FK back to it.
	"issues",
	"dependencies",
	"comments",
	"events",
	"labels",
	// Config / metadata tables (no FKs; order doesn't matter).
	"config",
	"metadata",
	"local_metadata",
	"child_counters",
	"issue_counter",
	"issue_snapshots",
	"compaction_snapshots",
	"repo_mtimes",
	"routes",
	"federation_peers",
	"interactions",
	// Wisps form a parallel hierarchy.
	"wisps",
	"wisp_comments",
	"wisp_dependencies",
	"wisp_events",
	"wisp_labels",
	// Custom-status / custom-type tables.
	"custom_statuses",
	"custom_types",
}

func main() {
	var (
		doltURI        = flag.String("dolt-uri", "root@tcp(127.0.0.1:3307)/", "Dolt server connection URI (MySQL DSN, no database)")
		postgresURI    = flag.String("postgres-uri", "", "Postgres URI (libpq-style); REQUIRED")
		sourceDatabase = flag.String("source-database", "", "Source Dolt database name (e.g. hq); REQUIRED")
		targetRig      = flag.String("target-rig", "", "Rig value to write into Postgres (defaults to --source-database)")
		dryRun         = flag.Bool("dry-run", false, "Read+validate but don't write to Postgres")
		validate       = flag.Bool("validate", false, "After migration, compare row counts source vs. target")
		limit          = flag.Int("limit", 0, "Cap rows per table (0 = unlimited). Useful for tests/smoke runs.")
	)
	flag.Parse()

	if *postgresURI == "" {
		fatal("--postgres-uri is required")
	}
	if *sourceDatabase == "" {
		fatal("--source-database is required")
	}
	if *targetRig == "" {
		*targetRig = *sourceDatabase
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	if err := run(ctx, *doltURI, *postgresURI, *sourceDatabase, *targetRig, *dryRun, *validate, *limit); err != nil {
		fatal(err.Error())
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "bd-pgmigrate: "+msg)
	os.Exit(1)
}

// run is the main migration entry point, factored out so tests can drive it.
func run(ctx context.Context, doltURI, pgURI, sourceDB, targetRig string, dryRun, validate bool, limit int) error {
	dolt, err := openDolt(ctx, doltURI, sourceDB)
	if err != nil {
		return fmt.Errorf("open dolt: %w", err)
	}
	defer dolt.Close()

	pg, err := openPostgres(ctx, pgURI)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer pg.Close()

	pgCols, err := loadPostgresColumns(ctx, pg)
	if err != nil {
		return fmt.Errorf("load postgres column metadata: %w", err)
	}

	totals := map[string]int{}
	start := time.Now()
	for _, table := range migrationTables {
		n, err := copyTable(ctx, dolt, pg, table, targetRig, pgCols[table], dryRun, limit)
		if err != nil {
			return fmt.Errorf("copy %s: %w", table, err)
		}
		totals[table] = n
		mode := "copied"
		if dryRun {
			mode = "would copy"
		}
		fmt.Printf("  %-22s %s %d rows\n", table, mode, n)
	}

	total := 0
	for _, n := range totals {
		total += n
	}
	fmt.Printf("\nDone in %s — %d rows across %d tables (rig=%s, source=%s, dry-run=%v)\n",
		time.Since(start).Round(time.Millisecond), total, len(migrationTables), targetRig, sourceDB, dryRun)

	if validate {
		fmt.Println("\nValidating row counts...")
		if err := validateCounts(ctx, dolt, pg, targetRig); err != nil {
			return fmt.Errorf("validate: %w", err)
		}
	}
	return nil
}

// openDolt opens a MySQL/Dolt connection scoped to the given source database.
func openDolt(ctx context.Context, baseURI, sourceDB string) (*sql.DB, error) {
	// MySQL DSN format is "user:pass@tcp(host:port)/dbname?args". The provided
	// baseURI typically ends in "/" — append the source DB name (and re-attach
	// any "?args" suffix if the user provided one).
	dsn := buildMySQLDSN(baseURI, sourceDB)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping (dsn=%s): %w", dsn, err)
	}
	return db, nil
}

// buildMySQLDSN inserts sourceDB into a MySQL DSN, preserving any query-string args.
// Examples:
//
//	buildMySQLDSN("root@tcp(127.0.0.1:3307)/", "hq") = "root@tcp(127.0.0.1:3307)/hq?parseTime=true"
//	buildMySQLDSN("root@tcp(127.0.0.1:3307)/?charset=utf8", "hq") = "root@tcp(127.0.0.1:3307)/hq?charset=utf8&parseTime=true"
func buildMySQLDSN(baseURI, sourceDB string) string {
	// Split on the last "/" — everything before it is the auth+host segment,
	// everything after it is "<dbname>?<args>".
	slash := strings.LastIndex(baseURI, "/")
	if slash < 0 {
		// No slash — synthesize one.
		return baseURI + "/" + sourceDB + "?parseTime=true"
	}
	prefix := baseURI[:slash+1]
	tail := baseURI[slash+1:]
	args := ""
	if q := strings.Index(tail, "?"); q >= 0 {
		args = tail[q+1:]
	}
	dsn := prefix + sourceDB
	if args == "" {
		dsn += "?parseTime=true"
	} else {
		// Ensure parseTime=true is set so DATETIME columns scan as time.Time.
		if !strings.Contains(args, "parseTime=") {
			args += "&parseTime=true"
		}
		dsn += "?" + args
	}
	return dsn
}

// openPostgres opens a Postgres connection via the pgx stdlib driver.
func openPostgres(ctx context.Context, uri string) (*sql.DB, error) {
	db, err := sql.Open("pgx", uri)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// pgColumn captures the bits of column metadata we need for type-aware INSERTs.
type pgColumn struct {
	Name     string
	DataType string // "boolean", "jsonb", "timestamp with time zone", ...
	Nullable bool
	Default  sql.NullString
}

// loadPostgresColumns fetches column metadata for every beads.* table in the
// target database, keyed by table name. Used to:
//
//  1. Know which Dolt columns to copy (intersection with Postgres columns).
//  2. Know which Postgres columns are NOT NULL with no default — so we can
//     supply zero-values when the source column is missing or NULL.
//  3. Know when a Postgres column is boolean (so we coerce TINYINT) or jsonb
//     (so we cast with ::jsonb).
func loadPostgresColumns(ctx context.Context, pg *sql.DB) (map[string]map[string]pgColumn, error) {
	const q = `
		SELECT table_name, column_name, data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = 'beads'
		ORDER BY table_name, ordinal_position
	`
	rows, err := pg.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]map[string]pgColumn{}
	for rows.Next() {
		var (
			tbl, col, dt, nullable string
			def                    sql.NullString
		)
		if err := rows.Scan(&tbl, &col, &dt, &nullable, &def); err != nil {
			return nil, err
		}
		if _, ok := out[tbl]; !ok {
			out[tbl] = map[string]pgColumn{}
		}
		out[tbl][col] = pgColumn{
			Name:     col,
			DataType: dt,
			Nullable: nullable == "YES",
			Default:  def,
		}
	}
	return out, rows.Err()
}

// copyTable copies a single Dolt table into Postgres beads.<table>.
//
// Algorithm:
//  1. Read column names from Dolt via SHOW COLUMNS.
//  2. Compute the column-set to copy: intersection of (Dolt columns,
//     Postgres beads.<table> columns), plus we always inject "rig".
//  3. SELECT all matching columns from Dolt, batched into a transaction
//     against Postgres.
//  4. INSERT one row at a time using parameterised queries with type-aware
//     casts (jsonb, boolean coercion).
//
// Returns the number of rows actually written (or — in dry-run mode — the
// number that WOULD have been written).
func copyTable(ctx context.Context, dolt, pg *sql.DB, table, targetRig string, pgTableCols map[string]pgColumn, dryRun bool, limit int) (int, error) {
	if pgTableCols == nil {
		return 0, fmt.Errorf("postgres table beads.%s does not exist", table)
	}

	doltCols, err := readDoltColumns(ctx, dolt, table)
	if err != nil {
		return 0, err
	}

	// Compute the columns to copy. We always want "rig" in the destination,
	// either from the source (wisps has a "rig" column already) or injected
	// from --target-rig.
	var copies []colCopy
	doltColSet := map[string]bool{}
	for _, c := range doltCols {
		doltColSet[c] = true
	}
	for _, name := range orderedKeys(pgTableCols) {
		pgInfo := pgTableCols[name]
		if name == "rig" {
			// Always inject the rig — overrides the source value if any. This
			// matches the spec ("set rig=<target-rig> explicitly").
			copies = append(copies, colCopy{Name: name, FromDolt: false, PgInfo: pgInfo})
			continue
		}
		if doltColSet[name] {
			copies = append(copies, colCopy{Name: name, FromDolt: true, PgInfo: pgInfo})
		}
	}

	// Build the SELECT projection in the same order as `copies` (only FromDolt
	// entries appear in the SELECT — others are synthesised).
	var sel []string
	for _, c := range copies {
		if c.FromDolt {
			sel = append(sel, "`"+c.Name+"`")
		}
	}
	selectQ := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(sel, ", "), table)
	if limit > 0 {
		selectQ += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := dolt.QueryContext(ctx, selectQ)
	if err != nil {
		return 0, fmt.Errorf("dolt select: %w", err)
	}
	defer rows.Close()

	// We need typed scan targets for boolean coercion. Easiest: scan into
	// []interface{} and treat each column based on its Postgres data_type.
	// First gather the Dolt column types via ColumnTypes for further nuance.
	doltTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}
	_ = doltTypes // we don't currently use the Dolt-side types — Postgres is the source of truth for coercion

	// Build the INSERT statement once.
	insertQ := buildInsertQuery(table, copies)

	var tx *sql.Tx
	if !dryRun {
		tx, err = pg.BeginTx(ctx, nil)
		if err != nil {
			return 0, fmt.Errorf("begin tx: %w", err)
		}
		defer func() {
			_ = tx.Rollback() // safe no-op after commit
		}()
		if _, err := tx.ExecContext(ctx, "SET LOCAL search_path TO beads, public"); err != nil {
			return 0, fmt.Errorf("set search_path: %w", err)
		}
	}

	count := 0
	skipped := 0
	for rows.Next() {
		// Build scan targets. One *interface{} per FromDolt column.
		scanTargets := make([]interface{}, 0, len(copies))
		holders := make([]interface{}, 0, len(copies))
		for _, c := range copies {
			if !c.FromDolt {
				continue
			}
			var v interface{}
			holders = append(holders, &v)
			scanTargets = append(scanTargets, &v)
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return count, fmt.Errorf("scan: %w", err)
		}

		// Build the actual INSERT args, applying coercions.
		args := make([]interface{}, 0, len(copies))
		holderIdx := 0
		for _, c := range copies {
			if !c.FromDolt {
				// Synthesised — currently only "rig".
				if c.Name == "rig" {
					args = append(args, targetRig)
					continue
				}
				args = append(args, nil) // shouldn't happen
				continue
			}
			raw := *(holders[holderIdx].(*interface{}))
			holderIdx++
			args = append(args, coerce(raw, c.PgInfo))
		}

		if !dryRun {
			// Use a savepoint so FK violations can be skipped without
			// aborting the whole table. Real-world Dolt rigs contain
			// cross-rig and wisp-vs-issue dependency rows that would
			// otherwise FK-fail; we log+skip those.
			if _, err := tx.ExecContext(ctx, "SAVEPOINT row_sp"); err != nil {
				return count, fmt.Errorf("savepoint: %w", err)
			}
			if _, err := tx.ExecContext(ctx, insertQ, args...); err != nil {
				if isFKError(err) {
					skipped++
					if _, rerr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT row_sp"); rerr != nil {
						return count, fmt.Errorf("rollback to savepoint: %w", rerr)
					}
					continue
				}
				return count, fmt.Errorf("insert row %d: %w", count+1, err)
			}
			if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT row_sp"); err != nil {
				return count, fmt.Errorf("release savepoint: %w", err)
			}
		}
		count++
	}
	if skipped > 0 {
		fmt.Printf("    (skipped %d row(s) with FK violations in %s — likely cross-rig or wisp/issue refs)\n", skipped, table)
	}
	if err := rows.Err(); err != nil {
		return count, fmt.Errorf("dolt rows: %w", err)
	}

	if !dryRun {
		if err := tx.Commit(); err != nil {
			return count, fmt.Errorf("commit: %w", err)
		}
	}
	return count, nil
}

// orderedKeys returns map keys in sorted order — gives stable INSERT column ordering.
func orderedKeys(m map[string]pgColumn) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// We want a stable but readable order: keep the natural information_schema
	// ordinal order would be ideal, but since we already store them
	// unordered, lexical works as deterministic fallback.
	// Use sort.Strings for stability:
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
	return keys
}

// colCopy describes one column being written into Postgres beads.<table>.
// FromDolt=true means scan the value from the source row; otherwise the
// value is synthesised by the tool (currently only "rig" is synthesised).
type colCopy struct {
	Name     string
	FromDolt bool
	PgInfo   pgColumn
}

// buildInsertQuery builds an INSERT for one row across the columns listed.
// Columns whose Postgres type is jsonb get a "::jsonb" cast on the placeholder
// so json-text inputs round-trip correctly.
func buildInsertQuery(table string, copies []colCopy) string {
	var cols, placeholders []string
	for i, c := range copies {
		cols = append(cols, `"`+c.Name+`"`)
		ph := fmt.Sprintf("$%d", i+1)
		if strings.EqualFold(c.PgInfo.DataType, "jsonb") {
			ph += "::jsonb"
		} else if strings.EqualFold(c.PgInfo.DataType, "json") {
			ph += "::json"
		}
		placeholders = append(placeholders, ph)
	}
	return fmt.Sprintf(
		`INSERT INTO beads.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING`,
		table,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)
}

// readDoltColumns lists the column names of a Dolt table in their natural order.
func readDoltColumns(ctx context.Context, dolt *sql.DB, table string) ([]string, error) {
	rows, err := dolt.QueryContext(ctx, "SHOW COLUMNS FROM `"+table+"`")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var (
			field string
			t     sql.NullString
			n, k  sql.NullString
			d     sql.NullString
			ex    sql.NullString
		)
		if err := rows.Scan(&field, &t, &n, &k, &d, &ex); err != nil {
			return nil, err
		}
		cols = append(cols, field)
	}
	return cols, rows.Err()
}

// coerce converts a value scanned from Dolt (via the MySQL driver) into a
// value the pgx driver will accept for the destination Postgres column.
//
// Specific conversions:
//
//   - boolean column + numeric input → bool (TINYINT(1) → BOOL)
//   - jsonb/json column + []byte     → string (raw JSON text)
//   - everything else                 → pass-through (driver handles time, int, string, []byte)
func coerce(raw interface{}, pg pgColumn) interface{} {
	if raw == nil {
		return nil
	}
	dt := strings.ToLower(pg.DataType)
	switch dt {
	case "boolean":
		switch v := raw.(type) {
		case bool:
			return v
		case int64:
			return v != 0
		case int:
			return v != 0
		case []byte:
			s := strings.TrimSpace(string(v))
			return s == "1" || strings.EqualFold(s, "true")
		case string:
			s := strings.TrimSpace(v)
			return s == "1" || strings.EqualFold(s, "true")
		}
	case "jsonb", "json":
		// JSON column. The MySQL driver returns []byte. Coerce to string so
		// pgx sends it as text and the ::jsonb cast in the INSERT does the rest.
		switch v := raw.(type) {
		case []byte:
			if len(v) == 0 {
				return "{}"
			}
			return string(v)
		case string:
			if v == "" {
				return "{}"
			}
			return v
		}
	case "uuid":
		// pgx accepts string for uuid; nothing to do.
	}
	// Default: pass-through. time.Time, int64, []byte→bytea, string all work.
	return raw
}

// isFKError reports whether err is a Postgres foreign-key violation
// (SQLSTATE 23503). We detect via string match to avoid pulling in pgconn
// directly — pgx returns errors whose Error() includes the SQLSTATE in
// parentheses.
func isFKError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLSTATE 23503") ||
		strings.Contains(msg, "foreign key constraint")
}

// validateCounts compares row counts in Dolt vs. Postgres for each migrated
// table, scoped to the target rig. Prints a summary and returns an error if
// any table mismatches.
func validateCounts(ctx context.Context, dolt, pg *sql.DB, targetRig string) error {
	var mismatches []string
	for _, table := range migrationTables {
		var src, dst int
		if err := dolt.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+table+"`").Scan(&src); err != nil {
			return fmt.Errorf("dolt count %s: %w", table, err)
		}

		// Postgres count: filter by rig where the table has a rig column;
		// otherwise unconditional. local_metadata is the only rig-less
		// table in our migration list.
		var pgQuery string
		if table == "local_metadata" {
			pgQuery = "SELECT COUNT(*) FROM beads." + table
		} else {
			pgQuery = "SELECT COUNT(*) FROM beads." + table + " WHERE rig = $1"
		}
		var err error
		if table == "local_metadata" {
			err = pg.QueryRowContext(ctx, pgQuery).Scan(&dst)
		} else {
			err = pg.QueryRowContext(ctx, pgQuery, targetRig).Scan(&dst)
		}
		if err != nil {
			return fmt.Errorf("pg count %s: %w", table, err)
		}

		status := "ok"
		if src != dst {
			status = "MISMATCH"
			mismatches = append(mismatches, fmt.Sprintf("%s: dolt=%d pg=%d", table, src, dst))
		}
		fmt.Printf("  %-22s dolt=%-7d pg=%-7d %s\n", table, src, dst, status)
	}
	if len(mismatches) > 0 {
		return errors.New("count mismatch: " + strings.Join(mismatches, "; "))
	}
	return nil
}
