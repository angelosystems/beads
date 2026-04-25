package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// migration represents a single .sql file in the migrations directory.
type migration struct {
	version int
	name    string // file basename, e.g. "0001_init.sql"
	body    string
}

// loadMigrations reads embedded migration files and returns them sorted by
// version ascending.
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(embeddedMigrations, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	var ms []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		// Filename format: NNNN_name.sql
		dash := strings.IndexByte(e.Name(), '_')
		if dash < 0 {
			continue
		}
		v, err := strconv.Atoi(e.Name()[:dash])
		if err != nil {
			continue
		}
		body, err := fs.ReadFile(embeddedMigrations, "migrations/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		ms = append(ms, migration{
			version: v,
			name:    e.Name(),
			body:    string(body),
		})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	return ms, nil
}

// Migrate runs all embedded migrations whose version is greater than the
// highest version recorded in beads.schema_migrations. Idempotent: running
// twice is a no-op. The 0001 migration bootstraps the schema_migrations
// table itself, so the first call against a fresh DB applies everything
// in order.
func (s *PostgresStore) Migrate(ctx context.Context) error {
	if s.readOnly {
		return fmt.Errorf("postgres: cannot migrate a read-only store")
	}

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		return fmt.Errorf("postgres: no embedded migrations found")
	}

	// Determine current applied version. If schema_migrations doesn't exist
	// yet, this returns 0 — the very first run.
	var currentVersion int
	const versionQuery = `
		SELECT COALESCE(MAX(version), 0)
		FROM beads.schema_migrations
	`
	row := s.db.QueryRowContext(ctx, versionQuery)
	if err := row.Scan(&currentVersion); err != nil {
		// If the table doesn't exist, treat as 0 and let 0001 create it.
		if !isUndefinedTable(err) {
			return fmt.Errorf("postgres: check schema_migrations: %w", err)
		}
		currentVersion = 0
	}

	for _, m := range migrations {
		if m.version <= currentVersion {
			continue
		}
		// Each migration file already wraps its DDL in BEGIN/COMMIT and
		// inserts its own version row into schema_migrations. So we just
		// execute it as a single statement batch.
		if _, err := s.db.ExecContext(ctx, m.body); err != nil {
			return fmt.Errorf("postgres: apply migration %s: %w", m.name, err)
		}
	}

	return nil
}

// isUndefinedTable returns true if err is the Postgres "undefined_table"
// error (SQLSTATE 42P01). Used to detect the cold-start case where
// beads.schema_migrations does not yet exist.
func isUndefinedTable(err error) bool {
	if err == nil {
		return false
	}
	// pgx exposes pgconn.PgError; we match by code via string.
	// Avoiding a hard dep on pgconn here keeps this file driver-agnostic.
	return strings.Contains(err.Error(), "42P01") ||
		strings.Contains(err.Error(), "does not exist")
}
