// Package postgres implements the storage interface using Postgres as backend.
//
// This is the second backend implementation (after dolt). Both satisfy the
// storage.Storage interface, allowing Beads consumers to switch via config
// without code changes.
//
// Postgres backend characteristics vs. Dolt backend:
//   - Mature operational tooling (30+ years vs. ~5)
//   - ElectricSQL-compatible (mobile-sync via logical replication)
//   - No native data versioning — audit trail via _audit tables + triggers
//   - No cross-machine sync via "git push" — uses Postgres logical replication
//
// Schema lives in internal/storage/postgres/migrations/*.sql and is embedded
// at build-time via //go:embed. Multi-rig support is built-in: every entity
// table has a "rig" column. See migrations/0001_init.sql for the full pattern.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	_ "github.com/jackc/pgx/v5/stdlib" // registers "pgx" driver with database/sql

	"github.com/steveyegge/beads/internal/storage"
)

// PostgresStore implements storage.Storage backed by a Postgres database.
//
// Compared to DoltStore, this is intentionally schlank: no per-invocation
// caches, no auto-start lifecycle (Postgres is assumed to be running and
// reachable). Caches can be added later if benchmarks show they help.
type PostgresStore struct {
	db        *sql.DB
	connStr   string
	rig       string // active rig — every query filters/inserts with this
	closed    atomic.Bool
	mu        sync.RWMutex
	readOnly  bool
}

// Config configures a PostgresStore.
type Config struct {
	// ConnString is a libpq-style Postgres connection string, e.g.
	// "postgres://user:pass@host:5432/dbname?sslmode=disable".
	ConnString string

	// Rig identifies the rig this store operates on. Every read/write is
	// scoped to this rig automatically. Use "default" for single-rig setups.
	Rig string

	// ReadOnly opens the store in read-only mode (no writes allowed).
	ReadOnly bool

	// AutoMigrate runs pending migrations on Open. Default false; callers
	// typically run migrations explicitly via the migration runner.
	AutoMigrate bool
}

// Open creates a new PostgresStore and verifies the connection.
// If cfg.AutoMigrate is true, runs all pending migrations from the embedded
// migrations bundle before returning.
func Open(ctx context.Context, cfg Config) (*PostgresStore, error) {
	if cfg.ConnString == "" {
		return nil, errors.New("postgres: ConnString is required")
	}
	if cfg.Rig == "" {
		cfg.Rig = "default"
	}

	db, err := sql.Open("pgx", cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	s := &PostgresStore{
		db:       db,
		connStr:  cfg.ConnString,
		rig:      cfg.Rig,
		readOnly: cfg.ReadOnly,
	}

	if cfg.AutoMigrate {
		if err := s.Migrate(ctx); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("postgres: migrate: %w", err)
		}
	}

	return s, nil
}

// Close releases the connection pool.
func (s *PostgresStore) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	return s.db.Close()
}

// Rig returns the rig this store is scoped to.
func (s *PostgresStore) Rig() string {
	return s.rig
}

// DB returns the underlying *sql.DB. Exposed for the optional
// storage.RawDBAccessor type assertion.
func (s *PostgresStore) DB() *sql.DB {
	return s.db
}

// setActor sets the session-local "beads.actor" config so the audit-trigger
// captures who is doing the change. Called at the start of every write
// transaction. Safe to call without a transaction (uses session-level config
// scoped to the connection).
func (s *PostgresStore) setActor(ctx context.Context, tx *sql.Tx, actor string) error {
	if actor == "" {
		return nil
	}
	const q = `SELECT set_config('beads.actor', $1, true)`
	if tx != nil {
		_, err := tx.ExecContext(ctx, q, actor)
		return err
	}
	_, err := s.db.ExecContext(ctx, q, actor)
	return err
}

// withTx runs fn inside a transaction with the actor set on the audit trigger.
// Commits on nil error from fn, rolls back otherwise.
func (s *PostgresStore) withTx(ctx context.Context, actor string, fn func(tx *sql.Tx) error) error {
	if s.readOnly {
		return errors.New("postgres: store is read-only")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback() // safe no-op after Commit
	}()

	if err := s.setActor(ctx, tx, actor); err != nil {
		return fmt.Errorf("postgres: set actor: %w", err)
	}

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}

// Compile-time check: PostgresStore satisfies storage.Storage.
var _ storage.Storage = (*PostgresStore)(nil)
