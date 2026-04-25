package postgres

import (
	"context"
	"database/sql"
	"errors"
)

// UnderlyingDB returns the raw *sql.DB. Implements storage.RawDBAccessor.
// Same handle as DB(); both exist because the Storage hierarchy distinguishes
// "the public DB() method" from the "raw escape hatch" used by migrations.
func (s *PostgresStore) UnderlyingDB() *sql.DB {
	return s.db
}

// Path returns the connection string this store was opened with.
// Postgres has no on-disk path the way Dolt does; the connection string is
// the closest equivalent.
func (s *PostgresStore) Path() string {
	return s.connStr
}

// CLIDir returns "" — Postgres has no per-database CLI directory the way
// Dolt does. Implements storage.StoreLocator for interface parity; callers
// that need a CLI directory should special-case the Dolt backend.
func (s *PostgresStore) CLIDir() string {
	return ""
}

// IsClosed reports whether Close() has been called on this store.
func (s *PostgresStore) IsClosed() bool {
	return s.closed.Load()
}

// DoltGC is a Dolt-specific operation. On Postgres the equivalent is VACUUM,
// which is managed by the DBA / autovacuum, not by Beads. Stubbed.
func (s *PostgresStore) DoltGC(ctx context.Context) error {
	return errors.New("DoltGC not applicable to postgres backend; use VACUUM instead")
}

// Flatten squashes Dolt commit history into a single commit; meaningless on
// Postgres, which has no per-commit history layer. Stubbed.
func (s *PostgresStore) Flatten(ctx context.Context) error {
	return errors.New("Flatten is dolt-specific")
}

// Compact is a Dolt commit-history operation. Stubbed on Postgres.
func (s *PostgresStore) Compact(ctx context.Context, initialHash, boundaryHash string, oldCommits int, recentHashes []string) error {
	return errors.New("Compact is dolt-specific")
}

// CommitPending is a no-op on Postgres: the backend autocommits per
// transaction, so there is nothing to flush. Returns (false, nil) to signal
// "nothing was committed" without raising an error.
func (s *PostgresStore) CommitPending(ctx context.Context, actor string) (bool, error) {
	return false, nil
}

// BackupAdd registers a Dolt backup remote. Stubbed; use pg_dump or
// pg_basebackup for Postgres backups.
func (s *PostgresStore) BackupAdd(ctx context.Context, name, url string) error {
	return errors.New("backup management is dolt-specific; use pg_dump for postgres")
}

// BackupSync is the Dolt CALL DOLT_BACKUP equivalent. Stubbed.
func (s *PostgresStore) BackupSync(ctx context.Context, name string) error {
	return errors.New("backup management is dolt-specific; use pg_dump for postgres")
}

// BackupRemove unregisters a Dolt backup remote. Stubbed.
func (s *PostgresStore) BackupRemove(ctx context.Context, name string) error {
	return errors.New("backup management is dolt-specific; use pg_dump for postgres")
}

// BackupDatabase performs a Dolt file-backed backup. Stubbed.
func (s *PostgresStore) BackupDatabase(ctx context.Context, dir string) error {
	return errors.New("backup management is dolt-specific; use pg_dump for postgres")
}

// RestoreDatabase restores from a Dolt backup. Stubbed.
func (s *PostgresStore) RestoreDatabase(ctx context.Context, dir string, force bool) error {
	return errors.New("backup management is dolt-specific; use pg_dump for postgres")
}
