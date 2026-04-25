package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
)

// SetConfig writes a key/value pair into the rig-scoped config table.
// Inserts on first use, updates on subsequent calls (UPSERT).
func (s *PostgresStore) SetConfig(ctx context.Context, key, value string) error {
	if key == "" {
		return errors.New("postgres: SetConfig: empty key")
	}
	return s.withTx(ctx, "system", func(tx *sql.Tx) error {
		const q = `
			INSERT INTO beads.config (rig, key, value)
			VALUES ($1, $2, $3)
			ON CONFLICT (rig, key) DO UPDATE
			   SET value = EXCLUDED.value,
			       deleted_at = NULL
		`
		_, err := tx.ExecContext(ctx, q, s.rig, key, value)
		if err != nil {
			return fmt.Errorf("postgres: set config %q: %w", key, err)
		}
		return nil
	})
}

// GetConfig returns the value for key in the current rig. Returns
// storage.ErrNotFound if not present (or soft-deleted).
func (s *PostgresStore) GetConfig(ctx context.Context, key string) (string, error) {
	const q = `
		SELECT value FROM beads.config
		WHERE rig = $1 AND key = $2 AND deleted_at IS NULL
	`
	var v string
	err := s.db.QueryRowContext(ctx, q, s.rig, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("postgres: get config %q: %w", key, err)
	}
	return v, nil
}

// GetAllConfig returns the full key/value map for the current rig (omitting
// soft-deleted entries).
func (s *PostgresStore) GetAllConfig(ctx context.Context) (map[string]string, error) {
	const q = `
		SELECT key, value FROM beads.config
		WHERE rig = $1 AND deleted_at IS NULL
	`
	rows, err := s.db.QueryContext(ctx, q, s.rig)
	if err != nil {
		return nil, fmt.Errorf("postgres: get all config: %w", err)
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("postgres: scan config: %w", err)
		}
		out[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}
	return out, nil
}

// SetLocalMetadata writes to the local_metadata table — machine-local, NOT
// rig-scoped, NOT replicated. Used for ephemeral runtime state per machine.
func (s *PostgresStore) SetLocalMetadata(ctx context.Context, key, value string) error {
	if key == "" {
		return errors.New("postgres: SetLocalMetadata: empty key")
	}
	return s.withTx(ctx, "system", func(tx *sql.Tx) error {
		const q = `
			INSERT INTO beads.local_metadata (key, value)
			VALUES ($1, $2)
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
		`
		_, err := tx.ExecContext(ctx, q, key, value)
		if err != nil {
			return fmt.Errorf("postgres: set local_metadata %q: %w", key, err)
		}
		return nil
	})
}

// GetLocalMetadata returns the value for a local_metadata key.
// Returns storage.ErrNotFound if not present.
func (s *PostgresStore) GetLocalMetadata(ctx context.Context, key string) (string, error) {
	const q = `SELECT value FROM beads.local_metadata WHERE key = $1`
	var v string
	err := s.db.QueryRowContext(ctx, q, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("postgres: get local_metadata %q: %w", key, err)
	}
	return v, nil
}
