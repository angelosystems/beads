package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
)

// SlotSet stores key->value as a JSON string inside the issue's metadata
// JSONB column. The value is stored as a JSON string (text), so callers
// reading back via SlotGet always receive a string.
//
// jsonb_set's path argument requires a TEXT[] — we build it with array_append
// at SQL level via to_jsonb(...) for safety, ensuring arbitrary keys are
// not interpreted as JSON path syntax.
func (s *PostgresStore) SlotSet(ctx context.Context, issueID, key, value, actor string) error {
	if issueID == "" {
		return errors.New("postgres: SlotSet: empty issue ID")
	}
	if key == "" {
		return errors.New("postgres: SlotSet: empty key")
	}
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		// jsonb_set(metadata, ARRAY[key], to_jsonb(value), true) — true = create
		// the key if missing. ARRAY[$3] keeps the key text-safe (no JSONPath
		// interpretation).
		const q = `
			UPDATE beads.issues
			   SET metadata = jsonb_set(
			       COALESCE(metadata, '{}'::jsonb),
			       ARRAY[$3]::text[],
			       to_jsonb($4::text),
			       true
			   )
			 WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
		`
		res, err := tx.ExecContext(ctx, q, issueID, s.rig, key, value)
		if err != nil {
			return fmt.Errorf("postgres: slot set %q.%q: %w", issueID, key, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("postgres: slot set rows: %w", err)
		}
		if n == 0 {
			return storage.ErrNotFound
		}
		return nil
	})
}

// SlotGet returns the string value for issueID's metadata key. Returns
// storage.ErrNotFound when the issue does not exist OR when the key is not
// present in its metadata.
//
// We distinguish "absent key" from "present-but-empty" by first checking
// `metadata ? key` (JSONB key-exists). An empty string at a present key
// returns ("", nil); a missing key returns ("", ErrNotFound).
func (s *PostgresStore) SlotGet(ctx context.Context, issueID, key string) (string, error) {
	if issueID == "" {
		return "", errors.New("postgres: SlotGet: empty issue ID")
	}
	if key == "" {
		return "", errors.New("postgres: SlotGet: empty key")
	}
	const q = `
		SELECT
		    (metadata ? $3)        AS has_key,
		    COALESCE(metadata->>$3, '') AS val
		FROM beads.issues
		WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
	`
	var hasKey bool
	var val string
	err := s.db.QueryRowContext(ctx, q, issueID, s.rig, key).Scan(&hasKey, &val)
	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("postgres: slot get %q.%q: %w", issueID, key, err)
	}
	if !hasKey {
		return "", storage.ErrNotFound
	}
	return val, nil
}

// SlotClear removes key from the issue's metadata. Returns
// storage.ErrNotFound when the issue is missing or soft-deleted. Removing
// an already-absent key is a no-op (no error).
func (s *PostgresStore) SlotClear(ctx context.Context, issueID, key, actor string) error {
	if issueID == "" {
		return errors.New("postgres: SlotClear: empty issue ID")
	}
	if key == "" {
		return errors.New("postgres: SlotClear: empty key")
	}
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const q = `
			UPDATE beads.issues
			   SET metadata = COALESCE(metadata, '{}'::jsonb) - $3::text
			 WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
		`
		res, err := tx.ExecContext(ctx, q, issueID, s.rig, key)
		if err != nil {
			return fmt.Errorf("postgres: slot clear %q.%q: %w", issueID, key, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("postgres: slot clear rows: %w", err)
		}
		if n == 0 {
			return storage.ErrNotFound
		}
		return nil
	})
}
