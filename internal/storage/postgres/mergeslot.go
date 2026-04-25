package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// mergeSlotID is the canonical bead ID for the per-rig merge slot.
//
// Note: the shared MergeSlotImpl helpers in internal/storage/merge_slot.go
// derive this from the issue_prefix config ("<prefix>-merge-slot"). The
// Postgres backend uses the literal "merge-slot" id per assignment, scoped
// per rig via the rig column.
const mergeSlotID = "merge-slot"

// mergeSlotMeta is the JSON shape stored in beads.issues.metadata for the
// merge slot bead.
type mergeSlotMeta struct {
	Holder  string   `json:"holder"`
	Waiters []string `json:"waiters"`
}

// mergeSlotAcquireTimeout caps blocking waits in MergeSlotAcquire. Callers
// that need different semantics should poll MergeSlotCheck themselves.
const mergeSlotAcquireTimeout = 30 * time.Second

// mergeSlotPollInterval is the backoff between FOR UPDATE attempts when
// waiting for the slot to become free.
const mergeSlotPollInterval = 250 * time.Millisecond

// MergeSlotCreate creates the merge-slot lock bead if it does not exist.
// Idempotent: returns the existing slot if one is already present.
func (s *PostgresStore) MergeSlotCreate(ctx context.Context, actor string) (*types.Issue, error) {
	// Fast path: already exists.
	if existing, err := s.GetIssue(ctx, mergeSlotID); err == nil {
		return existing, nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	initialMeta, _ := json.Marshal(mergeSlotMeta{Holder: "", Waiters: []string{}})

	err := s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const q = `
			INSERT INTO beads.issues (
				id, rig, title, description,
				status, priority, issue_type,
				created_by, metadata
			) VALUES (
				$1, $2, $3, $4,
				'open', 0, 'merge_slot',
				$5, $6::jsonb
			)
			ON CONFLICT (id) DO NOTHING
		`
		_, err := tx.ExecContext(ctx, q,
			mergeSlotID, s.rig,
			"Merge Slot",
			"Refinery merge-queue lock bead. Holder serializes PR merges; waiters queue.",
			actor, string(initialMeta),
		)
		if err != nil {
			return fmt.Errorf("postgres: insert merge slot: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.GetIssue(ctx, mergeSlotID)
}

// MergeSlotCheck returns the current merge-slot status. Returns
// storage.ErrNotFound if the slot has never been created.
func (s *PostgresStore) MergeSlotCheck(ctx context.Context) (*storage.MergeSlotStatus, error) {
	const q = `
		SELECT metadata FROM beads.issues
		WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
	`
	var raw []byte
	err := s.db.QueryRowContext(ctx, q, mergeSlotID, s.rig).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: read merge slot: %w", err)
	}

	meta := decodeMergeSlotMeta(raw)
	return &storage.MergeSlotStatus{
		SlotID:    mergeSlotID,
		Available: meta.Holder == "",
		Holder:    meta.Holder,
		Waiters:   meta.Waiters,
	}, nil
}

// MergeSlotAcquire attempts to acquire the merge slot for holder.
//
// If the slot is free (holder=="" in metadata) it sets holder atomically
// (SELECT ... FOR UPDATE inside a transaction) and returns Acquired=true.
//
// If the slot is held by someone else and wait==false, returns immediately
// with Acquired=false (and Waiting=false).
//
// If the slot is held and wait==true, the caller is appended to the waiters
// list and the function polls (every mergeSlotPollInterval) for up to
// mergeSlotAcquireTimeout (30s). On acquisition the caller is removed from
// the waiters list and Acquired=true. On timeout the caller stays in the
// waiters list and the function returns with Waiting=true, Acquired=false.
func (s *PostgresStore) MergeSlotAcquire(ctx context.Context, holder, actor string, wait bool) (*storage.MergeSlotResult, error) {
	if holder == "" {
		return nil, errors.New("postgres: MergeSlotAcquire: empty holder")
	}

	// First attempt — also enqueues if necessary.
	res, err := s.tryAcquireMergeSlot(ctx, holder, actor, wait)
	if err != nil {
		return nil, err
	}
	if res.Acquired || !wait || !res.Waiting {
		return res, nil
	}

	// Poll until acquired or timeout.
	deadline := time.Now().Add(mergeSlotAcquireTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(mergeSlotPollInterval):
		}
		next, err := s.tryAcquireMergeSlot(ctx, holder, actor, true)
		if err != nil {
			return nil, err
		}
		if next.Acquired {
			return next, nil
		}
		res = next
	}
	// Timed out; caller remains in waiters.
	return res, nil
}

// tryAcquireMergeSlot performs one transactional acquisition attempt.
func (s *PostgresStore) tryAcquireMergeSlot(ctx context.Context, holder, actor string, wait bool) (*storage.MergeSlotResult, error) {
	result := &storage.MergeSlotResult{SlotID: mergeSlotID}

	err := s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const sel = `
			SELECT metadata FROM beads.issues
			WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
			FOR UPDATE
		`
		var raw []byte
		if err := tx.QueryRowContext(ctx, sel, mergeSlotID, s.rig).Scan(&raw); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return storage.ErrNotFound
			}
			return fmt.Errorf("postgres: lock merge slot: %w", err)
		}
		meta := decodeMergeSlotMeta(raw)

		if meta.Holder == "" {
			// Free — take it. If we were a waiter, drop ourselves.
			meta.Waiters = removeString(meta.Waiters, holder)
			meta.Holder = holder
			if err := writeMergeSlotMeta(ctx, tx, s.rig, meta); err != nil {
				return err
			}
			result.Acquired = true
			result.Holder = holder
			return nil
		}

		if meta.Holder == holder {
			// Already ours — idempotent.
			result.Acquired = true
			result.Holder = holder
			return nil
		}

		// Held by someone else.
		result.Holder = meta.Holder
		if !wait {
			return nil
		}

		// Append to waiters if not already present.
		pos := indexOf(meta.Waiters, holder)
		if pos < 0 {
			meta.Waiters = append(meta.Waiters, holder)
			pos = len(meta.Waiters) - 1
			if err := writeMergeSlotMeta(ctx, tx, s.rig, meta); err != nil {
				return err
			}
		}
		result.Waiting = true
		result.Position = pos + 1
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MergeSlotRelease releases the slot if currently held by holder. If a
// waiter is queued, the first waiter is promoted to holder atomically.
// Returns an error if the current holder does not match.
func (s *PostgresStore) MergeSlotRelease(ctx context.Context, holder, actor string) error {
	if holder == "" {
		return errors.New("postgres: MergeSlotRelease: empty holder")
	}
	return s.withTx(ctx, actor, func(tx *sql.Tx) error {
		const sel = `
			SELECT metadata FROM beads.issues
			WHERE id = $1 AND rig = $2 AND deleted_at IS NULL
			FOR UPDATE
		`
		var raw []byte
		if err := tx.QueryRowContext(ctx, sel, mergeSlotID, s.rig).Scan(&raw); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return storage.ErrNotFound
			}
			return fmt.Errorf("postgres: lock merge slot: %w", err)
		}
		meta := decodeMergeSlotMeta(raw)

		if meta.Holder != holder {
			return fmt.Errorf("postgres: merge slot held by %q, not %q", meta.Holder, holder)
		}

		// Promote first waiter (if any).
		if len(meta.Waiters) > 0 {
			meta.Holder = meta.Waiters[0]
			meta.Waiters = meta.Waiters[1:]
		} else {
			meta.Holder = ""
		}
		return writeMergeSlotMeta(ctx, tx, s.rig, meta)
	})
}

// ════════════════════════════════════════════════════════════════════════
// helpers
// ════════════════════════════════════════════════════════════════════════

func decodeMergeSlotMeta(raw []byte) mergeSlotMeta {
	var meta mergeSlotMeta
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &meta)
	}
	if meta.Waiters == nil {
		meta.Waiters = []string{}
	}
	return meta
}

func writeMergeSlotMeta(ctx context.Context, tx *sql.Tx, rig string, meta mergeSlotMeta) error {
	if meta.Waiters == nil {
		meta.Waiters = []string{}
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("postgres: encode merge slot meta: %w", err)
	}
	const upd = `
		UPDATE beads.issues SET metadata = $1::jsonb
		WHERE id = $2 AND rig = $3 AND deleted_at IS NULL
	`
	if _, err := tx.ExecContext(ctx, upd, string(b), mergeSlotID, rig); err != nil {
		return fmt.Errorf("postgres: write merge slot meta: %w", err)
	}
	return nil
}

func removeString(xs []string, target string) []string {
	out := xs[:0]
	for _, x := range xs {
		if x != target {
			out = append(out, x)
		}
	}
	// Preserve the slice's len/cap behaviour for callers — but return a copy
	// to avoid surprising aliasing.
	cp := make([]string, len(out))
	copy(cp, out)
	return cp
}

func indexOf(xs []string, target string) int {
	for i, x := range xs {
		if x == target {
			return i
		}
	}
	return -1
}
