package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/steveyegge/beads/internal/idgen"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

// ensureIssueID generates an ID for the issue if one is not already set,
// mirroring the Dolt backend's prefix + counter/hash strategy. Must run inside
// an active transaction so the collision check and the subsequent INSERT are
// atomic against concurrent creators (GH#2002 in upstream Dolt).
//
// table is "issues" or "wisps" — controls counter-mode applicability and the
// table the collision check scans.
func ensureIssueID(ctx context.Context, tx *sql.Tx, rig, table string, issue *types.Issue, actor string) error {
	if issue == nil {
		return errors.New("postgres: ensureIssueID: nil issue")
	}
	if issue.ID != "" {
		return nil
	}

	configPrefix, err := getIssuePrefixTx(ctx, tx, rig)
	if err != nil {
		return err
	}
	if configPrefix == "" {
		return fmt.Errorf("%w: issue_prefix config is missing for rig %q", storage.ErrNotInitialized, rig)
	}
	configPrefix = strings.TrimSuffix(configPrefix, "-")

	var prefix string
	if table == "wisps" {
		prefix = wispPrefixForIssue(configPrefix, issue)
	} else {
		prefix = configPrefix
		if issue.PrefixOverride != "" {
			prefix = issue.PrefixOverride
		} else if issue.IDPrefix != "" {
			prefix = configPrefix + "-" + issue.IDPrefix
		}
	}

	generated, err := generateIssueIDInTablePg(ctx, tx, table, prefix, rig, issue, actor)
	if err != nil {
		return fmt.Errorf("failed to generate issue ID: %w", err)
	}
	issue.ID = generated
	return nil
}

func getIssuePrefixTx(ctx context.Context, tx *sql.Tx, rig string) (string, error) {
	var v string
	err := tx.QueryRowContext(ctx,
		`SELECT value FROM beads.config WHERE rig = $1 AND key = $2 AND deleted_at IS NULL`,
		rig, "issue_prefix",
	).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("postgres: read issue_prefix for rig %q: %w", rig, err)
	}
	return v, nil
}

func isCounterModeTxPg(ctx context.Context, tx *sql.Tx, rig string) (bool, error) {
	var mode string
	err := tx.QueryRowContext(ctx,
		`SELECT value FROM beads.config WHERE rig = $1 AND key = $2 AND deleted_at IS NULL`,
		rig, "issue_id_mode",
	).Scan(&mode)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("postgres: read issue_id_mode for rig %q: %w", rig, err)
	}
	return mode == "counter", nil
}

// nextCounterIDTxPg atomically allocates the next sequential counter ID for the
// given (rig, prefix). On first use it seeds from existing numeric IDs in
// beads.issues to avoid colliding with manually-numbered legacy IDs.
func nextCounterIDTxPg(ctx context.Context, tx *sql.Tx, rig, prefix string) (string, error) {
	seed, err := maxNumericSuffixPg(ctx, tx, prefix)
	if err != nil {
		return "", err
	}

	// INSERT … ON CONFLICT DO UPDATE makes the increment atomic per (rig, prefix).
	// On insert: last_id starts at seed+1 (claims the next free slot above any
	// pre-existing manually-numbered IDs). On conflict: increment the existing row.
	var nextID int
	err = tx.QueryRowContext(ctx, `
		INSERT INTO beads.issue_counter (rig, prefix, last_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (rig, prefix) DO UPDATE
		   SET last_id = beads.issue_counter.last_id + 1,
		       updated_at = NOW()
		RETURNING last_id
	`, rig, prefix, seed+1).Scan(&nextID)
	if err != nil {
		return "", fmt.Errorf("postgres: allocate next counter for rig=%q prefix=%q: %w", rig, prefix, err)
	}
	return fmt.Sprintf("%s-%d", prefix, nextID), nil
}

// maxNumericSuffixPg scans existing issue IDs of the form "<prefix>-<n>" and
// returns the highest n, or 0 if none exist. Used as the counter seed value
// to avoid colliding with manually-created sequential IDs.
func maxNumericSuffixPg(ctx context.Context, tx *sql.Tx, prefix string) (int, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM beads.issues WHERE id LIKE $1`, prefix+"-%")
	if err != nil {
		return 0, fmt.Errorf("postgres: scan existing IDs for prefix %q: %w", prefix, err)
	}
	defer rows.Close()

	maxNum := 0
	dashed := prefix + "-"
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return 0, fmt.Errorf("postgres: scan existing id: %w", scanErr)
		}
		suffix := strings.TrimPrefix(id, dashed)
		if suffix == id {
			continue
		}
		n, convErr := strconv.Atoi(suffix)
		if convErr != nil {
			continue
		}
		if n > maxNum {
			maxNum = n
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("postgres: iterate existing ids for prefix %q: %w", prefix, err)
	}
	return maxNum, nil
}

// generateIssueIDInTablePg picks an ID strategy (counter vs hash) and produces
// a unique ID against the target table. Mirrors dolt.generateIssueIDInTable.
func generateIssueIDInTablePg(ctx context.Context, tx *sql.Tx, table, prefix, rig string, issue *types.Issue, actor string) (string, error) {
	// Counter mode applies only to the canonical issues table.
	if table == "issues" {
		counterMode, err := isCounterModeTxPg(ctx, tx, rig)
		if err != nil {
			return "", err
		}
		if counterMode {
			return nextCounterIDTxPg(ctx, tx, rig, prefix)
		}
	}

	baseLength := getAdaptiveIDLengthPg(ctx, tx, table, prefix)
	const maxLength = 8
	if baseLength > maxLength {
		baseLength = maxLength
	}

	qualified := "beads." + table
	for length := baseLength; length <= maxLength; length++ {
		for nonce := 0; nonce < 10; nonce++ {
			candidate := idgen.GenerateHashID(prefix, issue.Title, issue.Description, actor, issue.CreatedAt, length, nonce)

			var count int
			query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE id = $1`, qualified)
			if err := tx.QueryRowContext(ctx, query, candidate).Scan(&count); err != nil {
				return "", fmt.Errorf("postgres: id-collision check on %s: %w", qualified, err)
			}
			if count == 0 {
				return candidate, nil
			}
		}
	}
	return "", fmt.Errorf("postgres: failed to generate unique ID for prefix %q after lengths %d-%d × 10 nonces", prefix, baseLength, maxLength)
}

// getAdaptiveIDLengthPg picks a starting hash-ID length based on how many
// existing IDs share the prefix. Larger tables → longer IDs to keep collision
// probability low.
func getAdaptiveIDLengthPg(ctx context.Context, tx *sql.Tx, table, prefix string) int {
	qualified := "beads." + table
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE id LIKE $1`, qualified)

	var count int
	if err := tx.QueryRowContext(ctx, query, prefix+"%").Scan(&count); err != nil {
		return 4
	}
	switch {
	case count < 100:
		return 4
	case count < 1000:
		return 5
	case count < 10000:
		return 6
	default:
		return 7
	}
}

// wispPrefixForIssue mirrors dolt.wispPrefix: PrefixOverride wins, else
// configPrefix-IDPrefix, else "<configPrefix>-wisp".
func wispPrefixForIssue(configPrefix string, issue *types.Issue) string {
	if issue.PrefixOverride != "" {
		return issue.PrefixOverride
	}
	if issue.IDPrefix != "" {
		return configPrefix + "-" + issue.IDPrefix
	}
	return configPrefix + "-wisp"
}
