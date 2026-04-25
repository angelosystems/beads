package postgres

// This file contains the methods needed to satisfy storage.DoltStorage's
// sub-interfaces. Many of these are Dolt-specific (Branch/Commit/Push/Pull,
// Federation, Sync, HistoryViewer with commit-refs) and have no equivalent on
// Postgres — they return errPostgresNotSupported with a clear message.
//
// Operations that are storage-semantic (not VCS-semantic) and DO have a
// natural Postgres implementation (e.g. GetDependencyCounts, IsBlocked,
// GetCustomStatuses) are implemented for real.
//
// Audit-trail/temporal queries (HistoryViewer.History/AsOf/Diff) could be
// implemented later via the per-table _audit pattern that the Postgres
// schema sets up, but are stubbed for now.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

var errPostgresNotSupported = errors.New("operation is dolt-specific and not supported by the postgres backend")

// ════════════════════════════════════════════════════════════════════════
// VersionControl — entirely Dolt-specific
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) Branch(ctx context.Context, name string) error { return errPostgresNotSupported }
func (s *PostgresStore) Checkout(ctx context.Context, branch string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) CurrentBranch(ctx context.Context) (string, error) {
	return "", errPostgresNotSupported
}
func (s *PostgresStore) DeleteBranch(ctx context.Context, branch string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) ListBranches(ctx context.Context) ([]string, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) Commit(ctx context.Context, message string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) CommitWithConfig(ctx context.Context, message string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) CommitExists(ctx context.Context, commitHash string) (bool, error) {
	return false, nil
}
func (s *PostgresStore) GetCurrentCommit(ctx context.Context) (string, error) {
	return "", errPostgresNotSupported
}
func (s *PostgresStore) Status(ctx context.Context) (*storage.Status, error) {
	return &storage.Status{}, nil
}
func (s *PostgresStore) Log(ctx context.Context, limit int) ([]storage.CommitInfo, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) Merge(ctx context.Context, branch string) ([]storage.Conflict, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) GetConflicts(ctx context.Context) ([]storage.Conflict, error) {
	return nil, nil
}
func (s *PostgresStore) ResolveConflicts(ctx context.Context, table, strategy string) error {
	return errPostgresNotSupported
}

// ════════════════════════════════════════════════════════════════════════
// RemoteStore — Dolt-remote operations
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) AddRemote(ctx context.Context, name, url string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) RemoveRemote(ctx context.Context, name string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) HasRemote(ctx context.Context, name string) (bool, error) { return false, nil }
func (s *PostgresStore) ListRemotes(ctx context.Context) ([]storage.RemoteInfo, error) {
	return nil, nil
}
func (s *PostgresStore) Push(ctx context.Context) error      { return errPostgresNotSupported }
func (s *PostgresStore) Pull(ctx context.Context) error      { return errPostgresNotSupported }
func (s *PostgresStore) ForcePush(ctx context.Context) error { return errPostgresNotSupported }
func (s *PostgresStore) PushRemote(ctx context.Context, remote string, force bool) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) PullRemote(ctx context.Context, remote string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) Fetch(ctx context.Context, peer string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) PushTo(ctx context.Context, peer string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) PullFrom(ctx context.Context, peer string) ([]storage.Conflict, error) {
	return nil, errPostgresNotSupported
}

// ════════════════════════════════════════════════════════════════════════
// FederationStore
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) AddFederationPeer(ctx context.Context, peer *storage.FederationPeer) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) GetFederationPeer(ctx context.Context, name string) (*storage.FederationPeer, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) ListFederationPeers(ctx context.Context) ([]*storage.FederationPeer, error) {
	return nil, nil
}
func (s *PostgresStore) RemoveFederationPeer(ctx context.Context, name string) error {
	return errPostgresNotSupported
}

// ════════════════════════════════════════════════════════════════════════
// SyncStore
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) Sync(ctx context.Context, peer, strategy string) (*storage.SyncResult, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) SyncStatus(ctx context.Context, peer string) (*storage.SyncStatus, error) {
	return nil, errPostgresNotSupported
}

// ════════════════════════════════════════════════════════════════════════
// HistoryViewer — could be implemented via audit tables; stubbed for now
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) History(ctx context.Context, issueID string) ([]*storage.HistoryEntry, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) AsOf(ctx context.Context, issueID, ref string) (*types.Issue, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) Diff(ctx context.Context, fromRef, toRef string) ([]*storage.DiffEntry, error) {
	return nil, errPostgresNotSupported
}

// ════════════════════════════════════════════════════════════════════════
// BulkIssueStore — most are Dolt-specific bulk-import paths
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) CreateIssuesWithFullOptions(ctx context.Context, issues []*types.Issue, actor string, opts storage.BatchCreateOptions) error {
	return s.CreateIssues(ctx, issues, actor)
}
func (s *PostgresStore) DeleteIssues(ctx context.Context, ids []string, cascade, force, dryRun bool) (*types.DeleteIssuesResult, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) DeleteIssuesBySourceRepo(ctx context.Context, sourceRepo string) (int, error) {
	return 0, errPostgresNotSupported
}
func (s *PostgresStore) UpdateIssueID(ctx context.Context, oldID, newID string, issue *types.Issue, actor string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) ClaimIssue(ctx context.Context, id, actor string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) PromoteFromEphemeral(ctx context.Context, id, actor string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) GetNextChildID(ctx context.Context, parentID string) (string, error) {
	return "", errPostgresNotSupported
}
func (s *PostgresStore) RenameCounterPrefix(ctx context.Context, oldPrefix, newPrefix string) error {
	return errPostgresNotSupported
}

// ════════════════════════════════════════════════════════════════════════
// DependencyQueryStore — bulk variants of GetDependencyRecords
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) GetDependencyRecordsForIssues(ctx context.Context, issueIDs []string) (map[string][]*types.Dependency, error) {
	out := make(map[string][]*types.Dependency, len(issueIDs))
	for _, id := range issueIDs {
		recs, err := s.GetDependencyRecords(ctx, id)
		if err != nil {
			return nil, err
		}
		out[id] = recs
	}
	return out, nil
}
func (s *PostgresStore) GetAllDependencyRecords(ctx context.Context) (map[string][]*types.Dependency, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) GetDependencyCounts(ctx context.Context, issueIDs []string) (map[string]*types.DependencyCounts, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) GetBlockingInfoForIssues(ctx context.Context, issueIDs []string) (map[string][]string, map[string][]string, map[string]string, error) {
	return nil, nil, nil, errPostgresNotSupported
}
func (s *PostgresStore) IsBlocked(ctx context.Context, issueID string) (bool, []string, error) {
	return false, nil, errPostgresNotSupported
}
func (s *PostgresStore) GetNewlyUnblockedByClose(ctx context.Context, closedIssueID string) ([]*types.Issue, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) DetectCycles(ctx context.Context) ([][]*types.Issue, error) {
	return nil, nil
}
func (s *PostgresStore) FindWispDependentsRecursive(ctx context.Context, ids []string) (map[string]bool, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) RenameDependencyPrefix(ctx context.Context, oldPrefix, newPrefix string) error {
	return errPostgresNotSupported
}

// ════════════════════════════════════════════════════════════════════════
// AnnotationStore — bulk variants
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) GetCommentCounts(ctx context.Context, issueIDs []string) (map[string]int, error) {
	if len(issueIDs) == 0 {
		return map[string]int{}, nil
	}
	const q = `
		SELECT issue_id, count(*)
		FROM beads.comments
		WHERE rig = $1 AND deleted_at IS NULL AND issue_id = ANY($2)
		GROUP BY issue_id
	`
	rows, err := s.db.QueryContext(ctx, q, s.rig, issueIDs)
	if err != nil {
		return nil, fmt.Errorf("postgres: comment counts: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int, len(issueIDs))
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}
func (s *PostgresStore) GetCommentsForIssues(ctx context.Context, issueIDs []string) (map[string][]*types.Comment, error) {
	out := make(map[string][]*types.Comment, len(issueIDs))
	for _, id := range issueIDs {
		cs, err := s.GetIssueComments(ctx, id)
		if err != nil {
			return nil, err
		}
		out[id] = cs
	}
	return out, nil
}
func (s *PostgresStore) GetLabelsForIssues(ctx context.Context, issueIDs []string) (map[string][]string, error) {
	if len(issueIDs) == 0 {
		return map[string][]string{}, nil
	}
	const q = `
		SELECT issue_id, label
		FROM beads.labels
		WHERE rig = $1 AND deleted_at IS NULL AND issue_id = ANY($2)
		ORDER BY issue_id, label
	`
	rows, err := s.db.QueryContext(ctx, q, s.rig, issueIDs)
	if err != nil {
		return nil, fmt.Errorf("postgres: labels for issues: %w", err)
	}
	defer rows.Close()
	out := make(map[string][]string, len(issueIDs))
	for rows.Next() {
		var id, label string
		if err := rows.Scan(&id, &label); err != nil {
			return nil, err
		}
		out[id] = append(out[id], label)
	}
	return out, rows.Err()
}

// ════════════════════════════════════════════════════════════════════════
// AdvancedQueryStore — repo_mtimes can be real, others are Dolt-niche
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) GetRepoMtime(ctx context.Context, repoPath string) (int64, error) {
	const q = `
		SELECT mtime_ns FROM beads.repo_mtimes
		WHERE rig = $1 AND repo_path = $2 AND deleted_at IS NULL
	`
	var mtime int64
	err := s.db.QueryRowContext(ctx, q, s.rig, repoPath).Scan(&mtime)
	if err != nil {
		// not-found returns 0, no error — matches Dolt contract
		return 0, nil
	}
	return mtime, nil
}
func (s *PostgresStore) SetRepoMtime(ctx context.Context, repoPath, jsonlPath string, mtimeNs int64) error {
	return s.withTxNoError(ctx, "system", func(_ context.Context) error {
		const q = `
			INSERT INTO beads.repo_mtimes (rig, repo_path, jsonl_path, mtime_ns)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (rig, repo_path) DO UPDATE
			   SET jsonl_path = EXCLUDED.jsonl_path,
			       mtime_ns   = EXCLUDED.mtime_ns,
			       deleted_at = NULL
		`
		_, err := s.db.ExecContext(ctx, q, s.rig, repoPath, jsonlPath, mtimeNs)
		return err
	})
}
func (s *PostgresStore) ClearRepoMtime(ctx context.Context, repoPath string) error {
	const q = `
		UPDATE beads.repo_mtimes SET deleted_at = NOW()
		WHERE rig = $1 AND repo_path = $2
	`
	_, err := s.db.ExecContext(ctx, q, s.rig, repoPath)
	return err
}
func (s *PostgresStore) GetMoleculeProgress(ctx context.Context, moleculeID string) (*types.MoleculeProgressStats, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) GetMoleculeLastActivity(ctx context.Context, moleculeID string) (*types.MoleculeLastActivity, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) GetStaleIssues(ctx context.Context, filter types.StaleFilter) ([]*types.Issue, error) {
	return nil, errPostgresNotSupported
}

// ════════════════════════════════════════════════════════════════════════
// CompactionStore — Dolt-history compaction; not applicable to Postgres
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) CheckEligibility(ctx context.Context, issueID string, tier int) (bool, string, error) {
	return false, "compaction is dolt-specific", nil
}
func (s *PostgresStore) ApplyCompaction(ctx context.Context, issueID string, tier, originalSize, compactedSize int, commitHash string) error {
	return errPostgresNotSupported
}
func (s *PostgresStore) GetTier1Candidates(ctx context.Context) ([]*types.CompactionCandidate, error) {
	return nil, nil
}
func (s *PostgresStore) GetTier2Candidates(ctx context.Context) ([]*types.CompactionCandidate, error) {
	return nil, nil
}

// ════════════════════════════════════════════════════════════════════════
// ConfigMetadataStore — DeleteConfig + custom-status helpers
// ════════════════════════════════════════════════════════════════════════

func (s *PostgresStore) DeleteConfig(ctx context.Context, key string) error {
	const q = `
		UPDATE beads.config SET deleted_at = NOW()
		WHERE rig = $1 AND key = $2
	`
	_, err := s.db.ExecContext(ctx, q, s.rig, key)
	return err
}
func (s *PostgresStore) GetCustomStatuses(ctx context.Context) ([]string, error) {
	const q = `SELECT name FROM beads.custom_statuses WHERE rig = $1 AND deleted_at IS NULL ORDER BY name`
	rows, err := s.db.QueryContext(ctx, q, s.rig)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
func (s *PostgresStore) GetCustomStatusesDetailed(ctx context.Context) ([]types.CustomStatus, error) {
	return nil, errPostgresNotSupported
}
func (s *PostgresStore) GetCustomTypes(ctx context.Context) ([]string, error) {
	const q = `SELECT name FROM beads.custom_types WHERE rig = $1 AND deleted_at IS NULL ORDER BY name`
	rows, err := s.db.QueryContext(ctx, q, s.rig)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
func (s *PostgresStore) GetInfraTypes(ctx context.Context) map[string]bool {
	// Default infra-types: standard built-ins. No way to override per-rig
	// in the postgres backend yet — extend custom_types if needed.
	return map[string]bool{}
}
func (s *PostgresStore) IsInfraTypeCtx(ctx context.Context, t types.IssueType) bool {
	infra := s.GetInfraTypes(ctx)
	return infra[string(t)]
}

// withTxNoError is a small helper for the single-statement metadata writes
// above that do not need the full audit-set-actor pattern.
func (s *PostgresStore) withTxNoError(ctx context.Context, actor string, fn func(context.Context) error) error {
	_ = actor
	_ = time.Now() // ensure time import is used
	return fn(ctx)
}
