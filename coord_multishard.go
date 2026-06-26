package yowza

// Axis B — MULTI-SHARD ROOT (one shared commit decision).
//
// A multi-shard operation is prepared per shard and
// published by ONE root record. There is no per-shard
// publish: either the root exists (committed — recovery
// must complete the operation on every shard) or it
// does not (not committed — recovery reclaims the
// staged data and applies nothing). A crash anywhere
// therefore resolves to all-old or all-new, never to a
// half-published state.
//
// Files, all in the coordinator directory:
//
//	coord-stage-<opID>-<shard>.json   one staged payload per shard (JSON + CRC-32)
//	coord-stage-<opID>.manifest.json  the staged shard list (JSON + CRC-32), written last
//	coord-root-<opID>.json            the commit decision (JSON + CRC-32, tmp + sync + rename + dir sync)
//
// Order inside Prepare: shard files first, manifest
// last. A manifest naming a shard implies that shard
// file is durable (it was synced before the manifest
// publish). PublishRoot validates the manifest and
// EVERY shard payload's CRC before writing the root,
// so a committed operation whose staged data is corrupt
// fails loudly with ErrCoordCorrupt and publishes
// nothing — the caller must not apply a prefix of it.
//
// Recovery (RecoverMultiShard) is validation-first:
//   - root present + manifest + all shard payloads
//     valid -> the operation is returned as committed
//     (the caller applies it to every shard, then
//     calls MarkDone). Nothing is applied by recovery
//     itself for DB-backed callers: recovery returns
//     validated payloads and the caller applies them
//     idempotently (puts/deletes), so a second
//     recovery after MarkDone is a no-op, and a
//     recovery before MarkDone re-applies the same
//     values harmlessly.
//   - staged manifest without a root -> staged files
//     deleted (reclaimed), nothing returned.
//   - root without a manifest, or any CRC failure on
//     a committed operation -> ErrCoordCorrupt, and
//     NO operation is applied or reclaimed in that
//     call (the caller must not proceed past a corrupt
//     committed operation to a half state).
//   - orphan shard files (no manifest, no root — a
//     crash mid-prepare) -> deleted.
//
// Shard IDs and operation IDs are plain names (no path
// separators, not "." or ".."): a staged file name is
// built from them, so a hostile ID must never escape
// the coordinator directory.

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// CoordMultiShardOp is one committed multi-shard
// operation returned by recovery: the operation ID and
// its per-shard write sets, validated. Deltas reuse the
// global-merge delta shape (key, deleted, value) so a
// domain caller can apply them with applyDeltasLocked.
//
// Item 1 extension: Kind, DBDir, MergeID and Fingerprint
// annotate decisions made through the CoordDecider
// layer (coord_decision.go). They are empty for legacy
// domain decisions (which predate the annotation):
// callers that filter by kind must treat empty-kind
// ops as domain ops, never as Tx/merge ops.
type CoordMultiShardOp struct {
	OpID   string
	Writes map[string][]globalMergeDelta // shard/DB ID -> deltas
	// Kind is CoordDecisionTx / CoordDecisionMerge for
	// decider operations, "" for legacy domain ops.
	Kind CoordDecisionKind
	// DBDir is the single DB directory a Tx/merge
	// decision applies to ("" for legacy domain ops).
	DBDir string
	// MergeID is the global-merge staging ID for merge
	// decisions ("" for Tx and legacy ops).
	MergeID string
	// Fingerprint is the SHA-256 hex of the canonical
	// write-set JSON (see coordDecisionFingerprint).
	Fingerprint string
}

type coordShardStage struct {
	OpID     string             `json:"op_id"`
	Shard    string             `json:"shard"`
	Deltas   []globalMergeDelta `json:"deltas"`
	Checksum uint32             `json:"checksum"`
}

type coordStageManifest struct {
	OpID     string   `json:"op_id"`
	Shards   []string `json:"shards"`
	Checksum uint32   `json:"checksum"`
}

type coordRoot struct {
	OpID     string   `json:"op_id"`
	OwnerID  string   `json:"owner_id"`
	Epoch    uint64   `json:"epoch"`
	Shards   []string `json:"shards"`
	Checksum uint32   `json:"checksum"`
}

func coordShardStageChecksum(s *coordShardStage) uint32 {
	body, _ := json.Marshal(struct {
		OpID   string             `json:"op_id"`
		Shard  string             `json:"shard"`
		Deltas []globalMergeDelta `json:"deltas"`
	}{OpID: s.OpID, Shard: s.Shard, Deltas: s.Deltas})
	return crc32.ChecksumIEEE(body)
}

func coordManifestChecksum(m *coordStageManifest) uint32 {
	body, _ := json.Marshal(struct {
		OpID   string   `json:"op_id"`
		Shards []string `json:"shards"`
	}{OpID: m.OpID, Shards: m.Shards})
	return crc32.ChecksumIEEE(body)
}

func coordRootChecksum(r *coordRoot) uint32 {
	body, _ := json.Marshal(struct {
		OpID    string   `json:"op_id"`
		OwnerID string   `json:"owner_id"`
		Epoch   uint64   `json:"epoch"`
		Shards  []string `json:"shards"`
	}{OpID: r.OpID, OwnerID: r.OwnerID, Epoch: r.Epoch, Shards: r.Shards})
	return crc32.ChecksumIEEE(body)
}

func coordValidName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.Contains(name, "/") && !strings.Contains(name, "\\")
}

func coordStageShardPath(dir, opID, shard string) string {
	return filepath.Join(dir, "coord-stage-"+opID+"-"+shard+".json")
}
func coordStageManifestPath(dir, opID string) string {
	return filepath.Join(dir, "coord-stage-"+opID+".manifest.json")
}
func coordRootPath(dir, opID string) string {
	return filepath.Join(dir, "coord-root-"+opID+".json")
}

func coordDeleteMultiShardFiles(dir, opID string, shards []string) {
	_ = os.Remove(coordStageManifestPath(dir, opID))
	_ = os.Remove(coordRootPath(dir, opID))
	for _, sh := range shards {
		_ = os.Remove(coordStageShardPath(dir, opID, sh))
	}
	_ = os.Remove(coordStageManifestPath(dir, opID) + ".tmp")
	_ = os.Remove(coordRootPath(dir, opID) + ".tmp")
}

func coordLoadManifest(dir, opID string) (*coordStageManifest, error) {
	data, err := os.ReadFile(coordStageManifestPath(dir, opID))
	if err != nil {
		return nil, err
	}
	var m coordStageManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: manifest for op %s unparseable: %v", ErrCoordCorrupt, opID, err)
	}
	if m.OpID != opID || coordManifestChecksum(&m) != m.Checksum {
		return nil, fmt.Errorf("%w: manifest for op %s invalid", ErrCoordCorrupt, opID)
	}
	return &m, nil
}

func coordLoadShard(dir, opID, shard string) ([]globalMergeDelta, error) {
	data, err := os.ReadFile(coordStageShardPath(dir, opID, shard))
	if err != nil {
		return nil, err
	}
	var s coordShardStage
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("%w: staged shard %s for op %s unparseable: %v", ErrCoordCorrupt, shard, opID, err)
	}
	if s.OpID != opID || s.Shard != shard || coordShardStageChecksum(&s) != s.Checksum {
		return nil, fmt.Errorf("%w: staged shard %s for op %s invalid", ErrCoordCorrupt, shard, opID)
	}
	return s.Deltas, nil
}

// PrepareMultiShard stages one payload per shard plus a
// manifest. It requires a live session and refuses an
// opID that already has staged files or a root. On any
// staging failure it best-effort cleans the staged
// files for this opID and returns the error: a failed
// prepare leaves no decision and no visible data.
func PrepareMultiShard(sess *CoordinatorSession, dir, opID string, writes map[string][]globalMergeDelta) error {
	if sess == nil {
		return fmt.Errorf("%w: nil session", ErrCoordReleased)
	}
	if !coordValidName(opID) {
		return fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	if len(writes) == 0 {
		return fmt.Errorf("yowza coordinator: empty multi-shard operation %s", opID)
	}
	shards := make([]string, 0, len(writes))
	for sh := range writes {
		if !coordValidName(sh) {
			return fmt.Errorf("yowza coordinator: invalid shard id %q", sh)
		}
		shards = append(shards, sh)
	}
	sort.Strings(shards)
	if err := sess.Validate(); err != nil {
		return err
	}
	if _, err := os.Stat(coordRootPath(dir, opID)); err == nil {
		return fmt.Errorf("%w: op %s already has a root", ErrCoordOpExists, opID)
	}
	if _, err := os.Stat(coordStageManifestPath(dir, opID)); err == nil {
		return fmt.Errorf("%w: op %s already staged", ErrCoordOpExists, opID)
	}
	for _, sh := range shards {
		st := coordShardStage{OpID: opID, Shard: sh, Deltas: writes[sh]}
		st.Checksum = coordShardStageChecksum(&st)
		data, err := json.Marshal(st)
		if err != nil {
			coordDeleteMultiShardFiles(dir, opID, shards)
			return err
		}
		if err := coordWriteAtomic(dir, coordStageShardPath(dir, opID, sh), data); err != nil {
			coordDeleteMultiShardFiles(dir, opID, shards)
			return err
		}
	}
	man := coordStageManifest{OpID: opID, Shards: shards}
	man.Checksum = coordManifestChecksum(&man)
	data, err := json.Marshal(man)
	if err != nil {
		coordDeleteMultiShardFiles(dir, opID, shards)
		return err
	}
	if err := coordWriteAtomic(dir, coordStageManifestPath(dir, opID), data); err != nil {
		coordDeleteMultiShardFiles(dir, opID, shards)
		return err
	}
	return nil
}

// PublishRoot publishes the single commit decision for a
// staged operation. It re-validates fencing and
// re-validates every staged payload before publishing:
// a stale session publishes nothing, and a corrupt
// staged operation publishes nothing. The root publish
// itself is tmp + sync + rename + dir sync, so the
// decision appears atomically to recovery.
func PublishRoot(sess *CoordinatorSession, dir, opID string) error {
	if sess == nil {
		return fmt.Errorf("%w: nil session", ErrCoordReleased)
	}
	if !coordValidName(opID) {
		return fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if err := sess.validateLocked(); err != nil {
		return err
	}
	if _, err := os.Stat(coordRootPath(dir, opID)); err == nil {
		return fmt.Errorf("%w: op %s already has a root", ErrCoordOpExists, opID)
	}
	man, err := coordLoadManifest(dir, opID)
	if err != nil {
		return err
	}
	for _, sh := range man.Shards {
		if _, err := coordLoadShard(dir, opID, sh); err != nil {
			return err
		}
	}
	root := coordRoot{OpID: opID, OwnerID: sess.ownerID, Epoch: sess.epoch, Shards: man.Shards}
	root.Checksum = coordRootChecksum(&root)
	data, err := json.Marshal(root)
	if err != nil {
		return err
	}
	return coordWriteAtomic(dir, coordRootPath(dir, opID), data)
}

// AbortMultiShard reclaims a staged-but-uncommitted
// operation. It refuses to abort a committed operation:
// a root means the decision is made and only recovery
// may complete it.
func AbortMultiShard(dir, opID string) error {
	if !coordValidName(opID) {
		return fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	if _, err := os.Stat(coordRootPath(dir, opID)); err == nil {
		return fmt.Errorf("%w: op %s", ErrCoordOpCommitted, opID)
	}
	man, err := coordLoadManifest(dir, opID)
	var shards []string
	if err == nil {
		shards = man.Shards
	}
	coordDeleteMultiShardFiles(dir, opID, shards)
	// Also sweep any shard file for this op even if the
	// manifest is missing (crash mid-prepare).
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	prefix := "coord-stage-" + opID + "-"
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), ".json") {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	return nil
}

// MarkDone deletes a committed operation's root and
// staged files after its payloads have been applied
// everywhere. It is best-effort per file but reports a
// failure if any existing file could not be removed:
// the caller then knows recovery may re-apply the
// operation (idempotently) instead of assuming it is
// cleaned up.
func MarkDone(dir, opID string) error {
	if !coordValidName(opID) {
		return fmt.Errorf("yowza coordinator: invalid operation id %q", opID)
	}
	man, _ := coordLoadManifest(dir, opID)
	var shards []string
	if man != nil {
		shards = man.Shards
	} else {
		// Manifest already gone: recover shard names
		// from the root, if present.
		if data, err := os.ReadFile(coordRootPath(dir, opID)); err == nil {
			var r coordRoot
			if json.Unmarshal(data, &r) == nil {
				shards = r.Shards
			}
		}
	}
	var firstErr error
	for _, p := range []string{coordRootPath(dir, opID), coordStageManifestPath(dir, opID)} {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	for _, sh := range shards {
		if err := os.Remove(coordStageShardPath(dir, opID, sh)); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// RecoverMultiShard scans the coordinator directory and
// returns every committed operation (root present, all
// staged payloads validated), sorted by opID, while
// reclaiming staged-but-uncommitted operations and
// orphan shard files. A corrupt committed operation
// fails the whole recovery loudly and applies nothing:
// the caller must not proceed to a half state. The
// returned operations are NOT cleaned up here — the
// caller applies them, then calls MarkDone.
func RecoverMultiShard(dir string) ([]CoordMultiShardOp, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	roots := map[string]bool{}
	manifests := map[string]bool{}
	orphanShards := []string{}
	for _, e := range ents {
		name := e.Name()
		switch {
		case strings.HasPrefix(name, "coord-root-") && strings.HasSuffix(name, ".json"):
			roots[strings.TrimSuffix(strings.TrimPrefix(name, "coord-root-"), ".json")] = true
		case strings.HasPrefix(name, "coord-stage-") && strings.HasSuffix(name, ".manifest.json"):
			manifests[strings.TrimSuffix(strings.TrimPrefix(name, "coord-stage-"), ".manifest.json")] = true
		case strings.HasPrefix(name, "coord-stage-") && strings.HasSuffix(name, ".json"):
			orphanShards = append(orphanShards, name)
		}
	}
	var committed []CoordMultiShardOp
	for opID := range roots {
		data, err := os.ReadFile(coordRootPath(dir, opID))
		if err != nil {
			return nil, err
		}
		var r coordRoot
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("%w: root for op %s unparseable: %v", ErrCoordCorrupt, opID, err)
		}
		if r.OpID != opID || coordRootChecksum(&r) != r.Checksum {
			return nil, fmt.Errorf("%w: root for op %s invalid", ErrCoordCorrupt, opID)
		}
		man, err := coordLoadManifest(dir, opID)
		if err != nil {
			return nil, err
		}
		writes := map[string][]globalMergeDelta{}
		for _, sh := range man.Shards {
			deltas, err := coordLoadShard(dir, opID, sh)
			if err != nil {
				return nil, err
			}
			writes[sh] = deltas
		}
		// Item 1: annotate decider decisions from
		// their annotation file (absent = legacy /
		// domain decision, Kind stays empty). A
		// corrupt annotation fails the whole recovery
		// loudly, like any other corrupt committed
		// state — the caller must not proceed past
		// it to a half state. The fingerprint check
		// proves the annotation describes exactly
		// this staged payload, not merely this opID.
		if meta, merr := coordDecisionReadMeta(dir, opID); merr != nil {
			return nil, merr
		} else if meta != nil {
			if coordDecisionFingerprint(writes) != meta.Fingerprint {
				return nil, fmt.Errorf("%w: committed op %s fingerprint differs from its annotation", ErrCoordCorrupt, opID)
			}
			committed = append(committed, CoordMultiShardOp{OpID: opID, Writes: writes, Kind: meta.Kind, DBDir: meta.DBDir, MergeID: meta.MergeID, Fingerprint: meta.Fingerprint})
		} else {
			committed = append(committed, CoordMultiShardOp{OpID: opID, Writes: writes})
		}
	}
	sort.Slice(committed, func(i, j int) bool { return committed[i].OpID < committed[j].OpID })
	// Reclaim staged-only operations (manifest, no root).
	for opID := range manifests {
		if roots[opID] {
			continue
		}
		man, err := coordLoadManifest(dir, opID)
		var shards []string
		if err == nil {
			shards = man.Shards
		}
		coordDeleteMultiShardFiles(dir, opID, shards)
	}
	// Reclaim orphan shard files left by a crash
	// mid-prepare (no manifest, no root). Files that
	// belong to a committed operation were already
	// validated above and must NOT be deleted here:
	// an orphan name belongs to no manifest and no
	// root, which is exactly the mid-prepare shape.
	committedOps := map[string]bool{}
	for _, op := range committed {
		committedOps[op.OpID] = true
	}
	manifestOps := map[string]bool{}
	for opID := range manifests {
		manifestOps[opID] = true
	}
	for _, name := range orphanShards {
		rest := strings.TrimSuffix(strings.TrimPrefix(name, "coord-stage-"), ".json")
		// rest is "<opID>-<shard>"; the opID is the part
		// before the last "-<shard>" only if a manifest
		// or root names it — orphan files by definition
		// have neither, so delete any coord-stage shard
		// file whose op is neither committed nor
		// manifested. To avoid deleting a committed
		// shard file whose name parsing is ambiguous,
		// skip files whose name starts with a committed
		// or manifested opID prefix.
		skip := false
		for opID := range committedOps {
			if strings.HasPrefix(rest, opID+"-") {
				skip = true
				break
			}
		}
		if !skip {
			for opID := range manifestOps {
				if strings.HasPrefix(rest, opID+"-") {
					skip = true
					break
				}
			}
		}
		if !skip {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
	return committed, nil
}
