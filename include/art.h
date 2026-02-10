#ifndef YOWZA_ART_H
#define YOWZA_ART_H

#include "arena.h"

// True Adaptive Radix Tree (ART) implementation.
// Radically improves search performance to O(k) bypassing linear sibling scans
// by introducing adaptive node sizing (Node4, Node16).

typedef enum {
    NODE4 = 1,
    NODE16 = 2,
    NODE48 = 3,
    NODE256 = 4
} NodeType;

typedef struct ARTNode {
    NodeType type;
    uint8_t is_end;
    uint32_t val_len;
    uint64_t timestamp; // For CRDT LWW
    uint8_t* val_ptr;
    /* Phase 1 (MVCC): commit sequence that last mutated this terminal
     * node (0 = never mutated). Set by art_insert / art_delete /
     * art_restore_tombstone; never serialized in snapshots (it is
     * rebuilt from commit order on restore). Tx validation compares
     * this against the transaction's snapshot sequence. */
    uint64_t version;
} ARTNode;

typedef struct ARTNode4 {
    ARTNode base;
    uint8_t num_children;
    uint8_t keys[4];
    ARTNode* children[4];
} ARTNode4;

typedef struct ARTNode16 {
    ARTNode base;
    uint8_t num_children;
    uint8_t keys[16];
    ARTNode* children[16];
} ARTNode16;

typedef struct ARTNode48 {
    ARTNode base;
    uint8_t num_children;
    uint8_t child_index[256];
    ARTNode* children[48];
} ARTNode48;

typedef struct ARTNode256 {
    ARTNode base;
    uint16_t num_children;
    ARTNode* children[256];
} ARTNode256;

/* Step-1 consistency limit: scan/snapshot traversal keeps the current key
 * on the stack, so keys longer than this are rejected fail-fast by
 * art_insert instead of being accepted and then failing to snapshot.
 * 1023 keeps the traversal buffer at a modest 1 KiB. */
#define YOWZA_MAX_KEY_LEN 1023

/* High-bit marker on the u32 ValLen field of a v2 snapshot entry
 * indicating a soft-deleted CRDT tombstone (0 value bytes follow,
 * followed by the 8-byte tombstone timestamp). Real value lengths
 * never set this bit (values are bounded by the arena), so old
 * snapshots (which never set it) read exactly as before, and the
 * v2 header/version ("YWZ6", 1) are unchanged. */
#define YOWZA_TOMBSTONE_FLAG 0x80000000U

typedef struct {
    ARTNode* root;
    Arena* arena;
    /* Highest CRDT timestamp ever inserted into this tree (see
     * art_insert / art_delete). Initialised to 1 by art_init, not 0:
     * a tombstone must carry a timestamp > 0 or the LWW check in
     * art_insert (which requires current->timestamp > 0) stays inactive
     * for it. NOTE: on deserialize the clock is rebuilt from live
     * entries (via art_insert) and from tombstone entries (via
     * art_restore_tombstone), so after a snapshot restore or a compact
     * the clock still reflects deleted keys' timestamps. */
    uint64_t max_timestamp;
    /* Phase 1 (MVCC): monotonically increasing commit sequence.
     * Every committed mutation (legacy put/delete, or one whole
     * transaction) advances it by exactly one for the transaction;
     * terminal nodes are stamped with the sequence of their commit.
     * Reset by art_init together with the tree (versions restart
     * with a restored tree, which is consistent: no pre-restore
     * version can be compared against a post-restore tree). */
    uint64_t commit_seq;
    /* Phase 1: while a transaction is applying its write set,
     * applying != 0 and art_insert/art_delete stamp nodes with
     * apply_seq instead of advancing commit_seq per operation, so
     * one transaction stamps all its keys with one sequence. */
    int applying;
    uint64_t apply_seq;
    /* Per-size free-lists for NODE4, NODE16, NODE48, NODE256
     * (index = type - 1). A retired node is only ever reused for the
     * same type, so sizes always match. art_init clears all four
     * BEFORE creating the root, so a reset arena can never leave a
     * dangling free-list pointer behind. */
    ARTNode* free_list[4];
} ART;

// Initialize the ART structure
void art_init(ART* art, Arena* arena);

// Insert or update a key-value pair in the ART.
// Returns 0 on success, -1 on failure (e.g., out of memory).
int art_insert(ART* art, const uint8_t* key, uint32_t key_len, const uint8_t* val, uint32_t val_len, uint64_t timestamp);

// Search for a key in the ART.
// Returns 0 if found and populates out_val and out_val_len. Returns -1 if not found.
int art_search(ART* art, const uint8_t* key, uint32_t key_len, uint8_t** out_val, uint32_t* out_val_len);

// Phase 1: look up a terminal node's MVCC version without its value.
// Returns 0 and sets *out_version (and *out_found = is_end) on a
// completed walk, even if the key is not live; -1 if the path is absent
// (then *out_found = 0 and *out_version = 0).
int art_lookup_version(ART* art, const uint8_t* key, uint32_t key_len, uint64_t* out_version, int* out_found);

// Phase 1: return the terminal node for a key's full path, or NULL if
// the path is absent. Used by tx undo (restore a terminal header).
// The pointer stays valid while the arena is not reset.
ARTNode* art_find_terminal(ART* art, const uint8_t* key, uint32_t key_len);

// Delete a key from the ART (soft delete for now).
// Returns 0 on success, -1 if key not found.
int art_delete(ART* art, const uint8_t* key, uint32_t key_len);

// Scans all keys matching a prefix and writes them into out_buffer sequentially
// Format for each entry: [KeyLen(4)][Key][ValLen(4)][Val]
// Returns the total number of bytes written to out_buffer, or -1 on error.
int art_scan(ART* art, const uint8_t* prefix, uint32_t prefix_len, uint8_t* out_buffer, uint32_t out_buffer_size);

// Serializes the entire ART index into out_buffer for snapshotting.
// Format v2 (written since step 1):
//   [Magic "YWZ6" (4)][Version u32=1][EntryCount u32]
//   per entry: [KeyLen u32][Key][ValLen u32][Val][Timestamp u64]
//   [Checksum u64: FNV-1a 64 over everything before the checksum]
// A tombstone entry (soft-deleted key, timestamp > 0) is written with
// ValLen = YOWZA_TOMBSTONE_FLAG and 0 value bytes, followed by its
// timestamp; art_deserialize restores it as a tombstone.
// Returns total bytes written, or -1 on error.
int art_serialize(ART* art, uint8_t* out_buffer, uint32_t out_buffer_size);

// Sums the 8-byte-aligned value bytes in a snapshot (plus the root
// node) for the pre-reset fit guard in snapshot restore. Returns 0
// and sets *out_total, or -1 on any inconsistency. See art.c: this
// sum is a necessary, not sufficient, condition for deserialize to
// fit in the arena.
int art_snapshot_value_bytes(const uint8_t* in_buffer, uint32_t in_buffer_size, uint64_t* out_total);

// Deserializes a snapshot buffer back into the ART index.
// Accepts both format v2 above (timestamp restored, checksum verified)
// and the legacy format (sequential [KeyLen][Key][ValLen][Val], timestamp 0).
// Returns 0 on success, -1 on error. Does NOT reset the ART itself:
// callers must validate, then reset, then deserialize (see
// yowza_db_snapshot_restore / yowza_snapshot_restore).
int art_deserialize(ART* art, const uint8_t* in_buffer, uint32_t in_buffer_size);

// Validates a snapshot buffer without mutating the ART: parses it fully,
// and for v2 also verifies version, entry count and the FNV-1a checksum.
// Returns 0 if the buffer is a clean snapshot, -1 otherwise.
int art_validate_snapshot(const uint8_t* in_buffer, uint32_t in_buffer_size);

// Phase 2, Track B: sorted iterator over every terminal (live AND
// tombstone). The callback receives the key, the value (NULL/0 for a
// tombstone), is_tombstone, the CRDT timestamp, and the MVCC version.
// Traversal is in lexicographic key order (NODE4/NODE16 children are
// visited sorted by key byte, not in insertion order; NODE48/NODE256
// are already byte-ordered). A node that is both a terminal and a
// parent is visited before its children (a key sorts before its
// extensions). A non-terminal node with timestamp > 0 and a non-empty
// key is a tombstone (same rule as art_serialize); the root
// (empty key) is never visited. If the callback returns non-zero,
// iteration stops immediately. Returns 0 if the whole tree was visited,
// 1 if stopped early by the callback, -1 on a bad argument. This
// function does not change insert/delete/scan/serialize semantics.
typedef int (*art_iter_cb)(void* ctx, const uint8_t* key, uint32_t key_len,
                           const uint8_t* val, uint32_t val_len,
                           int is_tombstone, uint64_t timestamp,
                           uint64_t version);
int art_iterate(ART* art, art_iter_cb cb, void* ctx);

#endif // YOWZA_ART_H
