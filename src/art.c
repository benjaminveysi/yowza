#include "art.h"

/* Per-size node free-list (see ART.free_list in art.h). A retired node
 * is only ever reused for the same NodeType, so its size always
 * matches. The next pointer is stored in the retired node's val_ptr:
 * that field is dead for a retired node (a live node's value pointer
 * is copied to the replacement node by add_child BEFORE the old node
 * is pushed, and a tombstone's val_ptr is already NULL), and every
 * create_node* overwrites val_ptr with NULL before returning a reused
 * node. Callers must push only nodes no longer referenced by the tree
 * (see add_child). */
static void free_node_push(ART* art, ARTNode* node) {
    if (!art || !node || node->type < NODE4 || node->type > NODE256) return;
    int idx = (int)node->type - 1;
    /* Store the next pointer inside val_ptr of the retired node header. */
    node->val_ptr = (uint8_t*)art->free_list[idx];
    art->free_list[idx] = node;
}

static ARTNode* free_node_pop(ART* art, NodeType type) {
    if (!art || type < NODE4 || type > NODE256) return NULL;
    int idx = (int)type - 1;
    ARTNode* node = art->free_list[idx];
    if (node) {
        art->free_list[idx] = (ARTNode*)node->val_ptr;
    }
    return node;
}

static ARTNode4* create_node4(ART* art) {
    ARTNode4* node = (ARTNode4*)free_node_pop(art, NODE4);
    if (!node) node = (ARTNode4*)arena_alloc(art->arena, sizeof(ARTNode4));
    if (node) {
        node->base.type = NODE4;
        node->base.is_end = 0;
        node->base.val_len = 0;
        node->base.val_ptr = NULL;
        /* Step 1: arena memory is NOT zeroed, so every field must be
         * initialized explicitly. An uninitialized timestamp used to
         * survive here and corrupt CRDT LWW decisions. */
        node->base.timestamp = 0;
        node->base.version = 0;
        node->num_children = 0;
        for (int i = 0; i < 4; i++) { node->keys[i] = 0; node->children[i] = NULL; }
    }
    return node;
}

static ARTNode48* create_node48(ART* art) {
    ARTNode48* node = (ARTNode48*)free_node_pop(art, NODE48);
    if (!node) node = (ARTNode48*)arena_alloc(art->arena, sizeof(ARTNode48));
    if (node) {
        node->base.type = NODE48;
        node->base.is_end = 0;
        node->base.val_len = 0;
        node->base.val_ptr = NULL;
        node->base.timestamp = 0;
        node->base.version = 0;
        node->num_children = 0;
        for (int i=0; i<256; i++) node->child_index[i] = 0;
        for (int i=0; i<48; i++) node->children[i] = NULL;
    }
    return node;
}

static ARTNode256* create_node256(ART* art) {
    ARTNode256* node = (ARTNode256*)free_node_pop(art, NODE256);
    if (!node) node = (ARTNode256*)arena_alloc(art->arena, sizeof(ARTNode256));
    if (node) {
        node->base.type = NODE256;
        node->base.is_end = 0;
        node->base.val_len = 0;
        node->base.val_ptr = NULL;
        node->base.timestamp = 0;
        node->base.version = 0;
        node->num_children = 0;
        for (int i=0; i<256; i++) node->children[i] = NULL;
    }
    return node;
}

static ARTNode16* create_node16(ART* art) {
    ARTNode16* node = (ARTNode16*)free_node_pop(art, NODE16);
    if (!node) node = (ARTNode16*)arena_alloc(art->arena, sizeof(ARTNode16));
    if (node) {
        node->base.type = NODE16;
        node->base.is_end = 0;
        node->base.val_len = 0;
        node->base.val_ptr = NULL;
        node->base.timestamp = 0;
        node->base.version = 0;
        node->num_children = 0;
        for (int i = 0; i < 16; i++) { node->keys[i] = 0; node->children[i] = NULL; }
    }
    return node;
}

void art_init(ART* art, Arena* arena) {
    if (!art || !arena) return;
    art->arena = arena;
    /* Clear all four free-lists BEFORE creating the root: a reset
     * arena (snapshot restore / compact) must never leave a free-list
     * pointing at reclaimed memory, and create_node4 must not pop a
     * stale node for the new root. */
    for (int i = 0; i < 4; i++) art->free_list[i] = NULL;
    /* 1, not 0: a tombstone created by art_delete must carry a
     * timestamp > 0, otherwise the LWW check in art_insert (which
     * requires current->timestamp > 0) never activates for it. After
     * a deserialize, the clock is rebuilt from live entries (via
     * art_insert) and tombstone entries (via art_restore_tombstone). */
    art->max_timestamp = 1;
    art->commit_seq = 0;
    art->applying = 0;
    art->apply_seq = 0;
    art->root = (ARTNode*)create_node4(art);
}

// Memory copy helper since we have no stdlib in Wasm
static void memory_copy(uint8_t* dst, const uint8_t* src, uint32_t len) {
    for (uint32_t i = 0; i < len; i++) {
        dst[i] = src[i];
    }
}

// Finds the child pointer in any Node
static ARTNode** find_child(ARTNode* node, uint8_t c) {
    if (!node) return NULL;

    switch (node->type) {
        case NODE4: {
            ARTNode4* n = (ARTNode4*)node;
            for (uint8_t i = 0; i < n->num_children; i++) {
                if (n->keys[i] == c) return &n->children[i];
            }
            break;
        }
        case NODE16: {
            ARTNode16* n = (ARTNode16*)node;
            // In real ART we'd use SSE/SIMD here. Since Wasm doesn't expose it by default,
            // we use a standard linear/binary search.
            for (uint8_t i = 0; i < n->num_children; i++) {
                if (n->keys[i] == c) return &n->children[i];
            }
            break;
        }
        case NODE48: {
            ARTNode48* n = (ARTNode48*)node;
            uint8_t idx = n->child_index[c];
            if (idx != 0) {
                return &n->children[idx - 1]; // 1-based to allow 0 for empty
            }
            break;
        }
        case NODE256: {
            ARTNode256* n = (ARTNode256*)node;
            if (n->children[c]) {
                return &n->children[c];
            }
            break;
        }
    }
    return NULL;
}

// Adds a child. Might return a new node if node expanded.
static ARTNode* add_child(ART* art, ARTNode* node, uint8_t c, ARTNode* child) {
    if (node->type == NODE4) {
        ARTNode4* n = (ARTNode4*)node;
        if (n->num_children < 4) {
            n->keys[n->num_children] = c;
            n->children[n->num_children] = child;
            n->num_children++;
            return node;
        } else {
            // Expand to Node16
            ARTNode16* n16 = create_node16(art);
            if (!n16) return NULL; // OOM: caller rolls back its new child
            n16->base.is_end = n->base.is_end;
            n16->base.val_ptr = n->base.val_ptr;
            n16->base.val_len = n->base.val_len;
            n16->base.timestamp = n->base.timestamp;
            n16->base.version = n->base.version;
            n16->num_children = 4;
            for (int i=0; i<4; i++) {
                n16->keys[i] = n->keys[i];
                n16->children[i] = n->children[i];
            }
            n16->keys[4] = c;
            n16->children[4] = child;
            n16->num_children++;
            /* Upgrade copy complete: the caller replaces its pointer
             * with n16, so the old Node4 is unreferenced from here on
             * and safe to recycle. Push ONLY here, never on the OOM
             * path above (create_node16 == NULL). */
            free_node_push(art, node);
            return (ARTNode*)n16;
        }
    } else if (node->type == NODE16) {
        ARTNode16* n = (ARTNode16*)node;
        if (n->num_children < 16) {
            n->keys[n->num_children] = c;
            n->children[n->num_children] = child;
            n->num_children++;
            return node;
        } else {
            // Expand to Node48
            ARTNode48* n48 = create_node48(art);
            if (!n48) return NULL; // OOM: caller rolls back its new child
            n48->base.is_end = n->base.is_end;
            n48->base.val_ptr = n->base.val_ptr;
            n48->base.val_len = n->base.val_len;
            n48->base.timestamp = n->base.timestamp;
            n48->base.version = n->base.version;
            n48->num_children = 16;
            for (int i=0; i<16; i++) {
                n48->child_index[n->keys[i]] = i + 1; // 1-based
                n48->children[i] = n->children[i];
            }
            n48->child_index[c] = 17;
            n48->children[16] = child;
            n48->num_children++;
            /* See Node4->Node16 above: copy complete, old Node16
             * unreferenced, safe to recycle. */
            free_node_push(art, node);
            return (ARTNode*)n48;
        }
    } else if (node->type == NODE48) {
        ARTNode48* n = (ARTNode48*)node;
        if (n->num_children < 48) {
            uint8_t target_idx = n->num_children;
            n->child_index[c] = target_idx + 1;
            n->children[target_idx] = child;
            n->num_children++;
            return node;
        } else {
            // Expand to Node256
            ARTNode256* n256 = create_node256(art);
            if (!n256) return NULL; // OOM: caller rolls back its new child
            n256->base.is_end = n->base.is_end;
            n256->base.val_ptr = n->base.val_ptr;
            n256->base.val_len = n->base.val_len;
            n256->base.timestamp = n->base.timestamp;
            n256->base.version = n->base.version;

            for (int i=0; i<256; i++) {
                if (n->child_index[i] != 0) {
                    n256->children[i] = n->children[n->child_index[i] - 1];
                    n256->num_children++;
                }
            }
            n256->children[c] = child;
            n256->num_children++;
            /* See Node4->Node16 above: copy complete, old Node48
             * unreferenced, safe to recycle. */
            free_node_push(art, node);
            return (ARTNode*)n256;
        }
    } else if (node->type == NODE256) {
        ARTNode256* n = (ARTNode256*)node;
        if (!n->children[c]) {
            n->children[c] = child;
            n->num_children++;
        }
        return node;
    }

    return node;
}

int art_insert(ART* art, const uint8_t* key, uint32_t key_len, const uint8_t* val, uint32_t val_len, uint64_t timestamp) {
    if (!art || !art->root || !key || key_len == 0) return -1;
    if (key_len > YOWZA_MAX_KEY_LEN) return -1; // Fail fast, see art.h
    if (val_len > 0 && !val) return -1;

    ARTNode** current_ref = &art->root;

    for (uint32_t i = 0; i < key_len; i++) {
        uint8_t c = key[i];

        ARTNode** child_ref = find_child(*current_ref, c);
        if (child_ref == NULL) {
            // Add a new path. The arena is a bump allocator, so on
            // failure we can roll the offset back to exactly before
            // this iteration's new node and leak nothing.
            size_t checkpoint = art->arena->offset;
            ARTNode* new_node = (ARTNode*)create_node4(art);
            if (!new_node) return -1; // OOM

            ARTNode* expanded = add_child(art, *current_ref, c, new_node);
            if (!expanded) {
                art->arena->offset = checkpoint;
                return -1; // OOM during node promotion
            }
            if (expanded != *current_ref) {
                // Replace pointer if node was expanded
                *current_ref = expanded;
            }
            current_ref = find_child(*current_ref, c);
            if (!current_ref) {
                art->arena->offset = checkpoint;
                return -1; // Should not happen
            }
        } else {
            current_ref = child_ref;
        }
    }


    ARTNode* current = *current_ref;

    /* Track the highest CRDT clock ever seen. A plain PUT carries
     * timestamp 0 and must not pollute the clock (0 is never > the
     * initial value 1), and a stale write cannot lower it: if the
     * write is ignored below as stale, its timestamp is below the
     * node's timestamp, which is itself <= max_timestamp, so this
     * update is a no-op for it. Updating here (before the LWW check)
     * also rebuilds the clock during art_deserialize, which replays
     * entries through art_insert. */
    if (timestamp > art->max_timestamp) {
        art->max_timestamp = timestamp;
    }

    // CRDT LWW logic, independent of is_end: a soft-deleted node keeps
    // its timestamp as a tombstone (see art_delete). A stale CRDT write
    // must not resurrect it, and must not resurrect a live value either.
    // timestamp == 0 means a plain PUT (no CRDT clock) and always wins.
    // For a tombstone (!current->is_end), an incoming write at the SAME
    // timestamp also loses: only a strictly newer write may revive the
    // key. This closes the degenerate timestamp == 1 tie case (plain
    // PUT + DELETE leaves a tombstone at 1, so a CRDT write at ts=1
    // must lose and ts=2 must win) and duplicate pre-delete packets
    // (a tombstone at ts=100 must not lose to a replayed ts=100).
    // For a live node the old strict rule stays: only a strictly
    // older write loses, an equal-timestamp write overwrites.
    if (timestamp > 0 && current->timestamp > 0) {
        if (!current->is_end ? (timestamp <= current->timestamp) : (timestamp < current->timestamp)) {
            // Incoming timestamp is older (or tied against a tombstone), ignore update
            return 0;
        }
    }

    // Copy the value into the arena. Note: if this OOMs, the (possibly
    // new) intermediate nodes on the path stay in the tree with
    // is_end == 0. They are harmless, reusable by later inserts, and
    // invisible to search/scan/snapshot.
    uint8_t* new_val_ptr = (uint8_t*)arena_alloc(art->arena, val_len);
    if (!new_val_ptr) return -1; // OOM
    memory_copy(new_val_ptr, val, val_len);

    current->is_end = 1;
    current->val_ptr = new_val_ptr;
    current->val_len = val_len;
    current->timestamp = timestamp;
    /* Phase 1 MVCC stamp: one sequence per committed transaction.
     * During tx apply, all keys share apply_seq (commit_seq was
     * advanced once for the whole transaction); a legacy put is its
     * own transaction and advances commit_seq by one. Stale writes
     * returned above without reaching here, so no sequence is spent. */
    current->version = art->applying ? art->apply_seq : ++art->commit_seq;

    return 0;
}

ARTNode* art_find_terminal(ART* art, const uint8_t* key, uint32_t key_len) {
    if (!art || !art->root || !key || key_len == 0) return NULL;
    ARTNode* current = art->root;
    for (uint32_t i = 0; i < key_len; i++) {
        ARTNode** child_ref = find_child(current, key[i]);
        if (child_ref == NULL) return NULL;
        current = *child_ref;
    }
    return current;
}

int art_lookup_version(ART* art, const uint8_t* key, uint32_t key_len, uint64_t* out_version, int* out_found) {
    if (!art || !art->root || !key || key_len == 0 || !out_version || !out_found) return -1;
    ARTNode* current = art->root;
    for (uint32_t i = 0; i < key_len; i++) {
        ARTNode** child_ref = find_child(current, key[i]);
        if (child_ref == NULL) {
            *out_version = 0;
            *out_found = 0;
            return 0; /* path absent: version 0, not found */
        }
        current = *child_ref;
    }
    *out_version = current->version;
    *out_found = current->is_end ? 1 : 0;
    return 0;
}

int art_search(ART* art, const uint8_t* key, uint32_t key_len, uint8_t** out_val, uint32_t* out_val_len) {
    if (!art || !art->root || !key || key_len == 0) return -1;

    ARTNode* current = art->root;

    for (uint32_t i = 0; i < key_len; i++) {
        uint8_t c = key[i];

        ARTNode** child_ref = find_child(current, c);
        if (child_ref == NULL) return -1; // Not found

        current = *child_ref;
    }

    if (current->is_end) {
        if (out_val) *out_val = current->val_ptr;
        if (out_val_len) *out_val_len = current->val_len;
        return 0;
    }

    return -1;
}

int art_delete(ART* art, const uint8_t* key, uint32_t key_len) {
    if (!art || !art->root || !key || key_len == 0) return -1;

    ARTNode* current = art->root;

    for (uint32_t i = 0; i < key_len; i++) {
        uint8_t c = key[i];
        ARTNode** child_ref = find_child(current, c);
        if (child_ref == NULL) return -1; // Not found
        current = *child_ref;
    }

    if (current->is_end) {
        // Soft delete the data
        // Hard deletion logic requiring node shifting in Node4/Node16 is omitted
        // to simplify this demo because the `yowza_db_compact` function solves
        // node leaking across the board by creating a new clean compacted tree.
        // Deliberately keep current->timestamp: it is the tombstone that
        // stops a stale CRDT write (timestamp < tombstone) from resurrecting
        // this key. See the LWW check in art_insert.
        // Upgrade the tombstone to the highest clock ever seen: a key
        // first written with a plain PUT (timestamp 0) would otherwise
        // keep timestamp 0 after deletion, and the LWW check (which
        // requires current->timestamp > 0) would let ANY later CRDT
        // write resurrect it. With the upgrade, writes at or below
        // max_timestamp lose against the tombstone (see the LWW check
        // in art_insert: tombstone ties lose, only a strictly newer
        // write revives the key). Tombstones are now also serialised
        // (YOWZA_TOMBSTONE_FLAG, see serialize_recursive), so this
        // protection survives a snapshot restore and a compact.
        if (current->timestamp < art->max_timestamp) {
            current->timestamp = art->max_timestamp;
        }
        current->is_end = 0;
        current->val_ptr = NULL;
        current->val_len = 0;
        current->version = art->applying ? art->apply_seq : ++art->commit_seq;
        return 0;
    }

    return -1;
}

// Helper to write u32 to buffer in little-endian format
static void write_u32(uint8_t* buf, uint32_t val) {
    buf[0] = val & 0xFF;
    buf[1] = (val >> 8) & 0xFF;
    buf[2] = (val >> 16) & 0xFF;
    buf[3] = (val >> 24) & 0xFF;
}

// Recursive helper to traverse nodes and collect data
static int scan_recursive(ARTNode* node, uint8_t* current_key, uint32_t current_key_len,
                          uint8_t* out_buffer, uint32_t out_buffer_size, uint32_t* offset) {
    if (!node) return 0;

    if (node->is_end) {
        // Check buffer size in 64-bit arithmetic: 4 + key + 4 + val can
        // overflow a 32-bit sum for large values.
        uint64_t required_space = (uint64_t)4 + current_key_len + 4 + node->val_len;
        if ((uint64_t)*offset + required_space > out_buffer_size) return -1; // Buffer too small

        // Write Key Length
        write_u32(&out_buffer[*offset], current_key_len);
        *offset += 4;

        // Write Key
        memory_copy(&out_buffer[*offset], current_key, current_key_len);
        *offset += current_key_len;

        // Write Val Length
        write_u32(&out_buffer[*offset], node->val_len);
        *offset += 4;

        // Write Val
        memory_copy(&out_buffer[*offset], node->val_ptr, node->val_len);
        *offset += node->val_len;
    }

    // Process children
    if (node->type == NODE4) {
        ARTNode4* n = (ARTNode4*)node;
        for (uint8_t i = 0; i < n->num_children; i++) {
            if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1; // See art.h
            current_key[current_key_len] = n->keys[i];

            if (scan_recursive(n->children[i], current_key, current_key_len + 1, out_buffer, out_buffer_size, offset) == -1) {
                return -1;
            }
        }
    } else if (node->type == NODE16) {
        ARTNode16* n = (ARTNode16*)node;
        for (uint8_t i = 0; i < n->num_children; i++) {
            if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
            current_key[current_key_len] = n->keys[i];

            if (scan_recursive(n->children[i], current_key, current_key_len + 1, out_buffer, out_buffer_size, offset) == -1) {
                return -1;
            }
        }
    } else if (node->type == NODE48) {
        ARTNode48* n = (ARTNode48*)node;
        for (int i = 0; i < 256; i++) {
            if (n->child_index[i] != 0) {
                if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
                current_key[current_key_len] = (uint8_t)i;
                if (scan_recursive(n->children[n->child_index[i] - 1], current_key, current_key_len + 1, out_buffer, out_buffer_size, offset) == -1) {
                    return -1;
                }
            }
        }
    } else if (node->type == NODE256) {
        ARTNode256* n = (ARTNode256*)node;
        for (int i = 0; i < 256; i++) {
            if (n->children[i] != NULL) {
                if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
                current_key[current_key_len] = (uint8_t)i;
                if (scan_recursive(n->children[i], current_key, current_key_len + 1, out_buffer, out_buffer_size, offset) == -1) {
                    return -1;
                }
            }
        }
    }

    return 0;
}

int art_scan(ART* art, const uint8_t* prefix, uint32_t prefix_len, uint8_t* out_buffer, uint32_t out_buffer_size) {
    if (!art || !art->root || !out_buffer || out_buffer_size == 0) return -1;

    ARTNode* current = art->root;
    uint8_t current_key[YOWZA_MAX_KEY_LEN + 1];

    if (prefix_len > YOWZA_MAX_KEY_LEN) return -1;

    // Fast-forward to the prefix node
    for (uint32_t i = 0; i < prefix_len; i++) {
        uint8_t c = prefix[i];
        ARTNode** child_ref = find_child(current, c);

        if (child_ref == NULL) return 0; // Prefix not found, return 0 bytes written

        current_key[i] = c;
        current = *child_ref;
    }

    uint32_t offset = 0;

    // Start traversal exactly from the found prefix node
    if (scan_recursive(current, current_key, prefix_len, out_buffer, out_buffer_size, &offset) == -1) {
        return -1;
    }

    return offset;
}

/* Snapshot v2 helpers. FNV-1a 64 is not a cryptographic checksum, but it
 * reliably catches truncation and single-byte corruption in snapshots,
 * which previously parsed as valid data until they happened to fail. */
static void write_u64(uint8_t* buf, uint64_t val) {
    for (int i = 0; i < 8; i++) buf[i] = (uint8_t)((val >> (i * 8)) & 0xFF);
}

static uint64_t read_u64(const uint8_t* buf) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)buf[i] << (i * 8);
    return v;
}

static uint64_t fnv1a_64(const uint8_t* data, uint32_t len) {
    uint64_t h = 14695981039346656037ULL;
    for (uint32_t i = 0; i < len; i++) {
        h ^= data[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static int snapshot_is_v2(const uint8_t* buf, uint32_t size) {
    return size >= 4 && buf[0] == 'Y' && buf[1] == 'W' && buf[2] == 'Z' && buf[3] == '6';
}

/* Same traversal as scan_recursive, but each entry also carries its
 * CRDT timestamp, and live entries are counted for the v2 header. */
static int serialize_recursive(ARTNode* node, uint8_t* current_key, uint32_t current_key_len,
                               uint8_t* out_buffer, uint32_t out_buffer_size,
                               uint32_t* offset, uint32_t* count) {
    if (!node) return 0;

    /* Live entries, plus tombstones: a node with timestamp > 0 that is
     * not an end node is a soft-deleted key (a node that never was an
     * end node has timestamp 0: create_node* initialises it so, and
     * art_insert only stamps the terminal node). The root
     * (current_key_len == 0) is never a key, so it is excluded. A
     * tombstone is written with raw ValLen = YOWZA_TOMBSTONE_FLAG and
     * 0 value bytes, so the old value bytes are freed by a later
     * compact while the tombstone itself survives. */
    if (node->is_end || (node->timestamp > 0 && current_key_len > 0)) {
        uint32_t vlen = node->is_end ? node->val_len : 0;
        uint32_t raw_vlen = node->is_end ? node->val_len : YOWZA_TOMBSTONE_FLAG;
        uint64_t required_space = (uint64_t)4 + current_key_len + 4 + vlen + 8;
        if ((uint64_t)*offset + required_space > out_buffer_size) return -1;
        write_u32(&out_buffer[*offset], current_key_len);
        *offset += 4;
        memory_copy(&out_buffer[*offset], current_key, current_key_len);
        *offset += current_key_len;
        write_u32(&out_buffer[*offset], raw_vlen);
        *offset += 4;
        if (vlen > 0) {
            memory_copy(&out_buffer[*offset], node->val_ptr, vlen);
            *offset += vlen;
        }
        write_u64(&out_buffer[*offset], node->timestamp);
        *offset += 8;
        (*count)++;
    }

    if (node->type == NODE4) {
        ARTNode4* n = (ARTNode4*)node;
        for (uint8_t i = 0; i < n->num_children; i++) {
            if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
            current_key[current_key_len] = n->keys[i];
            if (serialize_recursive(n->children[i], current_key, current_key_len + 1, out_buffer, out_buffer_size, offset, count) == -1) return -1;
        }
    } else if (node->type == NODE16) {
        ARTNode16* n = (ARTNode16*)node;
        for (uint8_t i = 0; i < n->num_children; i++) {
            if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
            current_key[current_key_len] = n->keys[i];
            if (serialize_recursive(n->children[i], current_key, current_key_len + 1, out_buffer, out_buffer_size, offset, count) == -1) return -1;
        }
    } else if (node->type == NODE48) {
        ARTNode48* n = (ARTNode48*)node;
        for (int i = 0; i < 256; i++) {
            if (n->child_index[i] != 0) {
                if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
                current_key[current_key_len] = (uint8_t)i;
                if (serialize_recursive(n->children[n->child_index[i] - 1], current_key, current_key_len + 1, out_buffer, out_buffer_size, offset, count) == -1) return -1;
            }
        }
    } else if (node->type == NODE256) {
        ARTNode256* n = (ARTNode256*)node;
        for (int i = 0; i < 256; i++) {
            if (n->children[i] != NULL) {
                if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
                current_key[current_key_len] = (uint8_t)i;
                if (serialize_recursive(n->children[i], current_key, current_key_len + 1, out_buffer, out_buffer_size, offset, count) == -1) return -1;
            }
        }
    }
    return 0;
}

int art_serialize(ART* art, uint8_t* out_buffer, uint32_t out_buffer_size) {
    if (!art || !art->root || !out_buffer || out_buffer_size < 20) return -1;

    uint32_t offset = 12; /* header, count patched in after traversal */
    uint32_t count = 0;
    uint8_t current_key[YOWZA_MAX_KEY_LEN + 1];

    out_buffer[0] = 'Y'; out_buffer[1] = 'W'; out_buffer[2] = 'Z'; out_buffer[3] = '6';
    write_u32(&out_buffer[4], 1); /* version */
    write_u32(&out_buffer[8], 0); /* count placeholder */

    /* Leave 8 bytes at the end for the checksum. */
    if (serialize_recursive(art->root, current_key, 0, out_buffer, out_buffer_size - 8, &offset, &count) == -1) {
        return -1;
    }

    write_u32(&out_buffer[8], count);
    write_u64(&out_buffer[offset], fnv1a_64(out_buffer, offset));
    return (int)(offset + 8);
}

static uint32_t read_u32(const uint8_t* buf) {
    return (uint32_t)buf[0] |
           ((uint32_t)buf[1] << 8) |
           ((uint32_t)buf[2] << 16) |
           ((uint32_t)buf[3] << 24);
}

int art_validate_snapshot(const uint8_t* in_buffer, uint32_t in_buffer_size) {
    if (!in_buffer) return -1;
    if (in_buffer_size == 0) return 0; /* empty legacy snapshot */

    if (snapshot_is_v2(in_buffer, in_buffer_size)) {
        if (in_buffer_size < 20) return -1;
        if (read_u32(&in_buffer[4]) != 1) return -1; /* version */
        uint32_t count = read_u32(&in_buffer[8]);
        uint32_t limit = in_buffer_size - 8; /* entries end before checksum */
        uint32_t offset = 12;
        for (uint32_t i = 0; i < count; i++) {
            if (offset + 4 > limit) return -1;
            uint32_t key_len = read_u32(&in_buffer[offset]); offset += 4;
            if (key_len == 0 || key_len > YOWZA_MAX_KEY_LEN) return -1;
            if (offset + key_len > limit) return -1;
            offset += key_len;
            if (offset + 4 > limit) return -1;
            uint32_t raw_val_len = read_u32(&in_buffer[offset]); offset += 4;
            uint32_t is_tomb = (raw_val_len & YOWZA_TOMBSTONE_FLAG) != 0;
            uint32_t val_len = raw_val_len & ~YOWZA_TOMBSTONE_FLAG;
            if (is_tomb && val_len != 0) return -1; /* tombstone carries 0 value bytes */
            if (offset + val_len > limit) return -1;
            offset += val_len;
            if (offset + 8 > limit) return -1; /* timestamp */
            offset += 8;
        }
        if (offset != limit) return -1; /* trailing garbage or wrong count */
        if (read_u64(&in_buffer[in_buffer_size - 8]) != fnv1a_64(in_buffer, in_buffer_size - 8)) return -1;
        return 0;
    }

    /* Legacy format: strict parse, no checksum available. */
    uint32_t offset = 0;
    while (offset < in_buffer_size) {
        if (offset + 4 > in_buffer_size) return -1;
        uint32_t key_len = read_u32(&in_buffer[offset]); offset += 4;
        if (key_len == 0 || key_len > YOWZA_MAX_KEY_LEN) return -1;
        if (offset + key_len > in_buffer_size) return -1;
        offset += key_len;
        if (offset + 4 > in_buffer_size) return -1;
        uint32_t val_len = read_u32(&in_buffer[offset]); offset += 4;
        if (offset + val_len > in_buffer_size) return -1;
        offset += val_len;
    }
    return 0;
}

/* Sum of the 8-byte-aligned value bytes in a snapshot, plus the root
 * node allocation, for the pre-reset fit guard in snapshot restore
 * (see yowza_snapshot_restore / yowza_db_snapshot_restore). The
 * caller MUST have validated the snapshot first; this walk is
 * defensive and returns -1 on any inconsistency. Tombstones carry
 * 0 value bytes. This sum is a NECESSARY condition for deserialize
 * to fit in the arena, not a sufficient one: ART node allocations
 * along each key path are not counted, so a snapshot can still OOM
 * on node overhead in pathological cases. But a snapshot whose
 * values alone exceed the arena definitely cannot fit — that is
 * the oversized-external-snapshot case this guard exists for, and
 * it must be rejected BEFORE arena_reset, not after the live data
 * has already been wiped. A snapshot produced by art_serialize from
 * an arena of the same size always fits by the free-list argument
 * (rebuild uses no more than the original tree's memory), so this
 * guard cannot reject a legitimate same-arena restore on value
 * bytes alone. */
int art_snapshot_value_bytes(const uint8_t* in_buffer, uint32_t in_buffer_size, uint64_t* out_total) {
    if (!in_buffer || !out_total) return -1;
    if (in_buffer_size == 0) {
        *out_total = 0;
        return 0;
    }
    uint64_t total = (uint64_t)arena_align_forward(sizeof(ARTNode4), 8);
    if (snapshot_is_v2(in_buffer, in_buffer_size)) {
        if (in_buffer_size < 20) return -1;
        uint32_t count = read_u32(&in_buffer[8]);
        uint32_t limit = in_buffer_size - 8;
        uint32_t offset = 12;
        for (uint32_t i = 0; i < count; i++) {
            if (offset + 4 > limit) return -1;
            uint32_t key_len = read_u32(&in_buffer[offset]); offset += 4;
            if (key_len == 0 || key_len > YOWZA_MAX_KEY_LEN) return -1;
            if (offset + key_len > limit) return -1;
            offset += key_len;
            if (offset + 4 > limit) return -1;
            uint32_t raw_val_len = read_u32(&in_buffer[offset]); offset += 4;
            uint32_t val_len = raw_val_len & ~YOWZA_TOMBSTONE_FLAG;
            if (offset + val_len > limit) return -1;
            offset += val_len;
            if (offset + 8 > limit) return -1;
            offset += 8;
            uint64_t aligned = (uint64_t)arena_align_forward(val_len, 8);
            if (aligned == (uint64_t)(size_t)-1 || total > (uint64_t)-1 - aligned) return -1;
            total += aligned;
        }
        if (offset != limit) return -1;
    } else {
        uint32_t offset = 0;
        while (offset < in_buffer_size) {
            if (offset + 4 > in_buffer_size) return -1;
            uint32_t key_len = read_u32(&in_buffer[offset]); offset += 4;
            if (key_len == 0 || key_len > YOWZA_MAX_KEY_LEN) return -1;
            if (offset + key_len > in_buffer_size) return -1;
            offset += key_len;
            if (offset + 4 > in_buffer_size) return -1;
            uint32_t val_len = read_u32(&in_buffer[offset]); offset += 4;
            if (offset + val_len > in_buffer_size) return -1;
            offset += val_len;
            uint64_t aligned = (uint64_t)arena_align_forward(val_len, 8);
            if (aligned == (uint64_t)(size_t)-1 || total > (uint64_t)-1 - aligned) return -1;
            total += aligned;
        }
    }
    *out_total = total;
    return 0;
}

/* Internal helper to restore a soft-deleted tombstone node during
 * art_deserialize without allocating a value buffer. Mirrors the
 * path-walk of art_insert (including its arena-checkpoint rollback
 * on failure), but stamps the terminal node as a tombstone instead
 * of a live value. A restored tombstone also rebuilds max_timestamp,
 * so a stale write after restore still loses. LWW IS enforced here
 * against a pre-existing node for the same key: a stale tombstone
 * (timestamp < current->timestamp) loses and leaves the node
 * untouched, so a hand-crafted snapshot with duplicate entries for
 * one key is commutative in the same sense as art_insert. A
 * tombstone at the SAME timestamp as a live value still wins (it is
 * the delete that followed that put); a live write at the same
 * timestamp as a tombstone loses in art_insert, so the two orders
 * agree. */
static int art_restore_tombstone(ART* art, const uint8_t* key, uint32_t key_len, uint64_t timestamp) {
    if (!art || !art->root || !key || key_len == 0 || key_len > YOWZA_MAX_KEY_LEN) return -1;
    ARTNode** current_ref = &art->root;
    for (uint32_t i = 0; i < key_len; i++) {
        uint8_t c = key[i];
        ARTNode** child_ref = find_child(*current_ref, c);
        if (child_ref == NULL) {
            size_t checkpoint = art->arena->offset;
            ARTNode* new_node = (ARTNode*)create_node4(art);
            if (!new_node) return -1;
            ARTNode* expanded = add_child(art, *current_ref, c, new_node);
            if (!expanded) {
                art->arena->offset = checkpoint;
                return -1;
            }
            if (expanded != *current_ref) *current_ref = expanded;
            current_ref = find_child(*current_ref, c);
            if (!current_ref) {
                art->arena->offset = checkpoint;
                return -1;
            }
        } else {
            current_ref = child_ref;
        }
    }
    ARTNode* current = *current_ref;
    if (timestamp > art->max_timestamp) art->max_timestamp = timestamp;
    /* Enforce LWW even if a hand-crafted snapshot contains duplicate entries
     * for the same key: a stale tombstone (< current->timestamp) loses. */
    if (current->timestamp > 0 && timestamp < current->timestamp) {
        return 0;
    }
    current->is_end = 0;
    current->val_ptr = NULL;
    current->val_len = 0;
    current->timestamp = timestamp;
    current->version = art->applying ? art->apply_seq : ++art->commit_seq;
    return 0;
}

/* Phase 2, Track B: sorted iterator (see art.h). The key buffer is
 * owned by art_iterate (one buffer passed down the recursion, never
 * reallocated per frame); NODE4/NODE16 children are sorted by key byte
 * with insertion sort (at most 16 entries) before recursing. */
static int art_iterate_recursive(ARTNode* node, uint8_t* current_key,
                                 uint32_t current_key_len, art_iter_cb cb,
                                 void* ctx, int* stopped) {
    if (!node || *stopped) return 0;

    if (current_key_len > 0 &&
        (node->is_end || node->timestamp > 0)) {
        int is_tomb = node->is_end ? 0 : 1;
        const uint8_t* val = node->is_end ? node->val_ptr : NULL;
        uint32_t vlen = node->is_end ? node->val_len : 0;
        int rc = cb(ctx, current_key, current_key_len, val, vlen, is_tomb,
                    node->timestamp, node->version);
        if (rc != 0) {
            *stopped = 1;
            return 0;
        }
    }

    if (node->type == NODE4) {
        ARTNode4* n = (ARTNode4*)node;
        uint8_t keys[4];
        ARTNode* children[4];
        int i, j, count = n->num_children;
        for (i = 0; i < count; i++) {
            keys[i] = n->keys[i];
            children[i] = n->children[i];
        }
        for (i = 1; i < count; i++) {
            uint8_t k = keys[i];
            ARTNode* c = children[i];
            j = i - 1;
            while (j >= 0 && keys[j] > k) {
                keys[j + 1] = keys[j];
                children[j + 1] = children[j];
                j--;
            }
            keys[j + 1] = k;
            children[j + 1] = c;
        }
        for (i = 0; i < count; i++) {
            if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
            current_key[current_key_len] = keys[i];
            if (art_iterate_recursive(children[i], current_key,
                                      current_key_len + 1, cb, ctx,
                                      stopped) != 0)
                return -1;
            if (*stopped) return 0;
        }
    } else if (node->type == NODE16) {
        ARTNode16* n = (ARTNode16*)node;
        uint8_t keys[16];
        ARTNode* children[16];
        int i, j, count = n->num_children;
        for (i = 0; i < count; i++) {
            keys[i] = n->keys[i];
            children[i] = n->children[i];
        }
        for (i = 1; i < count; i++) {
            uint8_t k = keys[i];
            ARTNode* c = children[i];
            j = i - 1;
            while (j >= 0 && keys[j] > k) {
                keys[j + 1] = keys[j];
                children[j + 1] = children[j];
                j--;
            }
            keys[j + 1] = k;
            children[j + 1] = c;
        }
        for (i = 0; i < count; i++) {
            if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
            current_key[current_key_len] = keys[i];
            if (art_iterate_recursive(children[i], current_key,
                                      current_key_len + 1, cb, ctx,
                                      stopped) != 0)
                return -1;
            if (*stopped) return 0;
        }
    } else if (node->type == NODE48) {
        ARTNode48* n = (ARTNode48*)node;
        int b;
        for (b = 0; b < 256; b++) {
            if (n->child_index[b] != 0) {
                if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
                current_key[current_key_len] = (uint8_t)b;
                if (art_iterate_recursive(
                        n->children[n->child_index[b] - 1], current_key,
                        current_key_len + 1, cb, ctx, stopped) != 0)
                    return -1;
                if (*stopped) return 0;
            }
        }
    } else if (node->type == NODE256) {
        ARTNode256* n = (ARTNode256*)node;
        int b;
        for (b = 0; b < 256; b++) {
            if (n->children[b] != NULL) {
                if (current_key_len >= YOWZA_MAX_KEY_LEN) return -1;
                current_key[current_key_len] = (uint8_t)b;
                if (art_iterate_recursive(n->children[b], current_key,
                                          current_key_len + 1, cb, ctx,
                                          stopped) != 0)
                    return -1;
                if (*stopped) return 0;
            }
        }
    }
    return 0;
}

int art_iterate(ART* art, art_iter_cb cb, void* ctx) {
    uint8_t current_key[YOWZA_MAX_KEY_LEN + 1];
    int stopped = 0;
    if (!art || !art->root || !cb) return -1;
    if (art_iterate_recursive(art->root, current_key, 0, cb, ctx,
                              &stopped) != 0)
        return -1;
    return stopped ? 1 : 0;
}

int art_deserialize(ART* art, const uint8_t* in_buffer, uint32_t in_buffer_size) {

    if (!art || !in_buffer) return -1;
    if (art_validate_snapshot(in_buffer, in_buffer_size) != 0) return -1;

    if (snapshot_is_v2(in_buffer, in_buffer_size)) {
        uint32_t count = read_u32(&in_buffer[8]);
        uint32_t offset = 12;
        for (uint32_t i = 0; i < count; i++) {
            uint32_t key_len = read_u32(&in_buffer[offset]); offset += 4;
            const uint8_t* key = &in_buffer[offset]; offset += key_len;
            uint32_t raw_val_len = read_u32(&in_buffer[offset]); offset += 4;
            uint32_t is_tomb = (raw_val_len & YOWZA_TOMBSTONE_FLAG) != 0;
            uint32_t val_len = raw_val_len & ~YOWZA_TOMBSTONE_FLAG;
            const uint8_t* val = &in_buffer[offset]; offset += val_len;
            uint64_t timestamp = read_u64(&in_buffer[offset]); offset += 8;
            if (is_tomb) {
                if (art_restore_tombstone(art, key, key_len, timestamp) != 0) return -1;
            } else {
                if (art_insert(art, key, key_len, val, val_len, timestamp) != 0) return -1;
            }
        }
        return 0;
    }

    uint32_t offset = 0;
    while (offset < in_buffer_size) {
        uint32_t key_len = read_u32(&in_buffer[offset]);
        offset += 4;
        const uint8_t* key = &in_buffer[offset];
        offset += key_len;
        uint32_t val_len = read_u32(&in_buffer[offset]);
        offset += 4;
        const uint8_t* val = &in_buffer[offset];
        offset += val_len;
        if (art_insert(art, key, key_len, val, val_len, 0) != 0) {
            return -1; // OOM during restore
        }
    }

    return 0;
}
