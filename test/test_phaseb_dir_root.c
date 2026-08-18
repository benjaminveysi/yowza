/* Phase B tests: shared published-state resolver + versioned
 * immutable directory root + legacy-directory migration.
 *
 * OWNERSHIP NOTE (same pattern as test_scan_cursor.c):
 * test/main.c is owned by another agent, so this file
 * self-registers through a constructor: the tests below run
 * when the test_runner binary starts, before main's suite.
 * If main.c later gains extern declarations + calls for
 * these functions, delete the constructor at the bottom to
 * avoid running them twice.
 *
 * These tests use the memory store directly (no SegDB):
 * they prove the resolver's kinds, the root format, and
 * migration. SegDB-level use of the resolver (recover +
 * reconcile) is exercised by the existing recovery /
 * crash C tests in make test.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "manifest.h"
#include "storage2.h"

static Manifest pb_man;
static SegdbPublishedState pb_state;

static void pb_fill_manifest(Manifest* m, uint64_t gen, uint64_t seq) {
    memset(m, 0, sizeof(*m));
    m->generation = gen;
    m->checkpoint_seq = seq;
    m->schema_version = 1;
    m->segment_count = 1;
    memset(&m->segments[0], 0, sizeof(m->segments[0]));
    strncpy(m->segments[0].obj, "seg.1", MANIFEST_MAX_OBJ_LEN);
    m->segments[0].first_key_len = 1;
    m->segments[0].first_key[0] = 'a';
    m->segments[0].last_key_len = 1;
    m->segments[0].last_key[0] = 'b';
    m->segments[0].min_seq = 1;
    m->segments[0].max_seq = 2;
    m->segments[0].entry_count = 10;
    m->segments[0].index_type = 0;
    m->segments[0].model_version = 0;
    m->segments[0].byte_size = 100;
    m->segments[0].checksum = 0;
}

static int pb_publish_gen(YowzaStore* st, uint64_t gen, uint64_t seq) {
    char tmp[64], final_name[64];
    pb_fill_manifest(&pb_man, gen, seq);
    snprintf(tmp, sizeof(tmp), "manifest.%llu.tmp", (unsigned long long)gen);
    snprintf(final_name, sizeof(final_name), "manifest.%llu", (unsigned long long)gen);
    if (manifest_write(st, tmp, &pb_man) != 0) return -1;
    if (manifest_publish(st, tmp, final_name) != 0) return -1;
    if (manifest_write_current(st, "CURRENT.tmp", final_name) != 0) return -1;
    if (manifest_publish(st, "CURRENT.tmp", "CURRENT") != 0) return -1;
    return 0;
}

static void pb_publish_dir(YowzaStore* st, ManifestDir* dir) {
    assert(manifest_dir_write(st, "manifest.dir.tmp", dir) == 0);
    assert(manifest_publish(st, "manifest.dir.tmp", MANIFEST_DIR_OBJ) == 0);
    assert(manifest_write_current(st, "CURRENT.tmp", MANIFEST_DIR_OBJ) == 0);
    assert(manifest_publish(st, "CURRENT.tmp", "CURRENT") == 0);
}

static void pb_fill_dir(ManifestDir* dir, uint64_t gen, uint64_t seq) {
    memset(dir, 0, sizeof(*dir));
    dir->generation = gen;
    dir->checkpoint_seq = seq;
    dir->schema_version = 1;
    dir->group_count = 2;
    strncpy(dir->groups[0], "manifest.1", MANIFEST_MAX_OBJ_LEN);
    strncpy(dir->groups[1], "manifest.2", MANIFEST_MAX_OBJ_LEN);
}

void test_phaseb_resolver_kinds(void) {
    printf("Running test_phaseb_resolver_kinds...\n");
    {
        /* Virgin store. */
        YowzaStore* st = yowza_store_mem_create();
        assert(st);
        assert(segdb_resolve_published_state(st, &pb_state) == 0);
        assert(pb_state.kind == SEGDB_PUBLISHED_VIRGIN);
        yowza_store_close(st);
    }
    {
        /* Single manifest. */
        YowzaStore* st = yowza_store_mem_create();
        assert(st);
        assert(pb_publish_gen(st, 7, 100) == 0);
        assert(segdb_resolve_published_state(st, &pb_state) == 0);
        assert(pb_state.kind == SEGDB_PUBLISHED_SINGLE);
        assert(pb_state.generation == 7);
        assert(strcmp(pb_state.pointer, "manifest.7") == 0);
        assert(pb_state.manifest.generation == 7);
        assert(pb_state.manifest.checkpoint_seq == 100);
        yowza_store_close(st);
    }
    {
        /* Fallback: gen 5 corrupt, gen 4 valid. Never a
         * blind pick: the fallback is reported as
         * FALLBACK with used_fallback, for the caller
         * (reconcile) to compare against pending/old. */
        YowzaStore* st = yowza_store_mem_create();
        uint8_t garbage[8] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
        char tmp[64];
        assert(st);
        assert(pb_publish_gen(st, 4, 40) == 0);
        pb_fill_manifest(&pb_man, 5, 50);
        snprintf(tmp, sizeof(tmp), "manifest.5.tmp");
        assert(manifest_write(st, tmp, &pb_man) == 0);
        assert(manifest_publish(st, tmp, "manifest.5") == 0);
        assert(manifest_write_current(st, "CURRENT.tmp", "manifest.5") == 0);
        assert(manifest_publish(st, "CURRENT.tmp", "CURRENT") == 0);
        assert(st->vt->write_at(st->state, "manifest.5", 0, garbage, sizeof(garbage)) == 0);
        assert(segdb_resolve_published_state(st, &pb_state) == 0);
        assert(pb_state.kind == SEGDB_PUBLISHED_FALLBACK);
        assert(pb_state.used_fallback == 1);
        assert(pb_state.generation == 4);
        assert(pb_state.manifest.checkpoint_seq == 40);
        yowza_store_close(st);
    }
    {
        /* Ambiguous: both pointers gone, objects remain
         * — never virgin, never a silent fallback. */
        YowzaStore* st = yowza_store_mem_create();
        assert(st);
        assert(pb_publish_gen(st, 3, 30) == 0);
        assert(st->vt->remove(st->state, "CURRENT") == 0);
        assert(segdb_resolve_published_state(st, &pb_state) == 0);
        assert(pb_state.kind == SEGDB_PUBLISHED_AMBIGUOUS);
        yowza_store_close(st);
    }
    printf("Passed.\n");
}

void test_phaseb_root_and_migration(void) {
    printf("Running test_phaseb_root_and_migration...\n");
    {
        /* Root name helpers. */
        char name[MANIFEST_MAX_OBJ_LEN + 1];
        uint64_t gen = 0;
        assert(manifest_root_name(42, name, sizeof(name)) == 0);
        assert(strcmp(name, "manifest.root.42") == 0);
        assert(manifest_root_parse_gen(name, &gen) == 0);
        assert(gen == 42);
        assert(manifest_root_parse_gen("manifest.dir", &gen) != 0);
        assert(manifest_root_parse_gen("manifest.root.0", &gen) != 0);
        assert(manifest_root_parse_gen("manifest.root.", &gen) != 0);
        assert(manifest_root_name(0, name, sizeof(name)) != 0);
    }
    {
        /* Legacy directory resolves as DIRECTORY, then
         * migrates to an immutable root that resolves
         * as DIRECTORY_ROOT — with the legacy object
         * untouched (still readable, same groups). */
        YowzaStore* st = yowza_store_mem_create();
        ManifestDir dir, dir_after, root_out;
        char root_name[MANIFEST_MAX_OBJ_LEN + 1];
        assert(st);
        pb_fill_dir(&dir, 2, 200);
        pb_publish_dir(st, &dir);
        assert(segdb_resolve_published_state(st, &pb_state) == 0);
        assert(pb_state.kind == SEGDB_PUBLISHED_DIRECTORY);
        assert(pb_state.generation == 2);
        assert(pb_state.dir.group_count == 2);
        assert(strcmp(pb_state.dir.groups[0], "manifest.1") == 0);
        assert(strcmp(pb_state.dir.groups[1], "manifest.2") == 0);
        /* Migrate to generation 3. */
        assert(manifest_migrate_dir_to_root(st, &dir, 3, root_name, sizeof(root_name)) == 0);
        assert(strcmp(root_name, "manifest.root.3") == 0);
        /* Legacy directory object unchanged. */
        assert(manifest_dir_read(st, MANIFEST_DIR_OBJ, &dir_after) == 0);
        assert(dir_after.group_count == 2);
        assert(strcmp(dir_after.groups[0], "manifest.1") == 0);
        assert(strcmp(dir_after.groups[1], "manifest.2") == 0);
        /* Root object reads back with identical groups. */
        assert(manifest_root_read(st, root_name, &root_out) == 0);
        assert(root_out.generation == 3);
        assert(root_out.checkpoint_seq == 200);
        assert(root_out.group_count == 2);
        assert(strcmp(root_out.groups[0], "manifest.1") == 0);
        assert(strcmp(root_out.groups[1], "manifest.2") == 0);
        /* Point CURRENT at the root: resolver sees it. */
        assert(manifest_write_current(st, "CURRENT.tmp", root_name) == 0);
        assert(manifest_publish(st, "CURRENT.tmp", "CURRENT") == 0);
        assert(segdb_resolve_published_state(st, &pb_state) == 0);
        assert(pb_state.kind == SEGDB_PUBLISHED_DIRECTORY_ROOT);
        assert(pb_state.generation == 3);
        assert(pb_state.dir.group_count == 2);
        assert(strcmp(pb_state.dir.groups[1], "manifest.2") == 0);
        yowza_store_close(st);
    }
    {
        /* Name/body generation mismatch is rejected,
         * not silently adopted. */
        YowzaStore* st = yowza_store_mem_create();
        ManifestDir dir;
        assert(st);
        pb_fill_dir(&dir, 7, 70);
        assert(manifest_root_write(st, "manifest.root.9.tmp", &dir) == 0);
        assert(manifest_publish(st, "manifest.root.9.tmp", "manifest.root.9") == 0);
        assert(manifest_write_current(st, "CURRENT.tmp", "manifest.root.9") == 0);
        assert(manifest_publish(st, "CURRENT.tmp", "CURRENT") == 0);
        assert(segdb_resolve_published_state(st, &pb_state) == 0);
        assert(pb_state.kind == SEGDB_PUBLISHED_AMBIGUOUS);
        yowza_store_close(st);
    }
    printf("Passed.\n");
}

/* Self-registration: see the ownership note at the top. */
__attribute__((constructor)) static void test_phaseb_dir_root_register(void) {
    test_phaseb_resolver_kinds();
    test_phaseb_root_and_migration();
}
