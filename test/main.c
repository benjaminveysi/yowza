#include <stdio.h>

extern void test_arena_initialization();
extern void test_arena_allocation();
extern void test_arena_out_of_memory();
extern void test_arena_reset();

extern void test_vm_execute_put();
extern void test_vm_execute_get_del();
extern void test_vm_execute_unknown_opcode();
extern void test_vm_execute_scan();

extern void test_art_insert_and_search();
extern void test_art_delete();
extern void test_art_scan_and_snapshot();

extern void test_c_api();

extern void test_step1_arena_overflow();
extern void test_step1_art_timestamp_promotion();
extern void test_step1_crdt_snapshot_and_zombie();
extern void test_step1_snapshot_corruption_and_legacy();
extern void test_step1_vm_malformed_and_wal();
extern void test_step1_c_api_empty_compact_restore_pool();
extern void test_step1_crdt_wal_carries_timestamp();
extern void test_step1_crdt_tombstone_max_timestamp();
extern void test_step1_serialize_too_small();
extern void test_step1_restore_tombstone_lww_duplicates();
extern void test_step1_restore_oversized_snapshot_preserves_data();

extern void test_tx_commit_atomic_multiop();
extern void test_tx_abort_invisible_and_preexisting_intact();
extern void test_tx_read_your_writes_snapshot_invisible();
extern void test_tx_conflict_aborts_and_writer_survives();
extern void test_tx_unique_and_assert_failures_roll_back();
extern void test_tx_vm_program_incomplete_and_protocol();
extern void test_tx_apply_oom_undoes_applied_ops();
extern void test_tx_wal_replay_complete_vs_incomplete();
extern void test_tx_delete_atomic_with_index();
extern void test_tx_assert_checks_committed_state_not_projected();
extern void test_tx_history_hook_captures_and_fail_closed();
extern void test_tx_history_hook_wal_failure_rolls_back();
extern void test_tx_hist_snapshot_put_overwrite();
extern void test_tx_hist_snapshot_delete();
extern void test_tx_hist_abort_leaves_nothing();
extern void test_tx_hist_history_full_fail_closed();
extern void test_tx_hist_apply_oom_rolls_back_history();
extern void test_tx_hist_flush_transfers_tx_history();
extern void test_tx_hist_production_wiring();

extern void test_storage2_mem_roundtrip();
extern void test_storage2_sparse_above_4gib();
extern void test_storage2_short_read();
extern void test_storage2_truncate();
extern void test_storage2_publish_atomicity();
extern void test_storage2_names_and_remove();
extern void test_storage2_posix_roundtrip_publish_sync();
extern void test_storage2_write_all_read_all_1mib();

extern void test_segment_roundtrip();
extern void test_segment_unsorted_rejected();
extern void test_segment_block_checksum_corruption();
extern void test_segment_footer_truncation();
extern void test_segment_sparse_index_lookup();
extern void test_segment_bloom_no_false_negative();
extern void test_segment_iterator_no_whole_load();
extern void test_segment_empty();
extern void test_segment_index_checksum_corruption();
extern void test_segment_art_iterate();
extern void test_manifest_roundtrip();
extern void test_manifest_corruption();
extern void test_manifest_validation();
extern void test_segment_manifest_checksum_link();

extern void test_segdb_legacy_fail_append();
extern void test_segdb_legacy_fail_sync();
extern void test_segdb_legacy_del_absent_and_no_second_sync();
extern void test_segdb_get_precedence();
extern void test_segdb_scan_merge_snapshot();
extern void test_segdb_flush_restart();
extern void test_segdb_flush_empty_and_virgin_recover();
extern void test_segdb_large_hot_budget();
extern void test_segdb_compact_tombstone_barrier();
extern void test_segdb_compact_hot_untouched();
extern void test_segdb_sparse_gt4g();
extern void test_segdb_art_iterate_sorted();
extern void test_segdb_preflush_hot_lost_on_mem_restart();

extern void test_crash_mid_seg_write();
extern void test_crash_before_manifest_publish();
extern void test_crash_during_publish();
extern void test_crash_after_current_before_wal_clear();
extern void test_crash_before_old_segment_removal();
extern void test_crash_manifest_corruption_fallback();
extern void test_crash_segment_corruption();

extern void test_tracka_publish_indeterminate_current();
extern void test_tracka_publish_definite_failure();
extern void test_tracka_current_bak_recovery();
extern void test_tracka_io_error_no_silent_fallback();
extern void test_tracka_double_loss_is_virgin_indistinguishable();
extern void test_tracka_persistent_wal_tombstone();
extern void test_tracka_streaming_compact_big();
extern void test_tracka_multi_level_manifest();
extern void test_tracka_transition_indeterminate();
extern void test_lifecycle_enumerate_mem();
extern void test_lifecycle_enumerate_posix();
extern void test_lifecycle_detect_prior_content();
extern void test_lifecycle_unknown_backend_never_not_found();
extern void test_lifecycle_manifest_pointer_recovery();
extern void test_lifecycle_manifest_generation_fallback();
extern void test_lifecycle_manifest_current_status();

extern void test_recovery_wire_triple_loss_fails();
extern void test_recovery_wire_generation_fallback();
extern void test_recovery_wire_virgin_ok();
extern void test_recovery_wire_io_error_never_not_found();
extern void test_recovery_wire_directory_modes();
extern void test_heap_store_recover_virgin();
extern void test_heap_store_recover_flushed_copy();

int main() {
    printf("Starting Tests...\n");

    printf("\n--- ART Tests ---\n");
    test_art_insert_and_search();
    test_art_delete();
    test_art_scan_and_snapshot();

    printf("\n--- Arena Tests ---\n");
    test_arena_initialization();
    test_arena_allocation();
    test_arena_out_of_memory();
    test_arena_reset();

    printf("\n--- VM Tests ---\n");
    test_vm_execute_put();
    test_vm_execute_scan();
    test_vm_execute_get_del();
    test_vm_execute_unknown_opcode();

    printf("\n--- C API Tests ---\n");
    test_c_api();

    printf("\n--- Step-1 Regression Tests ---\n");
    test_step1_arena_overflow();
    test_step1_art_timestamp_promotion();
    test_step1_crdt_snapshot_and_zombie();
    test_step1_snapshot_corruption_and_legacy();
    test_step1_vm_malformed_and_wal();
    test_step1_c_api_empty_compact_restore_pool();
    test_step1_crdt_wal_carries_timestamp();
    test_step1_crdt_tombstone_max_timestamp();
    test_step1_serialize_too_small();
    test_step1_restore_tombstone_lww_duplicates();
    test_step1_restore_oversized_snapshot_preserves_data();

    printf("\n--- Phase-1 Transaction Tests ---\n");
    test_tx_commit_atomic_multiop();
    test_tx_abort_invisible_and_preexisting_intact();
    test_tx_read_your_writes_snapshot_invisible();
    test_tx_conflict_aborts_and_writer_survives();
    test_tx_unique_and_assert_failures_roll_back();
    test_tx_vm_program_incomplete_and_protocol();
    test_tx_apply_oom_undoes_applied_ops();
    test_tx_wal_replay_complete_vs_incomplete();
    test_tx_delete_atomic_with_index();
    test_tx_assert_checks_committed_state_not_projected();
    test_tx_history_hook_captures_and_fail_closed();
    test_tx_history_hook_wal_failure_rolls_back();

    printf("\n--- TX + Hot History Tests ---\n");
    test_tx_hist_snapshot_put_overwrite();
    test_tx_hist_snapshot_delete();
    test_tx_hist_abort_leaves_nothing();
    test_tx_hist_history_full_fail_closed();
    test_tx_hist_apply_oom_rolls_back_history();
    test_tx_hist_flush_transfers_tx_history();
    test_tx_hist_production_wiring();

    printf("\n--- Phase-2 Storage ABI Tests ---\n");
    test_storage2_mem_roundtrip();
    test_storage2_sparse_above_4gib();
    test_storage2_short_read();
    test_storage2_truncate();
    test_storage2_publish_atomicity();
    test_storage2_names_and_remove();
    test_storage2_posix_roundtrip_publish_sync();
    test_storage2_write_all_read_all_1mib();

    printf("\n--- Phase-2 Segment / Manifest Tests ---\n");
    test_segment_roundtrip();
    test_segment_unsorted_rejected();
    test_segment_block_checksum_corruption();
    test_segment_footer_truncation();
    test_segment_sparse_index_lookup();
    test_segment_bloom_no_false_negative();
    test_segment_iterator_no_whole_load();
    test_segment_empty();
    test_segment_index_checksum_corruption();
    test_segment_art_iterate();
    test_manifest_roundtrip();
    test_manifest_corruption();
    test_manifest_validation();
    test_segment_manifest_checksum_link();

    printf("\n--- Phase-2 Layered DB Tests ---\n");
    test_segdb_legacy_fail_append();
    test_segdb_legacy_fail_sync();
    test_segdb_legacy_del_absent_and_no_second_sync();
    test_segdb_get_precedence();
    test_segdb_scan_merge_snapshot();
    test_segdb_flush_restart();
    test_segdb_flush_empty_and_virgin_recover();
    test_segdb_large_hot_budget();
    test_segdb_compact_tombstone_barrier();
    test_segdb_compact_hot_untouched();
    test_segdb_sparse_gt4g();
    test_segdb_art_iterate_sorted();
    test_segdb_preflush_hot_lost_on_mem_restart();

    printf("\n--- Phase-2 Crash Simulation Tests ---\n");
    test_crash_mid_seg_write();
    test_crash_before_manifest_publish();
    test_crash_during_publish();
    test_crash_after_current_before_wal_clear();
    test_crash_before_old_segment_removal();
    test_crash_manifest_corruption_fallback();
    test_crash_segment_corruption();

    printf("\n--- Track A Tests ---\n");
    test_tracka_publish_indeterminate_current();
    test_tracka_publish_definite_failure();
    test_tracka_current_bak_recovery();
    test_tracka_io_error_no_silent_fallback();
    test_tracka_double_loss_is_virgin_indistinguishable();
    test_tracka_persistent_wal_tombstone();
    test_tracka_streaming_compact_big();
    test_tracka_multi_level_manifest();
    test_tracka_transition_indeterminate();

    printf("\n--- Lifecycle Tests ---\n");
    test_lifecycle_enumerate_mem();
    test_lifecycle_enumerate_posix();
    test_lifecycle_detect_prior_content();
    test_lifecycle_unknown_backend_never_not_found();
    test_lifecycle_manifest_pointer_recovery();
    test_lifecycle_manifest_generation_fallback();
    test_lifecycle_manifest_current_status();

    printf("\n--- Recovery Wire (O2) Tests ---\n");
    test_recovery_wire_triple_loss_fails();
    test_recovery_wire_generation_fallback();
    test_recovery_wire_virgin_ok();
    test_recovery_wire_io_error_never_not_found();
    test_recovery_wire_directory_modes();

    printf("\n--- Heap Store Recover (hp_enumerate) Tests ---\n");
    test_heap_store_recover_virgin();
    test_heap_store_recover_flushed_copy();

    printf("\nAll Tests Passed!\n");
    return 0;
}
