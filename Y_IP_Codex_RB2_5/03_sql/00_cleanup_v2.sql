
-- EXPLICT CLEANUP SCRIPT (Safe for same-run-id re-execution)
-- Deletes data for the specific RUN ID from all pipeline tables (Reverse Order)

-- Phase 8: QA
DELETE FROM rb20_v2_5.qa_assert WHERE run_id='{{run_id}}';

-- Phase 7: Pipeline 2
DELETE FROM rb20_v2_5.f_members WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.e_members WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.r1_members WHERE run_id='{{run_id}}';

-- Phase 6: Global H
DELETE FROM rb20_v2_5.core_numbers WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.h_blocks WHERE run_id='{{run_id}}';

-- Phase 5: Pipeline 1 Post-Opt
DELETE FROM rb20_v2_5.profile_final WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.block_final WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.map_member_block_final WHERE run_id='{{run_id}}';

-- Phase 4: Step 11 Opt
DELETE FROM rb20_v2_5.window_headtail_64 WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.split_events_64 WHERE run_id='{{run_id}}';

-- Phase 3: Step 03 Opt Support
DELETE FROM rb20_v2_5.drop_members WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.keep_members WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.preh_blocks WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.profile_pre WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.profile_pre_stage WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.step03_block_bucket WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.step03_task_plan WHERE run_id='{{run_id}}';

-- Phase 2: Pre-Opt
DELETE FROM rb20_v2_5.map_member_block_natural WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.block_natural WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.step_stats WHERE run_id='{{run_id}}';

-- Phase 1: Init / Source
DELETE FROM rb20_v2_5.source_members WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.source_members_slim WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.shard_plan WHERE run_id='{{run_id}}';
DELETE FROM rb20_v2_5.abnormal_dedup WHERE run_id='{{run_id}}';

-- Config & Meta (Optional, usually we KEEP meta but reset status)
-- UPDATE rb20_v2_5.run_meta SET status='INIT' WHERE run_id='{{run_id}}';
