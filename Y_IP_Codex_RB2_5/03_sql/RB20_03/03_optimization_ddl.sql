-- Optimization DDL for Y_IP_Codex_RB2_5 Step 03
-- Based on wenti.md and implementation_plan.md

-- 1. Task Planning Tables
CREATE TABLE IF NOT EXISTS rb20_v2_5.step03_task_plan (
  run_id           text     NOT NULL,
  shard_id         smallint NOT NULL,
  bucket_id        int      NOT NULL,
  est_member_rows  bigint   NOT NULL,
  est_block_cnt    int      NOT NULL,
  status           text     NOT NULL DEFAULT 'PENDING',
  started_at       timestamptz,
  finished_at      timestamptz,
  PRIMARY KEY (run_id, shard_id, bucket_id)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.step03_block_bucket (
  run_id          text     NOT NULL,
  shard_id        smallint NOT NULL,
  block_id_natural text    NOT NULL,
  bucket_id       int      NOT NULL,
  est_members     bigint,
  PRIMARY KEY (run_id, shard_id, block_id_natural)
);

CREATE INDEX IF NOT EXISTS idx_step03_bb_run_shard_bucket
ON rb20_v2_5.step03_block_bucket(run_id, shard_id, bucket_id, block_id_natural);

-- 2. Staging Table (UNLOGGED, No Indexes initially)
-- Mirror structure of profile_pre but unlogged for speed
CREATE UNLOGGED TABLE IF NOT EXISTS rb20_v2_5.profile_pre_stage (
    LIKE rb20_v2_5.profile_pre INCLUDING DEFAULTS
);

-- 3. Indexes for Input Tables Optimization
-- Essential for "lookup by block" pattern
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_map_run_shard_block
ON rb20_v2_5.map_member_block_natural (run_id, shard_id, block_id_natural)
INCLUDE (ip_long);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_map_run_shard_ip
ON rb20_v2_5.map_member_block_natural (run_id, shard_id, ip_long);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sm_run_shard_ip
ON rb20_v2_5.source_members (run_id, shard_id, ip_long);

-- 4. Slim Table (Actual creation will be done per-run via script to filter by run_id, 
-- but DDL template is here for reference)
-- CREATE UNLOGGED TABLE rb20_v2_5.source_members_slim AS ...
