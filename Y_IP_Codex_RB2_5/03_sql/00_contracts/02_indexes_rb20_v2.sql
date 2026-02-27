-- RB20 v2.0 / Contracts / Minimal Indexes (v1)
-- 原则：先最小可用索引，避免在超大表上一次性建过多索引导致长时间锁表；后续按 EXPLAIN 再补。

-- ShardPlan
CREATE INDEX IF NOT EXISTS shard_plan_run_range_idx
  ON rb20_v2_5.shard_plan (run_id, ip_long_start, ip_long_end);

-- Abnormal
CREATE INDEX IF NOT EXISTS abnormal_dedup_run_idx
  ON rb20_v2_5.abnormal_dedup (run_id);

-- Source Members: join/filter keys
CREATE INDEX IF NOT EXISTS source_members_run_ip_idx
  ON rb20_v2_5.source_members (run_id, ip_long);
CREATE INDEX IF NOT EXISTS source_members_run_shard_atom_idx
  ON rb20_v2_5.source_members (run_id, shard_id, atom27_id);
CREATE INDEX IF NOT EXISTS source_members_run_shard_bucket64_idx
  ON rb20_v2_5.source_members (run_id, shard_id, bucket64);
CREATE INDEX IF NOT EXISTS source_members_run_shard_valid_idx
  ON rb20_v2_5.source_members (run_id, shard_id, is_valid);

-- Natural map lookups
CREATE INDEX IF NOT EXISTS map_nat_run_shard_block_idx
  ON rb20_v2_5.map_member_block_natural (run_id, shard_id, block_id_natural);

-- PreH / Window / SplitEvents lookups
CREATE INDEX IF NOT EXISTS preh_blocks_run_shard_idx
  ON rb20_v2_5.preh_blocks (run_id, shard_id);
CREATE INDEX IF NOT EXISTS window_ht_run_shard_block_idx
  ON rb20_v2_5.window_headtail_64 (run_id, shard_id, block_id_natural);
CREATE INDEX IF NOT EXISTS split_events_run_shard_block_idx
  ON rb20_v2_5.split_events_64 (run_id, shard_id, block_id_natural);

-- Final map lookups
CREATE INDEX IF NOT EXISTS map_final_run_shard_block_final_idx
  ON rb20_v2_5.map_member_block_final (run_id, shard_id, block_id_final);
CREATE INDEX IF NOT EXISTS map_final_run_shard_parent_idx
  ON rb20_v2_5.map_member_block_final (run_id, shard_id, block_id_parent);

-- Profiles
CREATE INDEX IF NOT EXISTS profile_pre_run_shard_tier_idx
  ON rb20_v2_5.profile_pre (run_id, shard_id, network_tier_pre);
CREATE INDEX IF NOT EXISTS profile_final_run_shard_tier_idx
  ON rb20_v2_5.profile_final (run_id, shard_id, network_tier_final);

-- H/E/F membership keys
CREATE INDEX IF NOT EXISTS h_members_run_block_idx
  ON rb20_v2_5.h_members (run_id, block_id_final);
CREATE INDEX IF NOT EXISTS e_members_run_shard_atom_idx
  ON rb20_v2_5.e_members (run_id, shard_id, atom27_id);
CREATE INDEX IF NOT EXISTS f_members_run_shard_atom_idx
  ON rb20_v2_5.f_members (run_id, shard_id, atom27_id);

