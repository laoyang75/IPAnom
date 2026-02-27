-- RB20_03 Post-Processing (Run after Python Profile Optimization)
-- Populates preh_blocks, keep/drop members, and stats based on profile_pre

-- 1. PreH Blocks (Valid & Bucket Crossing)
INSERT INTO rb20_v2_5.preh_blocks(run_id, contract_version, shard_id, block_id_natural)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint, p.block_id_natural
FROM rb20_v2_5.profile_pre p
JOIN rb20_v2_5.block_natural bn
  ON bn.run_id=p.run_id AND bn.shard_id=p.shard_id AND bn.block_id_natural=p.block_id_natural
WHERE p.run_id='{{run_id}}' AND p.shard_id={{shard_id}}::smallint
  AND p.keep_flag
  AND p.valid_cnt > 0
  AND (bn.ip_start / 64) <> (bn.ip_end / 64);

-- 2. Keep Members
INSERT INTO rb20_v2_5.keep_members(run_id, contract_version, shard_id, ip_long, block_id_natural, keep_flag)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  map.ip_long, map.block_id_natural, true
FROM rb20_v2_5.map_member_block_natural map
JOIN rb20_v2_5.profile_pre p
  ON p.run_id=map.run_id AND p.shard_id=map.shard_id AND p.block_id_natural=map.block_id_natural
WHERE map.run_id='{{run_id}}' AND map.shard_id={{shard_id}}::smallint
  AND p.keep_flag;

-- 3. Drop Members
INSERT INTO rb20_v2_5.drop_members(run_id, contract_version, shard_id, ip_long, block_id_natural, drop_reason)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  map.ip_long, map.block_id_natural, p.drop_reason
FROM rb20_v2_5.map_member_block_natural map
JOIN rb20_v2_5.profile_pre p
  ON p.run_id=map.run_id AND p.shard_id=map.shard_id AND p.block_id_natural=map.block_id_natural
WHERE map.run_id='{{run_id}}' AND map.shard_id={{shard_id}}::smallint
  AND NOT p.keep_flag;

-- 4. Step Stats
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_03' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'preh_block_cnt', COUNT(*)::numeric
FROM rb20_v2_5.preh_blocks
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

-- (Additional stats omitted for brevity, can be added if needed)
