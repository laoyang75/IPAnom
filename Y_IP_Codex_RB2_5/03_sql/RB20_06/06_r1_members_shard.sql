-- RB20_06 (per-shard): R1 Residue = KeepMembers \ H_cov
-- 依赖：RB20_03 已生成 keep_members；RB20_04 已生成 map_member_block_final；RB20_05 已生成 h_members
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_id}}         0..(shard_cnt-1)

DELETE FROM rb20_v2_5.r1_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

SET enable_nestloop = off;
SET work_mem = '256MB';
SET statement_timeout = '15min';

WITH h_cov AS MATERIALIZED (
  SELECT ip_long
  FROM rb20_v2_5.h_members
  WHERE run_id='{{run_id}}'
),
final_map AS MATERIALIZED (
  SELECT ip_long, block_id_final
  FROM rb20_v2_5.map_member_block_final
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint
)
INSERT INTO rb20_v2_5.r1_members(
  run_id, contract_version, shard_id,
  ip_long, atom27_id,
  block_id_natural, block_id_final
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  km.ip_long,
  (km.ip_long / 32)::bigint AS atom27_id,
  km.block_id_natural,
  fm.block_id_final
FROM rb20_v2_5.keep_members km
LEFT JOIN final_map fm
  ON fm.ip_long=km.ip_long
LEFT JOIN h_cov h
  ON h.ip_long=km.ip_long
WHERE km.run_id='{{run_id}}' AND km.shard_id={{shard_id}}::smallint
  AND h.ip_long IS NULL;

-- StepStats（per-shard）
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_06' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_06', {{shard_id}}::smallint, 'r1_member_cnt', COUNT(*)::numeric
FROM rb20_v2_5.r1_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;
