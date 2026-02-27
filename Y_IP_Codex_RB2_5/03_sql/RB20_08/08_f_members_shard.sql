-- RB20_08 (per-shard): F Members = R1 \ E_cov
-- 红线：F 反连接必须用 atom27_id 等值 anti-join，禁止 BETWEEN/NOT BETWEEN
--
-- 依赖：RB20_06 r1_members；RB20_07 e_atoms（is_e_atom=true 表示 E_cov 的 /27 原子）
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_id}}         0..(shard_cnt-1)

DELETE FROM rb20_v2_5.f_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.f_members(
  run_id, contract_version, shard_id,
  ip_long, atom27_id
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  r1.ip_long,
  r1.atom27_id
FROM rb20_v2_5.r1_members r1
LEFT JOIN rb20_v2_5.e_atoms ea
  ON ea.run_id=r1.run_id
 AND ea.shard_id=r1.shard_id
 AND ea.atom27_id=r1.atom27_id
 AND ea.is_e_atom
WHERE r1.run_id='{{run_id}}' AND r1.shard_id={{shard_id}}::smallint
  AND ea.atom27_id IS NULL;

-- StepStats（per-shard）
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_08' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_08', {{shard_id}}::smallint, 'f_member_cnt', COUNT(*)::numeric
FROM rb20_v2_5.f_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;
