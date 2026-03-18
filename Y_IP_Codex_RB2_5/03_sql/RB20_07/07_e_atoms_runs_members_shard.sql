-- RB20_07 (per-shard): E Atoms + E Runs + E Members
-- 依赖：RB20_06 已生成 r1_members；RB20_01 已生成 source_members
--
-- 口径：
-- - atom27_id = floor(ip_long/32)
-- - valid_ip_cnt：在 R1 中按 is_valid=true 计数
-- - is_e_atom：valid_ip_cnt >= 7（密度 >= 0.2）
-- - run：连续 is_e_atom 的 atom27_id 片段；run_len<3 标记 short_run 但保留
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_id}}         0..(shard_cnt-1)

DELETE FROM rb20_v2_5.e_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

DELETE FROM rb20_v2_5.e_runs
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

DELETE FROM rb20_v2_5.e_atoms
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

SET enable_nestloop = off;
SET work_mem = '256MB';
SET statement_timeout = '15min';

-- 1) E Atoms（先产出全量 atoms，再标记 is_e_atom）
WITH r1 AS (
  SELECT ip_long, atom27_id
  FROM rb20_v2_5.r1_members
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint
),
r1v AS (
  SELECT
    r1.atom27_id,
    sm.is_valid
  FROM r1
  JOIN rb20_v2_5.source_members sm
    ON sm.run_id='{{run_id}}' AND sm.shard_id={{shard_id}}::smallint AND sm.ip_long=r1.ip_long
),
agg AS (
  SELECT
    atom27_id,
    COUNT(*) FILTER (WHERE is_valid)::integer AS valid_ip_cnt
  FROM r1v
  GROUP BY 1
)
INSERT INTO rb20_v2_5.e_atoms(
  run_id, contract_version, shard_id,
  atom27_id, ip_start, ip_end,
  valid_ip_cnt, atom_density, is_e_atom
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  atom27_id,
  (atom27_id * 32)::bigint AS ip_start,
  (atom27_id * 32 + 31)::bigint AS ip_end,
  valid_ip_cnt,
  (valid_ip_cnt::numeric / 32.0) AS atom_density,
  (valid_ip_cnt >= 7) AS is_e_atom
FROM agg;

-- 2) E Runs（连续 is_e_atom 的 atom27_id）
WITH ea AS (
  SELECT atom27_id
  FROM rb20_v2_5.e_atoms
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND is_e_atom
),
seq AS (
  SELECT
    atom27_id,
    (atom27_id - ROW_NUMBER() OVER (ORDER BY atom27_id)) AS grp
  FROM ea
),
runs AS (
  SELECT
    MIN(atom27_id)::bigint AS atom27_start,
    MAX(atom27_id)::bigint AS atom27_end,
    COUNT(*)::integer AS run_len
  FROM seq
  GROUP BY grp
)
INSERT INTO rb20_v2_5.e_runs(
  run_id, contract_version, shard_id,
  e_run_id, atom27_start, atom27_end,
  run_len, short_run,
  ip_start, ip_end
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  ('E' || LPAD({{shard_id}}::text, 2, '0') || '_' || atom27_start::text || '_' || atom27_end::text) AS e_run_id,
  atom27_start,
  atom27_end,
  run_len,
  (run_len < 3) AS short_run,
  (atom27_start * 32)::bigint AS ip_start,
  (atom27_end * 32 + 31)::bigint AS ip_end
FROM runs;

-- 3) E Members（R1 中落在 E Runs 覆盖的 atom27 段，额外排除 H 库成员防止交叉）
WITH r1 AS (
  SELECT ip_long, atom27_id
  FROM rb20_v2_5.r1_members
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint
    AND NOT EXISTS (
      SELECT 1 FROM rb20_v2_5.h_members hm
      WHERE hm.run_id='{{run_id}}' AND hm.ip_long=r1_members.ip_long
    )
),
atom_to_run AS (
  -- 先把 /27 原子映射到 run（行数≈e_atom_cnt_pass），再与 r1 做等值 join，避免 70w×range 的大范围 join
  SELECT
    ea.atom27_id,
    er.e_run_id
  FROM rb20_v2_5.e_atoms ea
  JOIN rb20_v2_5.e_runs er
    ON er.run_id=ea.run_id AND er.shard_id=ea.shard_id
   AND ea.atom27_id BETWEEN er.atom27_start AND er.atom27_end
  WHERE ea.run_id='{{run_id}}' AND ea.shard_id={{shard_id}}::smallint
    AND ea.is_e_atom
    AND NOT er.short_run
)
INSERT INTO rb20_v2_5.e_members(
  run_id, contract_version, shard_id,
  ip_long, atom27_id, e_run_id
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  r1.ip_long,
  r1.atom27_id,
  atr.e_run_id
FROM r1
JOIN atom_to_run atr
  ON atr.atom27_id=r1.atom27_id;

-- StepStats（per-shard）
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_07' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_07', {{shard_id}}::smallint, 'e_atom_cnt_total', COUNT(*)::numeric
FROM rb20_v2_5.e_atoms
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_07', {{shard_id}}::smallint, 'e_atom_cnt_pass', COUNT(*) FILTER (WHERE is_e_atom)::numeric
FROM rb20_v2_5.e_atoms
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_07', {{shard_id}}::smallint, 'e_run_cnt_total', COUNT(*)::numeric
FROM rb20_v2_5.e_runs
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_07', {{shard_id}}::smallint, 'e_short_run_cnt', COUNT(*) FILTER (WHERE short_run)::numeric
FROM rb20_v2_5.e_runs
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_07', {{shard_id}}::smallint, 'e_run_len_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY run_len)::numeric
FROM rb20_v2_5.e_runs
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_07', {{shard_id}}::smallint, 'e_run_len_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY run_len)::numeric
FROM rb20_v2_5.e_runs
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_07', {{shard_id}}::smallint, 'e_run_len_p99',
       percentile_cont(0.99) WITHIN GROUP (ORDER BY run_len)::numeric
FROM rb20_v2_5.e_runs
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_07', {{shard_id}}::smallint, 'e_run_len_max',
       MAX(run_len)::numeric
FROM rb20_v2_5.e_runs
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_07', {{shard_id}}::smallint, 'e_member_cnt', COUNT(*)::numeric
FROM rb20_v2_5.e_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;
