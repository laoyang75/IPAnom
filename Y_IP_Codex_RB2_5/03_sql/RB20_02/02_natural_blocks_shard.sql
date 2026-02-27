-- RB20_02 (per-shard): Natural Block Entity + Member→Block Map
-- 依赖：RB20_01 已写入 rb20_v2_5.source_members
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_id}}         0..(shard_cnt-1)

SET work_mem = '128MB';
-- 注意：不要在会话级关闭 hashagg（会影响后续 RB20_03/RB20_04P 等聚合性能，尤其当执行器把多个 step 串在同一会话/事务里时）。

DELETE FROM rb20_v2_5.map_member_block_natural
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

DELETE FROM rb20_v2_5.block_natural
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

-- 1. 计算分组并存入临时表，避免重复窗口计算和全表扫描
CREATE TEMP TABLE tmp_member_grp AS
WITH ordered AS (
  SELECT
    ip_long,
    CASE
      WHEN LAG(ip_long) OVER (ORDER BY ip_long) = ip_long - 1 THEN 0
      ELSE 1
    END AS is_new_block
  FROM rb20_v2_5.source_members
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint
)
SELECT
  ip_long,
  SUM(is_new_block) OVER (ORDER BY ip_long) AS grp_id
FROM ordered;

CREATE INDEX idx_tmp_member_grp_id ON tmp_member_grp(grp_id);
ANALYZE tmp_member_grp;

-- 2. 聚合块信息
CREATE TEMP TABLE tmp_block_agg AS
SELECT
  grp_id,
  MIN(ip_long) AS ip_start,
  MAX(ip_long) AS ip_end,
  COUNT(*)::bigint AS member_cnt_total
FROM tmp_member_grp
GROUP BY 1;

CREATE INDEX idx_tmp_block_agg_id ON tmp_block_agg(grp_id);
ANALYZE tmp_block_agg;

-- 3. 写入 block_natural
INSERT INTO rb20_v2_5.block_natural(
  run_id, contract_version, shard_id,
  block_id_natural, ip_start, ip_end, member_cnt_total
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  ('N' || LPAD({{shard_id}}::text, 2, '0') || '_' || ip_start::text || '_' || ip_end::text),
  ip_start, ip_end, member_cnt_total
FROM tmp_block_agg;

-- 4. 写入 map_member_block_natural (使用 Index Join)
INSERT INTO rb20_v2_5.map_member_block_natural(
  run_id, contract_version, shard_id,
  ip_long, block_id_natural
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  m.ip_long,
  ('N' || LPAD({{shard_id}}::text, 2, '0') || '_' || b.ip_start::text || '_' || b.ip_end::text)
FROM tmp_member_grp m
JOIN tmp_block_agg b USING (grp_id);

-- 5. StepStats（per-shard）
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_02' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_02', {{shard_id}}::smallint, 'natural_block_cnt_total', COUNT(*)::numeric
FROM rb20_v2_5.block_natural
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_02', {{shard_id}}::smallint, 'natural_block_cnt_ge4', COUNT(*)::numeric
FROM rb20_v2_5.block_natural
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND member_cnt_total >= 4;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_02', {{shard_id}}::smallint, 'avg_member_cnt_total', AVG(member_cnt_total)::numeric
FROM rb20_v2_5.block_natural
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_02', {{shard_id}}::smallint, 'member_cnt_total_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY member_cnt_total)::numeric
FROM rb20_v2_5.block_natural
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_02', {{shard_id}}::smallint, 'member_cnt_total_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY member_cnt_total)::numeric
FROM rb20_v2_5.block_natural
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_02', {{shard_id}}::smallint, 'member_cnt_total_p99',
       percentile_cont(0.99) WITHIN GROUP (ORDER BY member_cnt_total)::numeric
FROM rb20_v2_5.block_natural
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_02', {{shard_id}}::smallint, 'member_cnt_total_max',
       MAX(member_cnt_total)::numeric
FROM rb20_v2_5.block_natural
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;
