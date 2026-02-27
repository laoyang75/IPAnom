-- RB20_11 (per-shard): HeadTail Window Entity（block×bucket64，k=5，valid）
-- 依赖：RB20_03 已生成 preh_blocks；RB20_01/RB20_02 已生成 source_members/map_member_block_natural/block_natural
--
-- Window 定义（按主版本 3.4.1/3.4.4）：
-- - 以 bucket64=floor(ip_long/64)，对跨 bucket 边界的候选切点 cut=(bucket64+1)*64
-- - 左窗口取 bucket64=b 内靠近边界的 k 个 valid IP（按 ip_long DESC）
-- - 右窗口取 bucket64=b+1 内靠近边界的 k 个 valid IP（按 ip_long ASC）
-- - 运营商唯一判定：distinct=1 才写入，否则 NULL
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_id}}         0..(shard_cnt-1)

DELETE FROM rb20_v2_5.window_headtail_64
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

WITH preh AS (
  SELECT block_id_natural
  FROM rb20_v2_5.preh_blocks
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint
),
base AS (
  SELECT
    map.block_id_natural,
    sm.bucket64,
    sm.ip_long,
    sm.is_valid,
    sm."上报次数" AS reports,
    sm."移动网络设备数量" AS mobile_devices,
    sm."IP归属运营商" AS operator
  FROM preh p
  JOIN rb20_v2_5.map_member_block_natural map
    ON map.run_id='{{run_id}}' AND map.shard_id={{shard_id}}::smallint AND map.block_id_natural=p.block_id_natural
  JOIN rb20_v2_5.source_members sm
    ON sm.run_id=map.run_id AND sm.shard_id=map.shard_id AND sm.ip_long=map.ip_long
),
-- 候选切点 bucket64：对每个自然块，取“出现过的 bucket64 集合”中相邻 bucket 的左侧 bucket。
-- 说明：
-- - 合同要求：当任一侧 cnt=0 时指标为 NULL，但仍要写审计行（因此 cand 必须覆盖 bucket 边界，而不能只看 valid）。
-- - 性能修复：不用 generate_series(按范围爆炸)，改为基于成员实际 bucket 集合生成 cand（自然块由连续 IP 构成，bucket 集合也会是连续/近连续）。
bucket_set AS (
  SELECT DISTINCT block_id_natural, bucket64
  FROM base
),
cand AS (
  SELECT b.block_id_natural, b.bucket64
  FROM bucket_set b
  JOIN bucket_set b2
    ON b2.block_id_natural=b.block_id_natural
   AND b2.bucket64=b.bucket64 + 1
),
-- 性能关键点：
-- - 避免对 cand(block×bucket) 逐行回扫 map_member_block_natural（会导致大量重复 join）。
-- - 一次性展开 PreH 的 valid 成员 m，再按 (block,bucket64) 做窗口/聚合。
m AS (
  SELECT
    block_id_natural,
    bucket64,
    ip_long,
    reports,
    mobile_devices,
    operator
  FROM base
  WHERE is_valid
),
left_k AS (
  SELECT
    block_id_natural,
    bucket64,
    reports,
    mobile_devices,
    operator,
    ROW_NUMBER() OVER (PARTITION BY block_id_natural, bucket64 ORDER BY ip_long DESC) AS rn
  FROM m
),
left_agg AS (
  SELECT
    block_id_natural,
    bucket64,
    COUNT(*) FILTER (WHERE rn <= 5)::smallint AS left_cnt_valid,
    COALESCE(SUM(reports) FILTER (WHERE rn <= 5),0)::bigint AS left_reports_sum_valid,
    COALESCE(SUM(mobile_devices) FILTER (WHERE rn <= 5),0)::bigint AS left_mobile_devices_sum_valid,
    COUNT(DISTINCT operator) FILTER (WHERE rn <= 5 AND operator IS NOT NULL) AS left_op_distinct,
    MAX(operator) FILTER (WHERE rn <= 5) AS left_op_any
  FROM left_k
  GROUP BY 1,2
),
right_k AS (
  SELECT
    block_id_natural,
    bucket64,
    reports,
    mobile_devices,
    operator,
    ROW_NUMBER() OVER (PARTITION BY block_id_natural, bucket64 ORDER BY ip_long ASC) AS rn
  FROM m
),
-- 右窗口是 bucket=b+1，因此这里把 bucket64 向左平移一格对齐到 cand.bucket64=b
right_agg AS (
  SELECT
    block_id_natural,
    (bucket64 - 1)::bigint AS bucket64,
    COUNT(*) FILTER (WHERE rn <= 5)::smallint AS right_cnt_valid,
    COALESCE(SUM(reports) FILTER (WHERE rn <= 5),0)::bigint AS right_reports_sum_valid,
    COALESCE(SUM(mobile_devices) FILTER (WHERE rn <= 5),0)::bigint AS right_mobile_devices_sum_valid,
    COUNT(DISTINCT operator) FILTER (WHERE rn <= 5 AND operator IS NOT NULL) AS right_op_distinct,
    MAX(operator) FILTER (WHERE rn <= 5) AS right_op_any
  FROM right_k
  GROUP BY 1,2
)
INSERT INTO rb20_v2_5.window_headtail_64(
  run_id, contract_version, shard_id,
  block_id_natural, bucket64, k,
  left_cnt_valid, right_cnt_valid,
  left_reports_sum_valid, right_reports_sum_valid,
  left_mobile_devices_sum_valid, right_mobile_devices_sum_valid,
  left_operator_unique, right_operator_unique
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  c.block_id_natural,
  c.bucket64,
  5::smallint,
  COALESCE(l.left_cnt_valid,0)::smallint,
  COALESCE(r.right_cnt_valid,0)::smallint,
  COALESCE(l.left_reports_sum_valid,0)::bigint,
  COALESCE(r.right_reports_sum_valid,0)::bigint,
  COALESCE(l.left_mobile_devices_sum_valid,0)::bigint,
  COALESCE(r.right_mobile_devices_sum_valid,0)::bigint,
  CASE WHEN COALESCE(l.left_op_distinct,0) = 1 THEN l.left_op_any ELSE NULL END,
  CASE WHEN COALESCE(r.right_op_distinct,0) = 1 THEN r.right_op_any ELSE NULL END
FROM cand c
LEFT JOIN left_agg l USING (block_id_natural, bucket64)
LEFT JOIN right_agg r USING (block_id_natural, bucket64);

-- StepStats（per-shard）
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_11' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_11', {{shard_id}}::smallint, 'window_rows_cnt', COUNT(*)::numeric
FROM rb20_v2_5.window_headtail_64
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_11', {{shard_id}}::smallint, 'window_cnt0_rows_cnt',
       COUNT(*) FILTER (WHERE left_cnt_valid=0 OR right_cnt_valid=0)::numeric
FROM rb20_v2_5.window_headtail_64
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;
