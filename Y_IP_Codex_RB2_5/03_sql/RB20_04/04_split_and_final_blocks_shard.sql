-- RB20_04 (per-shard): SplitEvents + Final Block Entity + Member→FinalBlock Map
-- 依赖：RB20_11 window_headtail_64；RB20_03 profile_pre/preh_blocks；RB20_02 block_natural/map_member_block_natural；RB20_01 source_members
--
-- Step64 指标定义：DP-013（见 metric_contract_draft_v1.md 第 7 节）
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_id}}         0..(shard_cnt-1)

-- 幂等清理
DELETE FROM rb20_v2_5.split_events_64
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

DELETE FROM rb20_v2_5.map_member_block_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

DELETE FROM rb20_v2_5.block_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

-- 1) SplitEvents（对 PreH 候选块的跨 bucket64 边界生成一行审计记录）
WITH preh AS (
  SELECT block_id_natural
  FROM rb20_v2_5.preh_blocks
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint
),
w AS (
  SELECT *
  FROM rb20_v2_5.window_headtail_64
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint
),
-- 优化：避免按 w(block×bucket) 重复扫描整块成员；先一次性展开 PreH 成员，再按 (block,bucket64) 取 k=5
m AS (
  SELECT
    map.block_id_natural,
    sm.bucket64,
    sm.ip_long,
    sm."上报次数"::numeric AS reports,
    sm."设备数量"::numeric AS devices,
    sm."移动网络设备数量"::numeric AS mobile_devices
  FROM preh p
  JOIN rb20_v2_5.map_member_block_natural map
    ON map.run_id='{{run_id}}' AND map.shard_id={{shard_id}}::smallint AND map.block_id_natural=p.block_id_natural
  JOIN rb20_v2_5.source_members sm
    ON sm.run_id=map.run_id AND sm.shard_id=map.shard_id AND sm.ip_long=map.ip_long
  WHERE sm.is_valid
),
left_k AS (
  SELECT
    block_id_natural,
    bucket64,
    reports,
    devices,
    mobile_devices,
    ROW_NUMBER() OVER (PARTITION BY block_id_natural, bucket64 ORDER BY ip_long DESC) AS rn
  FROM m
),
right_k AS (
  SELECT
    block_id_natural,
    bucket64,
    reports,
    devices,
    mobile_devices,
    ROW_NUMBER() OVER (PARTITION BY block_id_natural, bucket64 ORDER BY ip_long ASC) AS rn
  FROM m
),
left_stats AS (
  SELECT
    block_id_natural,
    bucket64,
    COUNT(*) FILTER (WHERE rn <= 5) AS cnt,
    AVG(reports) FILTER (WHERE rn <= 5) AS mean_reports,
    STDDEV_SAMP(reports) FILTER (WHERE rn <= 5) AS sd_reports,
    SUM(mobile_devices) FILTER (WHERE rn <= 5) AS sum_mobile,
    SUM(devices) FILTER (WHERE rn <= 5) AS sum_devices
  FROM left_k
  GROUP BY 1,2
),
right_stats AS (
  SELECT
    block_id_natural,
    bucket64,
    COUNT(*) FILTER (WHERE rn <= 5) AS cnt,
    AVG(reports) FILTER (WHERE rn <= 5) AS mean_reports,
    STDDEV_SAMP(reports) FILTER (WHERE rn <= 5) AS sd_reports,
    SUM(mobile_devices) FILTER (WHERE rn <= 5) AS sum_mobile,
    SUM(devices) FILTER (WHERE rn <= 5) AS sum_devices
  FROM right_k
  GROUP BY 1,2
),
metrics AS (
  SELECT
    w.block_id_natural,
    w.bucket64,
    ((w.bucket64 + 1) * 64)::bigint AS cut_ip_long,

    w.left_cnt_valid AS cntL_valid,
    w.right_cnt_valid AS cntR_valid,

    -- DP-013 (A): Report
    CASE
      WHEN COALESCE(ls.cnt,0)=0 OR COALESCE(rs.cnt,0)=0 THEN NULL
      ELSE GREATEST(rs.mean_reports / NULLIF(ls.mean_reports,0), ls.mean_reports / NULLIF(rs.mean_reports,0))
    END AS ratio_report,
    CASE
      WHEN COALESCE(ls.cnt,0)=0 THEN NULL
      ELSE (ls.sd_reports / NULLIF(ls.mean_reports,0))
    END AS cvL,
    CASE
      WHEN COALESCE(rs.cnt,0)=0 THEN NULL
      ELSE (rs.sd_reports / NULLIF(rs.mean_reports,0))
    END AS cvR,

    -- DP-013 (A): Mobile
    CASE
      WHEN COALESCE(ls.cnt,0)=0 OR COALESCE(rs.cnt,0)=0 THEN NULL
      ELSE ABS( (rs.sum_mobile / NULLIF(rs.sum_devices,0)) - (ls.sum_mobile / NULLIF(ls.sum_devices,0)) )
    END AS mobile_diff,
    CASE
      WHEN COALESCE(ls.cnt,0)=0 OR COALESCE(rs.cnt,0)=0 THEN NULL
      ELSE GREATEST(rs.sum_mobile / NULLIF(ls.sum_mobile,0), ls.sum_mobile / NULLIF(rs.sum_mobile,0))
    END AS mobile_cnt_ratio,

    w.left_operator_unique AS opL,
    w.right_operator_unique AS opR
  FROM w
  JOIN preh p ON p.block_id_natural=w.block_id_natural
  LEFT JOIN left_stats ls
    ON ls.block_id_natural=w.block_id_natural AND ls.bucket64=w.bucket64
  LEFT JOIN right_stats rs
    ON rs.block_id_natural=w.block_id_natural AND rs.bucket64=(w.bucket64 + 1)
),
flags AS (
  SELECT
    m.*,
    (m.ratio_report > 4 AND m.cvL < 1.1 AND m.cvR < 1.1) AS trigger_report,
    (m.mobile_diff > 0.5 OR m.mobile_cnt_ratio > 4) AS trigger_mobile,
    (m.opL IS NOT NULL AND m.opR IS NOT NULL AND m.opL <> m.opR) AS trigger_operator
  FROM metrics m
)
INSERT INTO rb20_v2_5.split_events_64(
  run_id, contract_version, shard_id,
  block_id_natural, bucket64, cut_ip_long,
  cntL_valid, cntR_valid,
  ratio_report, cvL, cvR,
  mobile_diff, mobile_cnt_ratio,
  opL, opR,
  trigger_report, trigger_mobile, trigger_operator,
  is_cut
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  block_id_natural, bucket64, cut_ip_long,
  cntL_valid, cntR_valid,
  ratio_report, cvL, cvR,
  mobile_diff, mobile_cnt_ratio,
  opL, opR,
  COALESCE(trigger_report,false),
  COALESCE(trigger_mobile,false),
  COALESCE(trigger_operator,false),
  COALESCE(trigger_report,false) OR COALESCE(trigger_mobile,false) OR COALESCE(trigger_operator,false)
FROM flags;

-- 2) Final Blocks：对所有 keep 自然块产出最终块（PreH 可多段；非 PreH 默认 1 段）
WITH keep_blocks AS (
  SELECT
    p.block_id_natural,
    bn.ip_start,
    bn.ip_end
  FROM rb20_v2_5.profile_pre p
  JOIN rb20_v2_5.block_natural bn
    ON bn.run_id=p.run_id AND bn.shard_id=p.shard_id AND bn.block_id_natural=p.block_id_natural
  WHERE p.run_id='{{run_id}}' AND p.shard_id={{shard_id}}::smallint AND p.keep_flag
),
cuts AS (
  SELECT
    se.block_id_natural,
    se.cut_ip_long::bigint AS cut_ip_long
  FROM rb20_v2_5.split_events_64 se
  WHERE se.run_id='{{run_id}}' AND se.shard_id={{shard_id}}::smallint AND se.is_cut
),
points AS (
  SELECT block_id_natural, ip_start AS pt FROM keep_blocks
  UNION ALL
  SELECT block_id_natural, cut_ip_long AS pt FROM cuts
  UNION ALL
  SELECT block_id_natural, (ip_end + 1) AS pt FROM keep_blocks
),
ordered AS (
  SELECT
    block_id_natural,
    pt,
    ROW_NUMBER() OVER (PARTITION BY block_id_natural ORDER BY pt) AS rn
  FROM points
),
segs AS (
  SELECT
    block_id_natural AS block_id_parent,
    pt AS seg_start,
    (LEAD(pt) OVER (PARTITION BY block_id_natural ORDER BY pt) - 1) AS seg_end,
    ROW_NUMBER() OVER (PARTITION BY block_id_natural ORDER BY pt) AS segment_seq
  FROM ordered
),
segs2 AS (
  SELECT
    block_id_parent,
    segment_seq,
    seg_start::bigint AS ip_start,
    seg_end::bigint AS ip_end
  FROM segs
  WHERE seg_end IS NOT NULL AND seg_start <= seg_end
)
INSERT INTO rb20_v2_5.block_final(
  run_id, contract_version, shard_id,
  block_id_final, block_id_parent, segment_seq,
  ip_start, ip_end, member_cnt_total
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  (s.block_id_parent || '_' || LPAD(s.segment_seq::text, 3, '0')) AS block_id_final,
  s.block_id_parent,
  s.segment_seq,
  s.ip_start,
  s.ip_end,
  (s.ip_end - s.ip_start + 1)::bigint AS member_cnt_total
FROM segs2 s;

-- 3) Member → FinalBlock map（仅 KeepMembers；Drop 不进入 final）
WITH keep_blocks AS (
  SELECT block_id_natural
  FROM rb20_v2_5.profile_pre
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND keep_flag
),
members AS (
  SELECT map.ip_long, map.block_id_natural AS block_id_parent
  FROM rb20_v2_5.map_member_block_natural map
  JOIN keep_blocks kb ON kb.block_id_natural=map.block_id_natural
  WHERE map.run_id='{{run_id}}' AND map.shard_id={{shard_id}}::smallint
)
INSERT INTO rb20_v2_5.map_member_block_final(
  run_id, contract_version, shard_id,
  ip_long, block_id_final, block_id_parent
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  m.ip_long,
  bf.block_id_final,
  m.block_id_parent
FROM members m
JOIN rb20_v2_5.block_final bf
  ON bf.run_id='{{run_id}}' AND bf.shard_id={{shard_id}}::smallint
 AND bf.block_id_parent=m.block_id_parent
 AND m.ip_long >= bf.ip_start AND m.ip_long <= bf.ip_end;

-- StepStats（per-shard）
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_04' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04', {{shard_id}}::smallint, 'split_events_cnt', COUNT(*)::numeric
FROM rb20_v2_5.split_events_64
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04', {{shard_id}}::smallint, 'split_events_cnt0_cnt',
       COUNT(*) FILTER (WHERE cntL_valid=0 OR cntR_valid=0)::numeric
FROM rb20_v2_5.split_events_64
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04', {{shard_id}}::smallint, 'cut_cnt', COUNT(*)::numeric
FROM rb20_v2_5.split_events_64
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND is_cut;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04', {{shard_id}}::smallint, 'final_block_cnt', COUNT(*)::numeric
FROM rb20_v2_5.block_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;
