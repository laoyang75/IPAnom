-- RB20_03 (per-shard): Pre Profile（Keep/Drop + network_tier_pre）+ PreH + Keep/Drop members
-- 依赖：RB20_02 已生成 block_natural / map_member_block_natural；RB20_01 已生成 source_members
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_id}}         0..(shard_cnt-1)

-- 注意：若执行器在同一会话内串行跑多个 step，可能遗留不利 GUC（例如之前的 enable_hashagg=off）。
-- 为避免 RB20_03 聚合极慢，这里显式恢复 hashagg。
SET enable_hashagg = on;

DELETE FROM rb20_v2_5.preh_blocks
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

DELETE FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

DELETE FROM rb20_v2_5.keep_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

DELETE FROM rb20_v2_5.drop_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

WITH m AS (
  SELECT
    map.block_id_natural,
    sm.ip_long,
    sm.is_valid,
    sm."设备数量" AS devices,
    sm."上报次数" AS reports,
    sm."移动网络设备数量" AS mobile_devices,
    sm."WiFi设备数量" AS wifi_devices,
    sm."VPN设备数量" AS vpn_devices,
    sm."有线网络设备数量" AS wired_devices,
    sm."异常网络设备数量" AS abnormal_net_devices,
    sm."空网络状态设备数量" AS empty_net_devices,
    sm."工作时上报次数" AS worktime_reports,
    sm."工作日上报次数" AS workday_reports,
    sm."周末上报次数" AS weekend_reports,
    sm."深夜上报次数" AS late_night_reports
  FROM rb20_v2_5.map_member_block_natural map
  JOIN rb20_v2_5.source_members sm
    ON sm.run_id=map.run_id AND sm.shard_id=map.shard_id AND sm.ip_long=map.ip_long
  WHERE map.run_id='{{run_id}}' AND map.shard_id={{shard_id}}::smallint
),
agg AS (
  SELECT
    block_id_natural,
    COUNT(*)::bigint AS member_cnt_total,
    COUNT(*) FILTER (WHERE is_valid)::bigint AS valid_cnt,

    COALESCE(SUM(reports),0)::bigint AS reports_sum_total,
    COALESCE(SUM(reports) FILTER (WHERE is_valid),0)::bigint AS reports_sum_valid,

    COALESCE(SUM(devices),0)::bigint AS devices_sum_total,
    COALESCE(SUM(COALESCE(devices,0)) FILTER (WHERE is_valid),0)::bigint AS devices_sum_valid,

    COALESCE(SUM(mobile_devices),0)::bigint AS mobile_devices_sum_total,
    COALESCE(SUM(mobile_devices) FILTER (WHERE is_valid),0)::bigint AS mobile_devices_sum_valid,

    COALESCE(SUM(wifi_devices),0)::bigint AS wifi_devices_sum_total,
    COALESCE(SUM(wifi_devices) FILTER (WHERE is_valid),0)::bigint AS wifi_devices_sum_valid,

    COALESCE(SUM(vpn_devices),0)::bigint AS vpn_devices_sum_total,
    COALESCE(SUM(vpn_devices) FILTER (WHERE is_valid),0)::bigint AS vpn_devices_sum_valid,

    COALESCE(SUM(wired_devices),0)::bigint AS wired_devices_sum_total,
    COALESCE(SUM(wired_devices) FILTER (WHERE is_valid),0)::bigint AS wired_devices_sum_valid,

    COALESCE(SUM(abnormal_net_devices),0)::bigint AS abnormal_net_devices_sum_total,
    COALESCE(SUM(abnormal_net_devices) FILTER (WHERE is_valid),0)::bigint AS abnormal_net_devices_sum_valid,

    COALESCE(SUM(empty_net_devices),0)::bigint AS empty_net_devices_sum_total,
    COALESCE(SUM(empty_net_devices) FILTER (WHERE is_valid),0)::bigint AS empty_net_devices_sum_valid,

    COALESCE(SUM(worktime_reports),0)::bigint AS worktime_reports_sum_total,
    COALESCE(SUM(worktime_reports) FILTER (WHERE is_valid),0)::bigint AS worktime_reports_sum_valid,

    COALESCE(SUM(workday_reports),0)::bigint AS workday_reports_sum_total,
    COALESCE(SUM(workday_reports) FILTER (WHERE is_valid),0)::bigint AS workday_reports_sum_valid,

    COALESCE(SUM(weekend_reports),0)::bigint AS weekend_reports_sum_total,
    COALESCE(SUM(weekend_reports) FILTER (WHERE is_valid),0)::bigint AS weekend_reports_sum_valid,

    COALESCE(SUM(late_night_reports),0)::bigint AS late_night_reports_sum_total,
    COALESCE(SUM(late_night_reports) FILTER (WHERE is_valid),0)::bigint AS late_night_reports_sum_valid
  FROM m
  GROUP BY 1
),
score AS (
  SELECT
    a.*,
    (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) AS density,
    (a.reports_sum_valid::numeric / NULLIF(a.valid_cnt,0)) AS report_density_valid,
    CASE
      WHEN a.valid_cnt = 0 THEN NULL
      WHEN a.valid_cnt BETWEEN 1 AND 16 THEN 1
      WHEN a.valid_cnt BETWEEN 17 AND 48 THEN 2
      WHEN a.valid_cnt BETWEEN 49 AND 128 THEN 4
      WHEN a.valid_cnt BETWEEN 129 AND 512 THEN 8
      ELSE 16
    END AS wA,
    CASE
      WHEN a.valid_cnt = 0 THEN NULL
      WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 3.5 THEN 1
      WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 6.5 THEN 2
      WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 30 THEN 4
      WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 200 THEN 16
      ELSE 32
    END AS wD
  FROM agg a
),
tier AS (
  SELECT
    s.*,
    CASE
      WHEN s.valid_cnt = 0 THEN NULL
      ELSE (s.wA + s.wD)
    END AS simple_score,
    CASE
      WHEN s.valid_cnt = 0 THEN '无效块'
      WHEN (s.wA + s.wD) >= 40 THEN '超大网络'
      WHEN (s.wA + s.wD) >= 30 THEN '大型网络'
      WHEN (s.wA + s.wD) >= 20 THEN '中型网络'
      WHEN (s.wA + s.wD) >= 10 THEN '小型网络'
      ELSE '微型网络'
    END AS network_tier_pre,
    CASE
      WHEN s.valid_cnt = 0 THEN false
      ELSE true
    END AS keep_flag,
    CASE
      WHEN s.valid_cnt = 0 THEN 'ALL_ABNORMAL_BLOCK'
      ELSE NULL
    END AS drop_reason
  FROM score s
)
INSERT INTO rb20_v2_5.profile_pre(
  run_id, contract_version, shard_id, block_id_natural,
  keep_flag, drop_reason,
  member_cnt_total, valid_cnt, devices_sum_valid, density, wA, wD, simple_score, network_tier_pre,
  reports_sum_total, reports_sum_valid, devices_sum_total,
  mobile_devices_sum_total, mobile_devices_sum_valid,
  wifi_devices_sum_total, wifi_devices_sum_valid,
  vpn_devices_sum_total, vpn_devices_sum_valid,
  wired_devices_sum_total, wired_devices_sum_valid,
  abnormal_net_devices_sum_total, abnormal_net_devices_sum_valid,
  empty_net_devices_sum_total, empty_net_devices_sum_valid,
  worktime_reports_sum_total, worktime_reports_sum_valid,
  workday_reports_sum_total, workday_reports_sum_valid,
  weekend_reports_sum_total, weekend_reports_sum_valid,
  late_night_reports_sum_total, late_night_reports_sum_valid,
  report_density_valid
)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint, block_id_natural,
  keep_flag, drop_reason,
  member_cnt_total, valid_cnt, devices_sum_valid, density, wA, wD, simple_score, network_tier_pre,
  reports_sum_total, reports_sum_valid, devices_sum_total,
  mobile_devices_sum_total, mobile_devices_sum_valid,
  wifi_devices_sum_total, wifi_devices_sum_valid,
  vpn_devices_sum_total, vpn_devices_sum_valid,
  wired_devices_sum_total, wired_devices_sum_valid,
  abnormal_net_devices_sum_total, abnormal_net_devices_sum_valid,
  empty_net_devices_sum_total, empty_net_devices_sum_valid,
  worktime_reports_sum_total, worktime_reports_sum_valid,
  workday_reports_sum_total, workday_reports_sum_valid,
  weekend_reports_sum_total, weekend_reports_sum_valid,
  late_night_reports_sum_total, late_night_reports_sum_valid,
  report_density_valid
FROM tier;

-- PreH（DP-004 选 C）：Keep 且 valid_cnt>0 且“跨 bucket64 边界”的自然块（仅减少无效评估，不改变切分定义）
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

-- KeepMembers / DropMembers（用于守恒与审计）
INSERT INTO rb20_v2_5.keep_members(run_id, contract_version, shard_id, ip_long, block_id_natural, keep_flag)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  map.ip_long, map.block_id_natural, true
FROM rb20_v2_5.map_member_block_natural map
JOIN rb20_v2_5.profile_pre p
  ON p.run_id=map.run_id AND p.shard_id=map.shard_id AND p.block_id_natural=map.block_id_natural
WHERE map.run_id='{{run_id}}' AND map.shard_id={{shard_id}}::smallint
  AND p.keep_flag;

INSERT INTO rb20_v2_5.drop_members(run_id, contract_version, shard_id, ip_long, block_id_natural, drop_reason)
SELECT
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  map.ip_long, map.block_id_natural, p.drop_reason
FROM rb20_v2_5.map_member_block_natural map
JOIN rb20_v2_5.profile_pre p
  ON p.run_id=map.run_id AND p.shard_id=map.shard_id AND p.block_id_natural=map.block_id_natural
WHERE map.run_id='{{run_id}}' AND map.shard_id={{shard_id}}::smallint
  AND NOT p.keep_flag;

-- StepStats（per-shard）
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_03' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'keep_block_cnt', COUNT(*)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND keep_flag;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'drop_block_cnt', COUNT(*)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND NOT keep_flag;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'valid_cnt_eq_0_block_cnt', COUNT(*)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt = 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'preh_block_cnt', COUNT(*)::numeric
FROM rb20_v2_5.preh_blocks
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'keep_member_cnt', COUNT(*)::numeric
FROM rb20_v2_5.keep_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'drop_member_cnt', COUNT(*)::numeric
FROM rb20_v2_5.drop_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

-- 画像统计补充：自然块的设备/上报量与密度分布（valid 口径）
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'avg_devices_sum_valid',
       AVG(devices_sum_valid)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'avg_reports_sum_valid',
       AVG(reports_sum_valid)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'device_density_valid_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY density)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'device_density_valid_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY density)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'device_density_valid_p99',
       percentile_cont(0.99) WITHIN GROUP (ORDER BY density)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'report_density_valid_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY report_density_valid)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'report_density_valid_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY report_density_valid)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_03', {{shard_id}}::smallint, 'report_density_valid_p99',
       percentile_cont(0.99) WITHIN GROUP (ORDER BY report_density_valid)::numeric
FROM rb20_v2_5.profile_pre
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;
