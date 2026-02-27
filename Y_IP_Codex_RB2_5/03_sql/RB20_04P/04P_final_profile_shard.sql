-- RB20_04P (per-shard): Final Profile（network_tier_final，同口径复算）
-- 依赖：RB20_04 已生成 block_final/map_member_block_final；RB20_01 source_members
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_id}}         0..(shard_cnt-1)

DELETE FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

WITH m AS (
  SELECT
    map.block_id_final,
    map.block_id_parent,
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
  FROM rb20_v2_5.map_member_block_final map
  JOIN rb20_v2_5.source_members sm
    ON sm.run_id=map.run_id AND sm.shard_id=map.shard_id AND sm.ip_long=map.ip_long
  WHERE map.run_id='{{run_id}}' AND map.shard_id={{shard_id}}::smallint
),
agg AS (
  SELECT
    block_id_final,
    MAX(block_id_parent) AS block_id_parent,
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
    END AS network_tier_final
  FROM score s
)
INSERT INTO rb20_v2_5.profile_final(
  run_id, contract_version, shard_id,
  block_id_final, block_id_parent,
  member_cnt_total, valid_cnt, devices_sum_valid, density, wA, wD, simple_score, network_tier_final,
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
  '{{run_id}}','{{contract_version}}', {{shard_id}}::smallint,
  block_id_final, block_id_parent,
  member_cnt_total, valid_cnt, devices_sum_valid, density, wA, wD, simple_score, network_tier_final,
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

-- StepStats（per-shard）
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_04P' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_profile_block_cnt', COUNT(*)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_profile_invalid_block_cnt',
       COUNT(*) FILTER (WHERE network_tier_final='无效块')::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

-- 分布与画像补充（用于你要求的：CIDR 平均 IP 数、密度、移动覆盖/工作时占比/假日占比）
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_avg_member_cnt_total',
       AVG(member_cnt_total)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_member_cnt_total_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY member_cnt_total)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_member_cnt_total_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY member_cnt_total)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_member_cnt_total_p99',
       percentile_cont(0.99) WITHIN GROUP (ORDER BY member_cnt_total)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_member_cnt_total_max',
       MAX(member_cnt_total)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

-- 密度类（分母=IP 数量；valid_cnt=0 时为 NULL）
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_device_density_valid_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY density)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_device_density_valid_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY density)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_device_density_valid_p99',
       percentile_cont(0.99) WITHIN GROUP (ORDER BY density)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_report_density_valid_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY report_density_valid)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_report_density_valid_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY report_density_valid)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_report_density_valid_p99',
       percentile_cont(0.99) WITHIN GROUP (ORDER BY report_density_valid)::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND valid_cnt > 0;

-- 行为比例类（DP-010 / DP-009；分母=设备量或上报量）
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_mobile_device_ratio_valid_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY (mobile_devices_sum_valid::numeric / NULLIF(devices_sum_valid,0)))::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND devices_sum_valid > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_mobile_device_ratio_valid_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY (mobile_devices_sum_valid::numeric / NULLIF(devices_sum_valid,0)))::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND devices_sum_valid > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_worktime_report_ratio_valid_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY (worktime_reports_sum_valid::numeric / NULLIF(reports_sum_valid,0)))::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND reports_sum_valid > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_worktime_report_ratio_valid_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY (worktime_reports_sum_valid::numeric / NULLIF(reports_sum_valid,0)))::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND reports_sum_valid > 0;

-- 假日=周末（DP-009 固化）
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_holiday_report_ratio_valid_p50',
       percentile_cont(0.50) WITHIN GROUP (ORDER BY (weekend_reports_sum_valid::numeric / NULLIF(reports_sum_valid,0)))::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND reports_sum_valid > 0;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_04P', {{shard_id}}::smallint, 'final_holiday_report_ratio_valid_p90',
       percentile_cont(0.90) WITHIN GROUP (ORDER BY (weekend_reports_sum_valid::numeric / NULLIF(reports_sum_valid,0)))::numeric
FROM rb20_v2_5.profile_final
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND reports_sum_valid > 0;
