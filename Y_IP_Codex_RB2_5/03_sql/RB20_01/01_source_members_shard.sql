-- RB20_01 (per-shard): Source Members（中国过滤 + abnormal 标记，只标记不删除）
-- 依赖：rb20_v2_5.shard_plan 已生成；rb20_v2_5.abnormal_dedup 已生成（RB20_01A）
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
DELETE FROM rb20_v2_5.source_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

WITH sp AS (
  SELECT ip_long_start, ip_long_end
  FROM rb20_v2_5.shard_plan
  WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint
)
INSERT INTO rb20_v2_5.source_members(
  run_id, contract_version, shard_id,
  ip_long, ip_address, "IP归属国家", "IP归属运营商",
  "过滤前上报次数", "上报次数", "过滤前设备数量", "设备数量",
  "应用数量", "活跃天数", "安卓ID数量", "OAID数量", "谷歌ID数量", "启动ID数量",
  "型号数量", "制造商数量",
  "深夜上报次数", "工作时上报次数", "工作日上报次数", "周末上报次数",
  "以太网接口上报次数", "代理上报次数", "Root设备上报次数", "ADB调试上报次数", "充电状态上报次数",
  "单设备最大上报次数", "DAA业务上报次数", "DNA业务上报次数", "WiFi可比上报次数",
  "SSID去重数", "BSSID去重数", "网关存在上报次数",
  "平均每设备上报次数", "周活跃天数比例", "深夜活动比例", "工作日周末平均比例",
  "平均每设备重启次数", "平均每设备应用数", "DAA DNA业务比例", "上报应用比例", "低安卓API设备比例",
  "WiFi设备数量", "WiFi设备比例",
  "移动网络设备数量", "移动网络设备比例",
  "VPN设备数量", "VPN设备比例",
  "空网络状态设备数量", "空网络状态设备比例",
  "异常网络设备数量", "异常网络设备比例",
  "有线网络设备数量", "有线网络设备比例",
  "SIM不可用比例", "无效总流量设备比例", "零移动流量设备比例",
  "制造商分布风险状态", "SDK版本分布异常分数",
  "开始日期", "结束日期", "创建时间", "活跃日期列表", "IP稳定性",
  is_abnormal, is_valid, atom27_id, bucket64
)
SELECT
  '{{run_id}}'::text AS run_id,
  '{{contract_version}}'::text AS contract_version,
  {{shard_id}}::smallint AS shard_id,

  w.ip_long,
  w.ip_address,
  w."IP归属国家",
  w."IP归属运营商",
  w."过滤前上报次数",
  w."上报次数",
  w."过滤前设备数量",
  w."设备数量",
  w."应用数量",
  w."活跃天数",
  w."安卓ID数量",
  w."OAID数量",
  w."谷歌ID数量",
  w."启动ID数量",
  w."型号数量",
  w."制造商数量",
  w."深夜上报次数",
  w."工作时上报次数",
  w."工作日上报次数",
  w."周末上报次数",
  w."以太网接口上报次数",
  w."代理上报次数",
  w."Root设备上报次数",
  w."ADB调试上报次数",
  w."充电状态上报次数",
  w."单设备最大上报次数",
  w."DAA业务上报次数",
  w."DNA业务上报次数",
  w."WiFi可比上报次数",
  w."SSID去重数",
  w."BSSID去重数",
  w."网关存在上报次数",
  w."平均每设备上报次数",
  w."周活跃天数比例",
  w."深夜活动比例",
  w."工作日周末平均比例",
  w."平均每设备重启次数",
  w."平均每设备应用数",
  w."DAA DNA业务比例",
  w."上报应用比例",
  w."低安卓API设备比例",
  w."WiFi设备数量",
  w."WiFi设备比例",
  w."移动网络设备数量",
  w."移动网络设备比例",
  w."VPN设备数量",
  w."VPN设备比例",
  w."空网络状态设备数量",
  w."空网络状态设备比例",
  w."异常网络设备数量",
  w."异常网络设备比例",
  w."有线网络设备数量",
  w."有线网络设备比例",
  w."SIM不可用比例",
  w."无效总流量设备比例",
  w."零移动流量设备比例",
  w."制造商分布风险状态",
  w."SDK版本分布异常分数",
  w."开始日期",
  w."结束日期",
  w."创建时间",
  w."活跃日期列表",
  w."IP稳定性",

  (a.ip_long IS NOT NULL) AS is_abnormal,
  (a.ip_long IS NULL) AS is_valid,
  (w.ip_long / 32)::bigint AS atom27_id,
  (w.ip_long / 64)::bigint AS bucket64
FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1" w
CROSS JOIN sp
LEFT JOIN rb20_v2_5.abnormal_dedup a
  ON a.run_id='{{run_id}}' AND a.ip_long=w.ip_long
WHERE w.ip_long >= sp.ip_long_start
  AND w.ip_long < sp.ip_long_end
  AND w."IP归属国家" IN ('中国');

-- StepStats（per-shard）
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_01' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_01', {{shard_id}}::smallint, 'source_members_rows', COUNT(*)::numeric
FROM rb20_v2_5.source_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_01', {{shard_id}}::smallint, 'source_members_abnormal_rows', COUNT(*)::numeric
FROM rb20_v2_5.source_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND is_abnormal;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_01', {{shard_id}}::smallint, 'source_members_valid_rows', COUNT(*)::numeric
FROM rb20_v2_5.source_members
WHERE run_id='{{run_id}}' AND shard_id={{shard_id}}::smallint AND is_valid;
