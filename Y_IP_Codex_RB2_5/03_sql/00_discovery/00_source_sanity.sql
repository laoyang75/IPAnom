-- RB20 v2.0 / Discovery
-- 轻量事实采样：用于 Decision Points 与合同确认（不跑大任务、不落重表）

-- W 源表行数与唯一性
SELECT
  COUNT(*) AS rows_total,
  COUNT(DISTINCT ip_long) AS distinct_ip_long
FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1";

-- W 关键字段 NULL 与基础约束
SELECT
  COUNT(*) FILTER (WHERE ip_long IS NULL) AS ip_long_null,
  COUNT(*) FILTER (WHERE ip_address IS NULL) AS ip_address_null,
  COUNT(*) FILTER (WHERE "IP归属国家" IS NULL) AS country_null,
  COUNT(*) FILTER (WHERE "设备数量" IS NULL) AS devices_null,
  COUNT(*) FILTER (WHERE "移动网络设备数量" IS NULL) AS mobile_devices_null,
  COUNT(*) FILTER (WHERE "上报次数" IS NULL) AS report_null
FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1";

-- 国家取值采样（DP-001）
SELECT "IP归属国家" AS country, COUNT(*) AS cnt
FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
GROUP BY 1
ORDER BY cnt DESC;

-- 运营商缺失情况
SELECT
  COUNT(*) FILTER (WHERE "IP归属运营商" IS NULL) AS operator_null,
  COUNT(*) FILTER (WHERE "IP归属运营商" = '') AS operator_empty,
  COUNT(DISTINCT "IP归属运营商") FILTER (WHERE "IP归属运营商" IS NOT NULL) AS operator_distinct_nonnull
FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1";

-- 异常表 NULL/唯一性（DP-002）
SELECT
  COUNT(*) FILTER (WHERE ipv4_bigint IS NULL) AS null_rows,
  COUNT(*) FILTER (WHERE ipv4_bigint IS NOT NULL) AS nonnull_rows,
  COUNT(DISTINCT ipv4_bigint) FILTER (WHERE ipv4_bigint IS NOT NULL) AS distinct_nonnull
FROM public."ip库构建项目_异常ip表_20250811_20250824_v2";

-- 移动占比语义校验：移动网络设备比例 ≈ ROUND(移动网络设备数量/设备数量,2)
WITH s AS (
  SELECT
    "移动网络设备比例"::numeric AS ratio_src,
    ROUND(("移动网络设备数量"::numeric / NULLIF("设备数量",0)), 2) AS ratio_calc
  FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
  TABLESAMPLE SYSTEM (0.1)
)
SELECT
  COUNT(*) AS sample_n,
  COUNT(*) FILTER (WHERE ratio_src = ratio_calc) AS eq_round2,
  MAX(ABS(ratio_src - ratio_calc)) AS max_abs_diff_to_round2
FROM s;

