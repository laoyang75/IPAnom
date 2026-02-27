-- RB20 v2.0 / Contracts / ShardPlan (SQL-only, DBHub compatible, FAST)
--
-- 说明：
-- - DBHub 的 execute_sql 对包含 $$ 的 plpgsql（procedure/function/DO）支持不稳定，因此此处提供纯 SQL 版本。
-- - 性能修复：原 NTILE(shard_cnt) 需要对“全量中国成员 ip_long”做排序（重任务）。
--   由于 W 表 `ip_long` 唯一，因此任意 /16（ip_long>>16）最多 65536 行；用 /16 直方图做累积“近似分位”即可极接近均衡，
--   且只需一次扫描 + GROUP BY（~65k 组），避免全量排序。
--   输出仍为连续不重叠的 ip_long 区间，且每 shard 非空（除非 shard_cnt > 非空 /16 数量）。
-- - 输出落表：rb20_v2_5.shard_plan（DP-006：rb20_v2_5）。
--
-- 使用前替换占位符：
--   {{run_id}}             例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}}   例如 contract_v1
--   {{shard_cnt}}          例如 64（或按 DP-014 选定的 shard 数量；必须 <=255）
--   {{eps}}                例如 0.10
--
-- ⚠️ 本脚本不会全量排序，但仍会扫描 W 表并做一次 GROUP BY（建议低峰运行）。

BEGIN;

CREATE SCHEMA IF NOT EXISTS rb20_v2_5;

-- 0) 幂等清理
DELETE FROM rb20_v2_5.shard_plan WHERE run_id = '{{run_id}}';

-- 1) 参数与数据范围（注意：按中国过滤；与 RB20_01 保持一致）
CREATE TEMP TABLE sp_params AS
SELECT
  '{{run_id}}'::text AS run_id,
  '{{contract_version}}'::text AS contract_version,
  {{shard_cnt}}::int AS shard_cnt,
  {{eps}}::float8 AS eps,
  COUNT(*)::bigint AS total_rows,
  MIN(ip_long)::bigint AS min_ip,
  (MAX(ip_long) + 1)::bigint AS max_excl
FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
WHERE "IP归属国家" IN ('中国');

-- 断言：至少 shard_cnt 行（否则无法分成 shard_cnt shard）
-- Note: If this query returns 0 rows, the data is insufficient for sharding
SELECT 1 AS assert_rows_ge_shard_cnt
FROM sp_params
WHERE total_rows >= shard_cnt;

-- 2) /16 直方图（ip_long>>16）
CREATE TEMP TABLE sp_hist16 AS
SELECT
  (ip_long >> 16)::int AS p16,
  COUNT(*)::bigint AS cnt
FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
WHERE "IP归属国家" IN ('中国')
GROUP BY 1
ORDER BY 1;

-- 3) 识别极端倾斜的 /16（可选兜底）：若某个 /16 行数大到可能吞掉多个 shard，
--    则在该 /16 内再按 /24（ip_long>>8）细分，避免 shard_id 缺失（分母稀疏导致断言失败）。
--    说明：当 W 表 ip_long 唯一时，/16 最多 65536 行，通常不会触发；此处是鲁棒性保险。
CREATE TEMP TABLE sp_big16 AS
SELECT h.p16
FROM sp_hist16 h
CROSS JOIN sp_params p
WHERE h.cnt::numeric > ((p.total_rows::numeric / NULLIF(p.shard_cnt,0)) * (1 + p.eps));

CREATE TEMP TABLE sp_hist24_big AS
SELECT
  (w.ip_long >> 8)::int AS p24,
  COUNT(*)::bigint AS cnt
FROM sp_big16 b
JOIN public."ip库构建项目_ip源表_20250811_20250824_v2_1" w
  ON (w.ip_long >> 16)::int = b.p16
WHERE w."IP归属国家" IN ('中国')
GROUP BY 1
ORDER BY 1;

-- 4) 混合直方图：
-- - 非 big16：按 /16 折叠成一个“伪 /24”点（p24 = p16<<8）
-- - big16：用真实 /24 细分
CREATE TEMP TABLE sp_hist AS
SELECT
  (h16.p16 << 8)::int AS p24,
  h16.cnt
FROM sp_hist16 h16
LEFT JOIN sp_big16 b USING (p16)
WHERE b.p16 IS NULL
UNION ALL
SELECT p24, cnt
FROM sp_hist24_big
ORDER BY 1;

-- 5) 给每个 bin 分配 shard_id（按累积行数均分；无需全量排序）
CREATE TEMP TABLE sp_assign AS
SELECT
  p24,
  cnt,
  (
    ((SUM(cnt) OVER (ORDER BY p24) - 1) * (SELECT shard_cnt FROM sp_params))
    / NULLIF((SELECT total_rows FROM sp_params), 0)
  )::int AS shard_id
FROM sp_hist;

-- 6) 聚合每个 shard 的起点（p24_min）与 est_rows
CREATE TEMP TABLE sp_agg AS
SELECT
  shard_id,
  MIN(p24)::int AS p24_min,
  SUM(cnt)::bigint AS cnt
FROM sp_assign
GROUP BY 1
ORDER BY 1;

-- 断言：必须正好 shard_cnt shard 且无空 shard
-- Note: If either query returns 0 rows, sharding validation failed
SELECT 1 AS assert_shard_cnt_ok
FROM sp_agg
HAVING COUNT(*) = (SELECT shard_cnt FROM sp_params);

SELECT 1 AS assert_no_empty_shard
WHERE NOT EXISTS (SELECT 1 FROM sp_agg WHERE cnt = 0);

-- 5) 生成连续不重叠区间（end 为 next shard 的 start；最后一个 end=max_excl）
CREATE TEMP TABLE sp_bounds AS
SELECT
  a.shard_id,
  (a.p24_min::bigint * 256)::bigint AS ip_long_start,
  COALESCE(
    LEAD((a.p24_min::bigint * 256)) OVER (ORDER BY a.shard_id),
    (SELECT max_excl FROM sp_params)
  )::bigint AS ip_long_end
FROM sp_agg a
ORDER BY a.shard_id;

-- 断言：区间非空（数值意义）且 start 单调递增
-- Note: If either query returns 0 rows, sharding validation failed
SELECT 1 AS assert_nonempty_range
WHERE NOT EXISTS (SELECT 1 FROM sp_bounds WHERE ip_long_start >= ip_long_end);

SELECT 1 AS assert_monotonic_start
WHERE NOT EXISTS (
  SELECT 1
  FROM (
    SELECT shard_id, ip_long_start, LAG(ip_long_start) OVER (ORDER BY shard_id) AS prev_start
    FROM sp_bounds
  ) t
  WHERE prev_start IS NOT NULL AND ip_long_start <= prev_start
);

-- 4) 写入 shard_plan（plan_round=0 表示 quantile 初始化完成；无需 5% 调整）
INSERT INTO rb20_v2_5.shard_plan(
  run_id, contract_version, shard_id,
  ip_long_start, ip_long_end,
  est_rows, plan_round, created_at
)
SELECT
  p.run_id,
  p.contract_version,
  b.shard_id::smallint,
  b.ip_long_start,
  b.ip_long_end,
  a.cnt,
  0::smallint AS plan_round,
  now()
FROM sp_bounds b
JOIN sp_agg a USING (shard_id)
CROSS JOIN sp_params p
ORDER BY b.shard_id;

COMMIT;
