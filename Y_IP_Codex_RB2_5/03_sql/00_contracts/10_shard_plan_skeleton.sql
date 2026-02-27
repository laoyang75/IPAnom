-- RB20 v2.0 / Contracts / ShardPlan Skeleton
-- 目的：生成 64 shard 的 ip_long 区间（初始等分 + 两轮 5% 调整）。
-- 注意：两轮 5% 调整的判定/移动规则需先确认 DP-012。

-- 0) 输入锚点（唯一）
-- public."ip库构建项目_ip源表_20250811_20250824_v2_1"

-- 1) 取 ip_long 全局范围
WITH bounds AS (
  SELECT
    MIN(ip_long) AS min_ip_long,
    MAX(ip_long) AS max_ip_long
  FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
),
params AS (
  SELECT
    min_ip_long,
    max_ip_long,
    (max_ip_long - min_ip_long + 1) AS span,
    CEIL((max_ip_long - min_ip_long + 1)::numeric / 64) AS step
  FROM bounds
)
SELECT * FROM params;

-- 2) 初始按范围等分（round=0）
-- 这里输出 shard_id, ip_long_start(含), ip_long_end(不含)
WITH params AS (
  SELECT
    MIN(ip_long) AS min_ip_long,
    MAX(ip_long) AS max_ip_long,
    CEIL((MAX(ip_long) - MIN(ip_long) + 1)::numeric / 64) AS step
  FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
),
seed AS (
  SELECT
    gs AS shard_id,
    (min_ip_long + gs*step)::bigint AS ip_long_start,
    LEAST((min_ip_long + (gs+1)*step)::bigint, (max_ip_long + 1)::bigint) AS ip_long_end
  FROM params, generate_series(0,63) gs
)
SELECT * FROM seed ORDER BY shard_id;

-- 3) 统计每 shard 行数（单次扫描：按 ip_long 映射到 shard_id）
-- 注意：这一步会扫 W 源表一次（~6000 万行）；属于 Gate-0 可接受的统计，但不应频繁重复。
WITH params AS (
  SELECT
    MIN(ip_long) AS min_ip_long,
    CEIL((MAX(ip_long) - MIN(ip_long) + 1)::numeric / 64) AS step
  FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
),
shard_cnt AS (
  SELECT
    LEAST(63, FLOOR((ip_long - (SELECT min_ip_long FROM params))::numeric / (SELECT step FROM params))::int) AS shard_id,
    COUNT(*) AS cnt
  FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
  GROUP BY 1
)
SELECT * FROM shard_cnt ORDER BY shard_id;

-- 4) 两轮 5% 调整：DP-012 确认后实现
-- 建议实现策略：边界只在相邻 shard 之间移动，保持无重叠/不断档；任何空 shard 或 start>=end ⇒ STOP 并重生成。

-- 已提供可执行实现：见 `10_shard_plan.sql`
