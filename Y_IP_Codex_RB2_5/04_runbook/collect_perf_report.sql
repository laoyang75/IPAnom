-- RB20 v2.0 / Performance Trial Report (read-only)
--
-- Usage:
--   psql -v ON_ERROR_STOP=1 -f Y_IP_Codex_RB2_5/04_runbook/collect_perf_report.sql -v run_id='rb20v2_YYYYMMDD_HHMMSS_sg_001'
--
-- Notes:
-- - Avoid scanning huge tables; rely on shard_plan + step_stats.

\echo '=== perf_report: basic ==='
SELECT :'run_id'::text AS run_id, now() AS generated_at;

\echo '=== perf_report: shard_plan summary ==='
SELECT
  COUNT(*) AS shard_cnt,
  MIN(shard_id) AS min_id,
  MAX(shard_id) AS max_id,
  MIN(est_rows) AS min_est_rows,
  MAX(est_rows) AS max_est_rows
FROM rb20_v2_5.shard_plan
WHERE run_id=:'run_id';

\echo '=== perf_report: step coverage (step_stats) ==='
SELECT
  step_id,
  COUNT(DISTINCT shard_id) FILTER (WHERE shard_id>=0) AS shard_done,
  MIN(created_at) AS first_at,
  MAX(created_at) AS last_at
FROM rb20_v2_5.step_stats
WHERE run_id=:'run_id'
GROUP BY 1
ORDER BY step_id;

\echo '=== perf_report: per-shard key metrics (from step_stats) ==='
WITH m AS (
  SELECT
    shard_id::int AS shard_id,
    MAX(metric_value_numeric) FILTER (WHERE step_id='RB20_01' AND metric_name='source_members_rows') AS source_rows,
    MAX(metric_value_numeric) FILTER (WHERE step_id='RB20_02' AND metric_name='natural_block_cnt_total') AS natural_blocks,
    MAX(metric_value_numeric) FILTER (WHERE step_id='RB20_03' AND metric_name='preh_block_cnt') AS preh_blocks,
    MAX(metric_value_numeric) FILTER (WHERE step_id='RB20_11' AND metric_name='window_rows_cnt') AS window_rows,
    MAX(metric_value_numeric) FILTER (WHERE step_id='RB20_04' AND metric_name='split_events_cnt') AS split_events,
    MAX(metric_value_numeric) FILTER (WHERE step_id='RB20_04' AND metric_name='cut_cnt') AS cut_cnt,
    MAX(metric_value_numeric) FILTER (WHERE step_id='RB20_04P' AND metric_name='final_profile_block_cnt') AS final_profile_blocks
  FROM rb20_v2_5.step_stats
  WHERE run_id=:'run_id' AND shard_id>=0
  GROUP BY 1
)
SELECT *
FROM m
ORDER BY source_rows DESC NULLS LAST, shard_id
LIMIT 30;

\echo '=== perf_report: missing shards for critical steps ==='
WITH sp AS (
  SELECT shard_id::int AS shard_id
  FROM rb20_v2_5.shard_plan
  WHERE run_id=:'run_id'
),
done AS (
  SELECT step_id, shard_id::int AS shard_id
  FROM rb20_v2_5.step_stats
  WHERE run_id=:'run_id' AND shard_id>=0
  GROUP BY 1,2
)
SELECT
  step_id,
  STRING_AGG(sp.shard_id::text, ',' ORDER BY sp.shard_id) AS missing_shards
FROM (VALUES ('RB20_01'),('RB20_02'),('RB20_03'),('RB20_11'),('RB20_04'),('RB20_04P')) v(step_id)
JOIN sp ON true
LEFT JOIN done d ON d.step_id=v.step_id AND d.shard_id=sp.shard_id
WHERE d.shard_id IS NULL
GROUP BY 1
ORDER BY 1;

