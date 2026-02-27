-- RB20_01A (global): Abnormal Dedup
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1

DELETE FROM rb20_v2_5.abnormal_dedup
WHERE run_id = '{{run_id}}';

INSERT INTO rb20_v2_5.abnormal_dedup(run_id, contract_version, ip_long)
SELECT
  '{{run_id}}'::text AS run_id,
  '{{contract_version}}'::text AS contract_version,
  ipv4_bigint::bigint AS ip_long
FROM public."ip库构建项目_异常ip表_20250811_20250824_v2"
WHERE ipv4_bigint IS NOT NULL;

-- StepStats
DELETE FROM rb20_v2_5.step_stats
WHERE run_id='{{run_id}}' AND step_id='RB20_01A' AND shard_id = (-1)::smallint;

INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','RB20_01A', (-1)::smallint, 'abnormal_dedup_rows', COUNT(*)::numeric
FROM rb20_v2_5.abnormal_dedup
WHERE run_id='{{run_id}}';
