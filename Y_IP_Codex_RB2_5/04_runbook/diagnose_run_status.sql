-- RB20 v2.0 / Diagnose Run Status (read-only)
--
-- 使用前替换：
--   {{run_id}} 例如 rb20v2_20260107_194500_sg_001
--
-- 目的：
-- - 不扫大表，不做全表 DISTINCT；只用 shard_plan + step_stats（轻量）判断“卡在哪里/漏了哪些 shard”
-- - 方便执行 agent 快速回传：缺失 shard 列表 + top hotspot shard 概览

WITH p AS (SELECT '{{run_id}}'::text AS run_id),
sp AS (
  SELECT shard_id::int, est_rows::bigint
  FROM rb20_v2_5.shard_plan
  WHERE run_id=(SELECT run_id FROM p)
),
done AS (
  SELECT step_id, shard_id::int, MAX(created_at) AS last_at
  FROM rb20_v2_5.step_stats
  WHERE run_id=(SELECT run_id FROM p) AND shard_id >= 0
  GROUP BY 1,2
),
missing AS (
  SELECT
    sp.shard_id,
    sp.est_rows,
    (d01.shard_id IS NOT NULL) AS has_01,
    (d02.shard_id IS NOT NULL) AS has_02,
    (d03.shard_id IS NOT NULL) AS has_03,
    (d11.shard_id IS NOT NULL) AS has_11,
    (d04.shard_id IS NOT NULL) AS has_04,
    (d4p.shard_id IS NOT NULL) AS has_04p,
    (d06.shard_id IS NOT NULL) AS has_06,
    (d07.shard_id IS NOT NULL) AS has_07,
    (d08.shard_id IS NOT NULL) AS has_08
  FROM sp
  LEFT JOIN done d01 ON d01.step_id='RB20_01' AND d01.shard_id=sp.shard_id
  LEFT JOIN done d02 ON d02.step_id='RB20_02' AND d02.shard_id=sp.shard_id
  LEFT JOIN done d03 ON d03.step_id='RB20_03' AND d03.shard_id=sp.shard_id
  LEFT JOIN done d11 ON d11.step_id='RB20_11' AND d11.shard_id=sp.shard_id
  LEFT JOIN done d04 ON d04.step_id='RB20_04' AND d04.shard_id=sp.shard_id
  LEFT JOIN done d4p ON d4p.step_id='RB20_04P' AND d4p.shard_id=sp.shard_id
  LEFT JOIN done d06 ON d06.step_id='RB20_06' AND d06.shard_id=sp.shard_id
  LEFT JOIN done d07 ON d07.step_id='RB20_07' AND d07.shard_id=sp.shard_id
  LEFT JOIN done d08 ON d08.step_id='RB20_08' AND d08.shard_id=sp.shard_id
)
SELECT
  'summary' AS section,
  (SELECT COUNT(*) FROM sp) AS shard_cnt,
  (SELECT COUNT(*) FROM done WHERE step_id='RB20_01') AS done_01_cnt,
  (SELECT COUNT(*) FROM done WHERE step_id='RB20_02') AS done_02_cnt,
  (SELECT COUNT(*) FROM done WHERE step_id='RB20_03') AS done_03_cnt,
  (SELECT COUNT(*) FROM done WHERE step_id='RB20_11') AS done_11_cnt,
  (SELECT COUNT(*) FROM done WHERE step_id='RB20_04') AS done_04_cnt,
  (SELECT COUNT(*) FROM done WHERE step_id='RB20_04P') AS done_04p_cnt,
  (SELECT COUNT(*) FROM done WHERE step_id='RB20_06') AS done_06_cnt,
  (SELECT COUNT(*) FROM done WHERE step_id='RB20_07') AS done_07_cnt,
  (SELECT COUNT(*) FROM done WHERE step_id='RB20_08') AS done_08_cnt
UNION ALL
SELECT
  'missing_shards' AS section,
  shard_id,
  est_rows,
  (CASE WHEN has_01 THEN 1 ELSE 0 END),
  (CASE WHEN has_02 THEN 1 ELSE 0 END),
  (CASE WHEN has_03 THEN 1 ELSE 0 END),
  (CASE WHEN has_11 THEN 1 ELSE 0 END),
  (CASE WHEN has_04 THEN 1 ELSE 0 END),
  (CASE WHEN has_04p THEN 1 ELSE 0 END),
  (CASE WHEN has_06 THEN 1 ELSE 0 END),
  (CASE WHEN has_07 THEN 1 ELSE 0 END),
  (CASE WHEN has_08 THEN 1 ELSE 0 END)
FROM missing
WHERE NOT (has_01 AND has_02 AND has_03 AND has_11 AND has_04 AND has_04p AND has_06 AND has_07 AND has_08)
ORDER BY section, 2;

