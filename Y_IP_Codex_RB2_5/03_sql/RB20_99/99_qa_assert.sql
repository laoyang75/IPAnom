-- RB20_99 (global): QA_Assert（severity=STOP）
-- 依赖：RB20_03 keep_members/drop_members；RB20_05 h_*；RB20_07 e_*；RB20_08 f_*；RB20_04 split_events_64；RB20_04P profile_final
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_cnt}}        例如 64（或按 DP-014 选定的 shard 数量）

DELETE FROM rb20_v2_5.qa_assert
WHERE run_id='{{run_id}}';

SET enable_nestloop = off;
SET work_mem = '256MB';
SET statement_timeout = '15min';

-- 1) H/E/F 两两交集=0
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'no_overlap_h_e' AS assert_name,
  'STOP' AS severity,
  (cnt = 0) AS pass_flag,
  ('overlap_cnt=' || cnt::text) AS details
FROM (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.h_members hm
  JOIN rb20_v2_5.e_members em
    ON em.run_id=hm.run_id AND em.ip_long=hm.ip_long
  WHERE hm.run_id='{{run_id}}'
) t;

INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'no_overlap_h_f',
  'STOP',
  (cnt = 0),
  ('overlap_cnt=' || cnt::text)
FROM (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.h_members hm
  JOIN rb20_v2_5.f_members fm
    ON fm.run_id=hm.run_id AND fm.ip_long=hm.ip_long
  WHERE hm.run_id='{{run_id}}'
) t;

INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'no_overlap_e_f',
  'STOP',
  (cnt = 0),
  ('overlap_cnt=' || cnt::text)
FROM (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.e_members em
  JOIN rb20_v2_5.f_members fm
    ON fm.run_id=em.run_id AND fm.ip_long=em.ip_long
  WHERE em.run_id='{{run_id}}'
) t;

-- 2) 守恒：KeepMembers = H_cov ∪ E_cov ∪ F
-- 使用分层计数，不再对 keep_members 做三次大反连接：
--   keep = h + r1
--   r1   = e + f
--   => keep = h + e + f
WITH cnts AS (
  SELECT
    COALESCE((
      SELECT SUM(metric_value_numeric)::bigint
      FROM rb20_v2_5.step_stats
      WHERE run_id='{{run_id}}' AND step_id='RB20_03' AND metric_name='keep_member_cnt'
    ), (
      SELECT COUNT(*)::bigint
      FROM rb20_v2_5.keep_members
      WHERE run_id='{{run_id}}'
    )) AS keep_cnt,
    COALESCE((
      SELECT metric_value_numeric::bigint
      FROM rb20_v2_5.core_numbers
      WHERE run_id='{{run_id}}' AND metric_name='h_member_cnt'
    ), 0) AS h_cnt,
    COALESCE((
      SELECT SUM(metric_value_numeric)::bigint
      FROM rb20_v2_5.step_stats
      WHERE run_id='{{run_id}}' AND step_id='RB20_06' AND metric_name='r1_member_cnt'
    ), (
      SELECT COUNT(*)::bigint
      FROM rb20_v2_5.r1_members
      WHERE run_id='{{run_id}}'
    )) AS r1_cnt,
    COALESCE((
      SELECT SUM(metric_value_numeric)::bigint
      FROM rb20_v2_5.step_stats
      WHERE run_id='{{run_id}}' AND step_id='RB20_07' AND metric_name='e_member_cnt'
    ), 0) AS e_cnt,
    COALESCE((
      SELECT SUM(metric_value_numeric)::bigint
      FROM rb20_v2_5.step_stats
      WHERE run_id='{{run_id}}' AND step_id='RB20_08' AND metric_name='f_member_cnt'
    ), 0) AS f_cnt,
    COALESCE((
      SELECT COUNT(*)::bigint
      FROM rb20_v2_5.qa_assert
      WHERE run_id='{{run_id}}' AND assert_name='no_overlap_h_e' AND pass_flag = false
    ), 0)
    + COALESCE((
      SELECT COUNT(*)::bigint
      FROM rb20_v2_5.qa_assert
      WHERE run_id='{{run_id}}' AND assert_name='no_overlap_h_f' AND pass_flag = false
    ), 0)
    + COALESCE((
      SELECT COUNT(*)::bigint
      FROM rb20_v2_5.qa_assert
      WHERE run_id='{{run_id}}' AND assert_name='no_overlap_e_f' AND pass_flag = false
    ), 0) AS overlap_fail_cnt
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'conservation_keep_equals_hef',
  'STOP',
  (
    overlap_fail_cnt = 0
    AND keep_cnt = (h_cnt + r1_cnt)
    AND r1_cnt = (e_cnt + f_cnt)
    AND keep_cnt = (h_cnt + e_cnt + f_cnt)
  ),
  (
    'keep_cnt='||keep_cnt
    ||',h_cnt='||h_cnt
    ||',r1_cnt='||r1_cnt
    ||',e_cnt='||e_cnt
    ||',f_cnt='||f_cnt
    ||',keep_minus_h_gap='||(keep_cnt - (h_cnt + r1_cnt))
    ||',r1_minus_ef_gap='||(r1_cnt - (e_cnt + f_cnt))
    ||',keep_minus_hef_gap='||(keep_cnt - (h_cnt + e_cnt + f_cnt))
    ||',overlap_fail_cnt='||overlap_fail_cnt
  )
FROM cnts;

-- 3) 无幽灵：H/E/F 必须是 SourceMembers 子集
WITH hef_u AS (
  SELECT DISTINCT ip_long FROM rb20_v2_5.h_members WHERE run_id='{{run_id}}'
  UNION
  SELECT DISTINCT ip_long FROM rb20_v2_5.e_members WHERE run_id='{{run_id}}'
  UNION
  SELECT DISTINCT ip_long FROM rb20_v2_5.f_members WHERE run_id='{{run_id}}'
),
ghost AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM hef_u h
  LEFT JOIN rb20_v2_5.source_members sm
    ON sm.run_id='{{run_id}}' AND sm.ip_long=h.ip_long
  WHERE sm.ip_long IS NULL
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'no_ghost_hef_outside_source',
  'STOP',
  (cnt=0),
  ('ghost_cnt='||cnt::text)
FROM ghost;

-- 4) Drop 成员映射不蒸发：DropMembers 必须存在 NaturalMap
WITH miss AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.drop_members dm
  LEFT JOIN rb20_v2_5.map_member_block_natural map
    ON map.run_id=dm.run_id AND map.shard_id=dm.shard_id AND map.ip_long=dm.ip_long
  WHERE dm.run_id='{{run_id}}' AND map.ip_long IS NULL
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'drop_members_have_natural_map',
  'STOP',
  (cnt=0),
  ('missing_cnt='||cnt::text)
FROM miss;

-- 5) H 不得包含无效块
WITH bad AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.h_blocks
  WHERE run_id='{{run_id}}' AND network_tier_final='无效块'
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'h_excludes_invalid_tier',
  'STOP',
  (cnt=0),
  ('bad_cnt='||cnt::text)
FROM bad;

-- 6) H 不得包含 valid_cnt < 4 的块
WITH bad AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.h_blocks
  WHERE run_id='{{run_id}}' AND valid_cnt < 4
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'h_excludes_valid_lt4',
  'STOP',
  (cnt=0),
  ('bad_cnt='||cnt::text)
FROM bad;

-- 7) F 反连接审计：F 成员不得命中“实际 E 覆盖”的 atom27
WITH e_cov AS (
  SELECT DISTINCT shard_id, atom27_id
  FROM rb20_v2_5.e_members
  WHERE run_id='{{run_id}}'
),
bad AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.f_members fm
  JOIN e_cov ec
    ON ec.shard_id=fm.shard_id AND ec.atom27_id=fm.atom27_id
  WHERE fm.run_id='{{run_id}}'
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'f_excludes_e_coverage',
  'STOP',
  (cnt=0),
  ('bad_cnt='||cnt::text)
FROM bad;

-- 8) Step64 cnt=0 审计：必须存在 cnt=0 事件行
WITH s AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.split_events_64
  WHERE run_id='{{run_id}}' AND (cntL_valid=0 OR cntR_valid=0)
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'split_events_include_cnt0',
  'WARN',
  (cnt>0),
  ('cnt0_rows='||cnt::text)
FROM s;

-- 9) ShardPlan 必须与 shard_cnt 对齐（数量/连续性）
WITH sp AS (
  SELECT shard_id::int
  FROM rb20_v2_5.shard_plan
  WHERE run_id='{{run_id}}'
),
stats AS (
  SELECT
    COUNT(*)::int AS cnt,
    COUNT(DISTINCT shard_id)::int AS distinct_cnt,
    MIN(shard_id)::int AS min_id,
    MAX(shard_id)::int AS max_id
  FROM sp
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'shard_plan_matches_shard_cnt',
  'STOP',
  (cnt={{shard_cnt}}::int AND distinct_cnt=cnt AND min_id=0 AND max_id=({{shard_cnt}}::int - 1)),
  ('cnt='||cnt||',distinct_cnt='||distinct_cnt||',min='||min_id||',max='||max_id||',expected='||({{shard_cnt}}::int))
FROM stats;

-- 10) per-shard 关键实体必须全覆盖（否则“汇总数字对”也不能验收）
WITH sp AS (
  SELECT shard_id
  FROM rb20_v2_5.shard_plan
  WHERE run_id='{{run_id}}'
),
probe AS (
  SELECT
    sp.shard_id,
    EXISTS (
      SELECT 1
      FROM rb20_v2_5.step_stats ss
      WHERE ss.run_id='{{run_id}}'
        AND ss.step_id='RB20_01'
        AND ss.shard_id=sp.shard_id
        AND ss.metric_name='source_members_rows'
        AND ss.metric_value_numeric > 0
    ) AS has_source_members,
    EXISTS (
      SELECT 1
      FROM rb20_v2_5.map_member_block_natural map
      WHERE map.run_id='{{run_id}}'
        AND map.shard_id=sp.shard_id
      LIMIT 1
    ) AS has_map_natural,
    EXISTS (
      SELECT 1
      FROM rb20_v2_5.step_stats ss
      WHERE ss.run_id='{{run_id}}'
        AND ss.step_id='RB20_03'
        AND ss.shard_id=sp.shard_id
        AND ss.metric_name='preh_block_cnt'
        AND ss.metric_value_numeric > 0
    ) AS has_preh_blocks,
    EXISTS (
      SELECT 1
      FROM rb20_v2_5.step_stats ss
      WHERE ss.run_id='{{run_id}}'
        AND ss.step_id='RB20_04'
        AND ss.shard_id=sp.shard_id
        AND ss.metric_name='final_block_cnt'
        AND ss.metric_value_numeric > 0
    ) AS has_block_final,
    EXISTS (
      SELECT 1
      FROM rb20_v2_5.map_member_block_final map
      WHERE map.run_id='{{run_id}}'
        AND map.shard_id=sp.shard_id
      LIMIT 1
    ) AS has_map_final,
    EXISTS (
      SELECT 1
      FROM rb20_v2_5.step_stats ss
      WHERE ss.run_id='{{run_id}}'
        AND ss.step_id='RB20_04P'
        AND ss.shard_id=sp.shard_id
        AND ss.metric_name='final_profile_block_cnt'
        AND ss.metric_value_numeric > 0
    ) AS has_profile_final
  FROM sp
),
viol AS (
  SELECT
    COUNT(*)::int AS cnt,
    STRING_AGG(shard_id::text, ',' ORDER BY shard_id) AS shard_list
  FROM probe
  WHERE
    (NOT has_source_members)
    OR (has_source_members AND NOT has_map_natural)
    OR (has_preh_blocks AND (NOT has_block_final OR NOT has_map_final OR NOT has_profile_final))
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'per_shard_outputs_complete',
  'STOP',
  (cnt=0),
  ('viol_cnt='||cnt||',shards='||COALESCE(shard_list,'')) AS details
FROM viol;
