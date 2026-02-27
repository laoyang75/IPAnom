-- RB20_99 (global): QA_Assert（severity=STOP）
-- 依赖：RB20_03 keep_members/drop_members；RB20_05 h_*；RB20_07 e_*；RB20_08 f_*；RB20_04 split_events_64；RB20_04P profile_final
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1
--   {{shard_cnt}}        例如 64（或按 DP-014 选定的 shard 数量）

DELETE FROM rb20_v2_5.qa_assert
WHERE run_id='{{run_id}}';

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

-- 2) 守恒：KeepMembers = H_cov ∪ E_cov ∪ F（按 ip_long 去重）
WITH keep_u AS (
  SELECT DISTINCT ip_long
  FROM rb20_v2_5.keep_members
  WHERE run_id='{{run_id}}'
),
hef_u AS (
  SELECT DISTINCT ip_long FROM rb20_v2_5.h_members WHERE run_id='{{run_id}}'
  UNION
  SELECT DISTINCT ip_long FROM rb20_v2_5.e_members WHERE run_id='{{run_id}}'
  UNION
  SELECT DISTINCT ip_long FROM rb20_v2_5.f_members WHERE run_id='{{run_id}}'
),
cnts AS (
  SELECT
    (SELECT COUNT(*)::bigint FROM keep_u) AS keep_cnt,
    (SELECT COUNT(*)::bigint FROM hef_u) AS hef_cnt,
    (SELECT COUNT(*)::bigint FROM keep_u k LEFT JOIN hef_u h USING (ip_long) WHERE h.ip_long IS NULL) AS keep_minus_hef,
    (SELECT COUNT(*)::bigint FROM hef_u h LEFT JOIN keep_u k USING (ip_long) WHERE k.ip_long IS NULL) AS hef_minus_keep
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'conservation_keep_equals_hef',
  'STOP',
  (keep_minus_hef=0 AND hef_minus_keep=0),
  ('keep_cnt='||keep_cnt||',hef_cnt='||hef_cnt||',keep_minus_hef='||keep_minus_hef||',hef_minus_keep='||hef_minus_keep)
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

-- 6) F 反连接审计：F 成员的 atom27_id 不得命中 is_e_atom=true
WITH bad AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.f_members fm
  JOIN rb20_v2_5.e_atoms ea
    ON ea.run_id=fm.run_id AND ea.shard_id=fm.shard_id AND ea.atom27_id=fm.atom27_id
  WHERE fm.run_id='{{run_id}}' AND ea.is_e_atom
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'f_excludes_e_atoms',
  'STOP',
  (cnt=0),
  ('bad_cnt='||cnt::text)
FROM bad;

-- 7) Step64 cnt=0 审计：必须存在 cnt=0 事件行
WITH s AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.split_events_64
  WHERE run_id='{{run_id}}' AND (cntL_valid=0 OR cntR_valid=0)
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'split_events_include_cnt0',
  'STOP',
  (cnt>0),
  ('cnt0_rows='||cnt::text)
FROM s;

-- 8) ShardPlan 必须与 shard_cnt 对齐（数量/连续性）
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

-- 9) per-shard 关键实体必须全覆盖（否则“汇总数字对”也不能验收）
WITH sp AS (
  SELECT shard_id
  FROM rb20_v2_5.shard_plan
  WHERE run_id='{{run_id}}'
),
sm AS (
  SELECT shard_id, COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.source_members
  WHERE run_id='{{run_id}}'
  GROUP BY 1
),
mapn AS (
  SELECT shard_id, COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.map_member_block_natural
  WHERE run_id='{{run_id}}'
  GROUP BY 1
),
preh AS (
  SELECT shard_id, COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.preh_blocks
  WHERE run_id='{{run_id}}'
  GROUP BY 1
),
bf AS (
  SELECT shard_id, COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.block_final
  WHERE run_id='{{run_id}}'
  GROUP BY 1
),
mapf AS (
  SELECT shard_id, COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.map_member_block_final
  WHERE run_id='{{run_id}}'
  GROUP BY 1
),
pf AS (
  SELECT shard_id, COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.profile_final
  WHERE run_id='{{run_id}}'
  GROUP BY 1
),
miss AS (
  SELECT
    sp.shard_id,
    COALESCE(sm.cnt,0) AS source_members_cnt,
    COALESCE(mapn.cnt,0) AS map_natural_cnt,
    COALESCE(preh.cnt,0) AS preh_blocks_cnt,
    COALESCE(bf.cnt,0) AS block_final_cnt,
    COALESCE(mapf.cnt,0) AS map_final_cnt,
    COALESCE(pf.cnt,0) AS profile_final_cnt
  FROM sp
  LEFT JOIN sm   USING (shard_id)
  LEFT JOIN mapn USING (shard_id)
  LEFT JOIN preh USING (shard_id)
  LEFT JOIN bf   USING (shard_id)
  LEFT JOIN mapf USING (shard_id)
  LEFT JOIN pf   USING (shard_id)
),
viol AS (
  SELECT
    COUNT(*)::int AS cnt,
    STRING_AGG(shard_id::text, ',' ORDER BY shard_id) AS shard_list
  FROM miss
  WHERE
    -- ShardPlan 中出现的 shard 必须有 source_members（否则 shard_plan 自相矛盾或漏跑 RB20_01）
    source_members_cnt = 0
    OR
    -- 只要 shard 有成员，则必须有 natural map（成员→自然块映射不可缺）
    (source_members_cnt > 0 AND map_natural_cnt = 0)
    OR
    -- 只要 shard 有 PreH，则必须产出 FinalBlock/FinalMap/FinalProfile（Step64 可能为 0 行是允许的，但 final 产物不可缺）
    (preh_blocks_cnt > 0 AND (block_final_cnt = 0 OR map_final_cnt = 0 OR profile_final_cnt = 0))
)
INSERT INTO rb20_v2_5.qa_assert(run_id, contract_version, assert_name, severity, pass_flag, details)
SELECT
  '{{run_id}}','{{contract_version}}',
  'per_shard_outputs_complete',
  'STOP',
  (cnt=0),
  ('viol_cnt='||cnt||',shards='||COALESCE(shard_list,'')) AS details
FROM viol;
