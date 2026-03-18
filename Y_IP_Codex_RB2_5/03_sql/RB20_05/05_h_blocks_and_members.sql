-- RB20_05 (global): H Blocks + H Members
-- 依赖：RB20_04/04P 已生成 block_final / map_member_block_final / profile_final
--
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1

DELETE FROM rb20_v2_5.h_members
WHERE run_id='{{run_id}}';

DELETE FROM rb20_v2_5.h_blocks
WHERE run_id='{{run_id}}';

-- H Blocks：final tier = 中型/大型/超大网络（高密度连续块）
INSERT INTO rb20_v2_5.h_blocks(
  run_id, contract_version,
  block_id_final, block_id_parent,
  network_tier_final,
  member_cnt_total, valid_cnt,
  devices_sum_valid, reports_sum_valid
)
SELECT
  '{{run_id}}','{{contract_version}}',
  pf.block_id_final,
  pf.block_id_parent,
  pf.network_tier_final,
  pf.member_cnt_total,
  pf.valid_cnt,
  pf.devices_sum_valid,
  pf.reports_sum_valid
FROM rb20_v2_5.profile_final pf
WHERE pf.run_id='{{run_id}}'
  AND pf.network_tier_final IN ('中型网络','大型网络','超大网络')
  AND pf.valid_cnt >= 4
  AND pf.member_cnt_total >= 4;

-- H Members：membership（含 abnormal IP），H 准入由 network_tier_final + valid_cnt>=4 + member_cnt_total>=4 决定
INSERT INTO rb20_v2_5.h_members(
  run_id, contract_version,
  ip_long, block_id_final
)
SELECT
  '{{run_id}}','{{contract_version}}',
  mf.ip_long,
  mf.block_id_final
FROM rb20_v2_5.map_member_block_final mf
JOIN rb20_v2_5.h_blocks hb
  ON hb.run_id=mf.run_id
 AND hb.block_id_final=mf.block_id_final
WHERE mf.run_id='{{run_id}}';

-- CoreNumbers（global）
DELETE FROM rb20_v2_5.core_numbers
WHERE run_id='{{run_id}}' AND metric_name IN ('h_block_cnt','h_member_cnt');

INSERT INTO rb20_v2_5.core_numbers(run_id, contract_version, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','h_block_cnt', COUNT(*)::numeric
FROM rb20_v2_5.h_blocks
WHERE run_id='{{run_id}}';

INSERT INTO rb20_v2_5.core_numbers(run_id, contract_version, metric_name, metric_value_numeric)
SELECT '{{run_id}}','{{contract_version}}','h_member_cnt', COUNT(*)::numeric
FROM rb20_v2_5.h_members
WHERE run_id='{{run_id}}';
