-- RB20 v2.0 / Gate-0 / Run Init + Config (v1)
-- 使用前替换：
--   {{run_id}}           例如 rb20v2_20260107_093000_sg_001
--   {{contract_version}} 例如 contract_v1

INSERT INTO rb20_v2_5.run_meta(run_id, contract_version, status, started_at, note)
VALUES ('{{run_id}}', '{{contract_version}}', 'INIT', now(), 'RB20 v2.0 init')
ON CONFLICT (run_id) DO UPDATE
SET contract_version = EXCLUDED.contract_version,
    status = EXCLUDED.status,
    started_at = EXCLUDED.started_at,
    note = EXCLUDED.note;

-- 幂等写入配置（DP 已全确认）
INSERT INTO rb20_v2_5.config_kv(run_id, contract_version, key, value_json)
VALUES
  ('{{run_id}}','{{contract_version}}','dp_001_cn_country_values', '["中国"]'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_002_abnormal_mode', '"distinct"'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_003_devices_coalesce_zero', 'true'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_004_preh_rule', '"keep_and_valid_cnt_gt_0"'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_005_keep_drop_rule', '"drop_only_all_abnormal_else_keep"'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_006_output_schema', '"rb20_v2_5"'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_007_f_antijoin', '"atom27_id_eq_antijoin"'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_008_member_attr_projection', '"mirror_w_full"'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_009_holiday_def', '"holiday_equals_weekend"'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_010_denominator_rules', '{"density":"ip_cnt","mobile_device_ratio":"devices_sum","report_ratios":"reports_sum"}'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_011_sum_empty_to_zero', 'true'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_012_shardplan', '{"impl":"sql_only_ntile_quantile","note":"DP-012 选C：按分位数重切；shard_cnt 由 DP-014 决定并必须在开跑前写死"}'::jsonb),
  ('{{run_id}}','{{contract_version}}','dp_014_shard_cnt_policy', '{"choice":"B","allow_gt_64":true,"rule":"shard_cnt must be fixed before RB20_00D; no mid-run change"}'::jsonb)
ON CONFLICT (run_id, key) DO UPDATE
SET contract_version = EXCLUDED.contract_version,
    value_json = EXCLUDED.value_json;
