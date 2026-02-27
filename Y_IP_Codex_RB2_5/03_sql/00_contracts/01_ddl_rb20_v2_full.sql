-- RB20 v2.0 / Contracts / Full DDL (v1)
-- Output schema: rb20_v2_5 (DP-006)
-- Contract version: contract_v1 (建议 run_meta/config_kv/所有表统一写入同一版本串)

CREATE SCHEMA IF NOT EXISTS rb20_v2_5;

-- ============ Infrastructure ============

CREATE TABLE IF NOT EXISTS rb20_v2_5.run_meta (
  run_id text PRIMARY KEY,
  contract_version text NOT NULL,
  status text NOT NULL,
  started_at timestamptz,
  finished_at timestamptz,
  note text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.config_kv (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  key text NOT NULL,
  value_text text,
  value_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, key)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.shard_plan (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  ip_long_start bigint NOT NULL,
  ip_long_end bigint NOT NULL,
  est_rows bigint,
  plan_round smallint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id),
  CONSTRAINT shard_plan_range_chk CHECK (ip_long_start < ip_long_end),
  CONSTRAINT shard_plan_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

-- ============ Abnormal ============

CREATE TABLE IF NOT EXISTS rb20_v2_5.abnormal_dedup (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  ip_long bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, ip_long)
);

-- ============ Source Members (mirror W + derived) ============
-- Per-shard, wide table (DP-008 C)

CREATE TABLE IF NOT EXISTS rb20_v2_5.source_members (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,

  ip_long bigint NOT NULL,
  ip_address varchar NOT NULL,
  "IP归属国家" text,
  "IP归属运营商" text,
  "过滤前上报次数" bigint,
  "上报次数" bigint,
  "过滤前设备数量" bigint,
  "设备数量" bigint,
  "应用数量" bigint,
  "活跃天数" bigint,
  "安卓ID数量" bigint,
  "OAID数量" bigint,
  "谷歌ID数量" bigint,
  "启动ID数量" bigint,
  "型号数量" bigint,
  "制造商数量" bigint,
  "深夜上报次数" bigint,
  "工作时上报次数" bigint,
  "工作日上报次数" bigint,
  "周末上报次数" bigint,
  "以太网接口上报次数" bigint,
  "代理上报次数" bigint,
  "Root设备上报次数" bigint,
  "ADB调试上报次数" bigint,
  "充电状态上报次数" bigint,
  "单设备最大上报次数" bigint,
  "DAA业务上报次数" bigint,
  "DNA业务上报次数" bigint,
  "WiFi可比上报次数" bigint,
  "SSID去重数" bigint,
  "BSSID去重数" bigint,
  "网关存在上报次数" bigint,
  "平均每设备上报次数" numeric,
  "周活跃天数比例" numeric,
  "深夜活动比例" numeric,
  "工作日周末平均比例" numeric,
  "平均每设备重启次数" numeric,
  "平均每设备应用数" numeric,
  "DAA DNA业务比例" numeric,
  "上报应用比例" numeric,
  "低安卓API设备比例" numeric,
  "WiFi设备数量" bigint,
  "WiFi设备比例" numeric,
  "移动网络设备数量" bigint,
  "移动网络设备比例" numeric,
  "VPN设备数量" bigint,
  "VPN设备比例" numeric,
  "空网络状态设备数量" bigint,
  "空网络状态设备比例" numeric,
  "异常网络设备数量" bigint,
  "异常网络设备比例" numeric,
  "有线网络设备数量" bigint,
  "有线网络设备比例" numeric,
  "SIM不可用比例" numeric,
  "无效总流量设备比例" numeric,
  "零移动流量设备比例" numeric,
  "制造商分布风险状态" integer,
  "SDK版本分布异常分数" varchar,
  "开始日期" varchar,
  "结束日期" varchar,
  "创建时间" text,
  "活跃日期列表" varchar,
  "IP稳定性" text,

  is_abnormal boolean NOT NULL,
  is_valid boolean NOT NULL,
  atom27_id bigint NOT NULL,
  bucket64 bigint NOT NULL,

  created_at timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (run_id, shard_id, ip_long),
  CONSTRAINT source_members_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

-- ============ Natural Blocks ============

CREATE TABLE IF NOT EXISTS rb20_v2_5.block_natural (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  block_id_natural text NOT NULL,
  ip_start bigint NOT NULL,
  ip_end bigint NOT NULL,
  member_cnt_total bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, block_id_natural),
  CONSTRAINT block_natural_range_chk CHECK (ip_start <= ip_end),
  CONSTRAINT block_natural_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.map_member_block_natural (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  ip_long bigint NOT NULL,
  block_id_natural text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, ip_long),
  CONSTRAINT map_nat_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

-- ============ Pre Profile / PreH ============

CREATE TABLE IF NOT EXISTS rb20_v2_5.profile_pre (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  block_id_natural text NOT NULL,

  keep_flag boolean NOT NULL,
  drop_reason text,

  member_cnt_total bigint NOT NULL,
  valid_cnt bigint NOT NULL,
  devices_sum_valid bigint NOT NULL,
  density numeric,
  wA integer,
  wD integer,
  simple_score integer,
  network_tier_pre text NOT NULL,

  reports_sum_total bigint NOT NULL,
  reports_sum_valid bigint NOT NULL,
  devices_sum_total bigint NOT NULL,
  mobile_devices_sum_total bigint NOT NULL,
  mobile_devices_sum_valid bigint NOT NULL,
  wifi_devices_sum_total bigint NOT NULL,
  wifi_devices_sum_valid bigint NOT NULL,
  vpn_devices_sum_total bigint NOT NULL,
  vpn_devices_sum_valid bigint NOT NULL,
  wired_devices_sum_total bigint NOT NULL,
  wired_devices_sum_valid bigint NOT NULL,
  abnormal_net_devices_sum_total bigint NOT NULL,
  abnormal_net_devices_sum_valid bigint NOT NULL,
  empty_net_devices_sum_total bigint NOT NULL,
  empty_net_devices_sum_valid bigint NOT NULL,
  worktime_reports_sum_total bigint NOT NULL,
  worktime_reports_sum_valid bigint NOT NULL,
  workday_reports_sum_total bigint NOT NULL,
  workday_reports_sum_valid bigint NOT NULL,
  weekend_reports_sum_total bigint NOT NULL,
  weekend_reports_sum_valid bigint NOT NULL,
  late_night_reports_sum_total bigint NOT NULL,
  late_night_reports_sum_valid bigint NOT NULL,

  report_density_valid numeric,

  created_at timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (run_id, shard_id, block_id_natural),
  CONSTRAINT profile_pre_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.preh_blocks (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  block_id_natural text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, block_id_natural),
  CONSTRAINT preh_blocks_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

-- ============ Window Head/Tail ============

CREATE TABLE IF NOT EXISTS rb20_v2_5.window_headtail_64 (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  block_id_natural text NOT NULL,
  bucket64 bigint NOT NULL,
  k smallint NOT NULL,

  left_cnt_valid smallint NOT NULL,
  right_cnt_valid smallint NOT NULL,
  left_reports_sum_valid bigint NOT NULL,
  right_reports_sum_valid bigint NOT NULL,
  left_mobile_devices_sum_valid bigint NOT NULL,
  right_mobile_devices_sum_valid bigint NOT NULL,
  left_operator_unique text,
  right_operator_unique text,

  created_at timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (run_id, shard_id, block_id_natural, bucket64),
  CONSTRAINT window_ht_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

-- ============ Split Events ============

CREATE TABLE IF NOT EXISTS rb20_v2_5.split_events_64 (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  block_id_natural text NOT NULL,
  bucket64 bigint NOT NULL,
  cut_ip_long bigint NOT NULL,

  cntL_valid smallint,
  cntR_valid smallint,

  ratio_report numeric,
  cvL numeric,
  cvR numeric,
  mobile_diff numeric,
  mobile_cnt_ratio numeric,
  opL text,
  opR text,

  trigger_report boolean NOT NULL,
  trigger_mobile boolean NOT NULL,
  trigger_operator boolean NOT NULL,
  is_cut boolean NOT NULL,

  created_at timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (run_id, shard_id, block_id_natural, cut_ip_long),
  CONSTRAINT split_events_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

-- ============ Final Blocks / Map / Profile ============

CREATE TABLE IF NOT EXISTS rb20_v2_5.block_final (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  block_id_final text NOT NULL,
  block_id_parent text NOT NULL,
  segment_seq integer NOT NULL,
  ip_start bigint NOT NULL,
  ip_end bigint NOT NULL,
  member_cnt_total bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, block_id_final),
  CONSTRAINT block_final_range_chk CHECK (ip_start <= ip_end),
  CONSTRAINT block_final_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.map_member_block_final (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  ip_long bigint NOT NULL,
  block_id_final text NOT NULL,
  block_id_parent text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, ip_long),
  CONSTRAINT map_final_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.profile_final (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  block_id_final text NOT NULL,
  block_id_parent text NOT NULL,

  member_cnt_total bigint NOT NULL,
  valid_cnt bigint NOT NULL,
  devices_sum_valid bigint NOT NULL,
  density numeric,
  wA integer,
  wD integer,
  simple_score integer,
  network_tier_final text NOT NULL,

  reports_sum_total bigint NOT NULL,
  reports_sum_valid bigint NOT NULL,
  devices_sum_total bigint NOT NULL,
  mobile_devices_sum_total bigint NOT NULL,
  mobile_devices_sum_valid bigint NOT NULL,
  wifi_devices_sum_total bigint NOT NULL,
  wifi_devices_sum_valid bigint NOT NULL,
  vpn_devices_sum_total bigint NOT NULL,
  vpn_devices_sum_valid bigint NOT NULL,
  wired_devices_sum_total bigint NOT NULL,
  wired_devices_sum_valid bigint NOT NULL,
  abnormal_net_devices_sum_total bigint NOT NULL,
  abnormal_net_devices_sum_valid bigint NOT NULL,
  empty_net_devices_sum_total bigint NOT NULL,
  empty_net_devices_sum_valid bigint NOT NULL,
  worktime_reports_sum_total bigint NOT NULL,
  worktime_reports_sum_valid bigint NOT NULL,
  workday_reports_sum_total bigint NOT NULL,
  workday_reports_sum_valid bigint NOT NULL,
  weekend_reports_sum_total bigint NOT NULL,
  weekend_reports_sum_valid bigint NOT NULL,
  late_night_reports_sum_total bigint NOT NULL,
  late_night_reports_sum_valid bigint NOT NULL,

  report_density_valid numeric,

  created_at timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (run_id, shard_id, block_id_final),
  CONSTRAINT profile_final_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

-- ============ H / E / F ============

CREATE TABLE IF NOT EXISTS rb20_v2_5.h_blocks (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  block_id_final text NOT NULL,
  block_id_parent text NOT NULL,
  network_tier_final text NOT NULL,
  member_cnt_total bigint NOT NULL,
  valid_cnt bigint NOT NULL,
  devices_sum_valid bigint NOT NULL,
  reports_sum_valid bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, block_id_final)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.h_members (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  ip_long bigint NOT NULL,
  block_id_final text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, ip_long)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.keep_members (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  ip_long bigint NOT NULL,
  block_id_natural text NOT NULL,
  keep_flag boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, ip_long),
  CONSTRAINT keep_members_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.drop_members (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  ip_long bigint NOT NULL,
  block_id_natural text NOT NULL,
  drop_reason text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, ip_long),
  CONSTRAINT drop_members_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.r1_members (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  ip_long bigint NOT NULL,
  atom27_id bigint NOT NULL,
  block_id_natural text NOT NULL,
  block_id_final text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, ip_long),
  CONSTRAINT r1_members_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.e_atoms (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  atom27_id bigint NOT NULL,
  ip_start bigint NOT NULL,
  ip_end bigint NOT NULL,
  valid_ip_cnt integer NOT NULL,
  atom_density numeric NOT NULL,
  is_e_atom boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, atom27_id),
  CONSTRAINT e_atoms_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.e_runs (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  e_run_id text NOT NULL,
  atom27_start bigint NOT NULL,
  atom27_end bigint NOT NULL,
  run_len integer NOT NULL,
  short_run boolean NOT NULL,
  ip_start bigint NOT NULL,
  ip_end bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, e_run_id),
  CONSTRAINT e_runs_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.e_members (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  ip_long bigint NOT NULL,
  atom27_id bigint NOT NULL,
  e_run_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, ip_long),
  CONSTRAINT e_members_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.f_members (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  ip_long bigint NOT NULL,
  atom27_id bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id, ip_long),
  CONSTRAINT f_members_shard_id_chk CHECK (shard_id >= 0 AND shard_id <= 255)
);

-- ============ Audit / QA ============

CREATE TABLE IF NOT EXISTS rb20_v2_5.step_stats (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  step_id text NOT NULL,
  shard_id smallint,
  metric_name text NOT NULL,
  metric_value_numeric numeric,
  metric_value_text text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, step_id, shard_id, metric_name)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.rule_impact (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  step_id text NOT NULL,
  shard_id smallint,
  rule_name text NOT NULL,
  hit_cnt bigint,
  impact_cnt bigint,
  note text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, step_id, shard_id, rule_name)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.qa_assert (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  assert_name text NOT NULL,
  severity text NOT NULL,
  pass_flag boolean NOT NULL,
  details text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, assert_name)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.core_numbers (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  metric_name text NOT NULL,
  metric_value_numeric numeric,
  metric_value_text text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, metric_name)
);
