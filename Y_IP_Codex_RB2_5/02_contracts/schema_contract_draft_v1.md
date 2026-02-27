# RB20 v2.0 — Schema Contract（Draft v1：固定表头草案）

唯一规范：`Y_IP_Codex_RB2_5/重构2.md`  
字段研究依据：`Y_IP_Codex_RB2_5/00_discovery/field_research_v1.md`

> 说明：本文件是“表头合同草案”，用于你确认后再生成可执行 DDL。任何字段/口径/枚举/统计方式的最终版本，必须在你确认后写入正式合同（去掉 Draft），并记录到 `Y_IP_Codex_RB2_5/CHANGELOG.md`。

## 0) 输出落库位置（待 DP-006 固化）

## 0) 输出落库位置（DP-006 已确认）

- `output_schema`：`rb20_v2_5`

## 1) 全局通用字段（所有输出表）

- `run_id`：text（同一批次隔离）
- `contract_version`：text（Schema/Metric/Report 合同版本号；建议同一个版本串）
- `created_at`：timestamptz（写入时间）

per-shard 表额外要求：
- `shard_id`：smallint（逻辑范围：`0..(shard_cnt-1)`；物理约束建议 `0..255`）
  - `shard_cnt`：本次 run 的分片数量，默认 64；若遇到严重倾斜导致无法完成 64 分片，必须走 DP-014 并在合同里写死（禁止执行中途改 shard_cnt/改约束“补丁继续跑”）。

## 2) 成员字段投影（H/E/F Members 统一复用）

> 目标：H/E/F 成员未来画像不回读 W 源表。该投影宽度建议走 DP-008（本草案先给“画像最小投影 v1”）。

### 2.1 `member_attr_profile_v1`（建议默认）

来自 W 源表（字段名保持一致）：
- `ip_long` bigint（成员主键）
- `ip_address` varchar
- `IP归属国家` text
- `IP归属运营商` text
- `上报次数` bigint
- `设备数量` bigint
- `移动网络设备数量` bigint
- `WiFi设备数量` bigint
- `VPN设备数量` bigint
- `有线网络设备数量` bigint
- `异常网络设备数量` bigint
- `空网络状态设备数量` bigint
- `深夜上报次数` bigint
- `工作时上报次数` bigint
- `工作日上报次数` bigint
- `周末上报次数` bigint
- `周活跃天数比例` numeric
- `深夜活动比例` numeric
- `工作日周末平均比例` numeric
- `平均每设备上报次数` numeric
- `移动网络设备比例` numeric
- `WiFi设备比例` numeric
- `VPN设备比例` numeric
- `空网络状态设备比例` numeric
- `异常网络设备比例` numeric
- `有线网络设备比例` numeric
- `SIM不可用比例` numeric
- `无效总流量设备比例` numeric
- `零移动流量设备比例` numeric
- `上报应用比例` numeric
- `低安卓API设备比例` numeric

派生/工程字段：
- `is_abnormal` boolean（异常只标记不删除）
- `is_valid` boolean（= NOT is_abnormal）
- `atom27_id` bigint（`floor(ip_long/32)`）
- `bucket64` bigint（`floor(ip_long/64)`）

> 备注：块级画像中若使用这些成员级 ratio 字段做聚合，必须先按 DP-010 固化聚合口径；对“有数量字段”的 ratio，仍建议块级从数量 sum 重算。

### 2.2 `member_attr_mirror_w_v1`（DP-008 选 C：全量镜像投影）

你已回复：DP-008 选 C（H/E/F Members 复制 W 源表全部列，避免未来画像回读 W 源表）。  
因此正式合同建议将 H/E/F Members（以及可选的 `source_members`）统一采用下列“全量镜像”字段：

- W 源表全部字段（63 列，字段名保持一致）：
  - `ip_long` bigint
  - `ip_address` varchar
  - `IP归属国家` text
  - `IP归属运营商` text
  - `过滤前上报次数` bigint
  - `上报次数` bigint
  - `过滤前设备数量` bigint
  - `设备数量` bigint
  - `应用数量` bigint
  - `活跃天数` bigint
  - `安卓ID数量` bigint
  - `OAID数量` bigint
  - `谷歌ID数量` bigint
  - `启动ID数量` bigint
  - `型号数量` bigint
  - `制造商数量` bigint
  - `深夜上报次数` bigint
  - `工作时上报次数` bigint
  - `工作日上报次数` bigint
  - `周末上报次数` bigint
  - `以太网接口上报次数` bigint
  - `代理上报次数` bigint
  - `Root设备上报次数` bigint
  - `ADB调试上报次数` bigint
  - `充电状态上报次数` bigint
  - `单设备最大上报次数` bigint
  - `DAA业务上报次数` bigint
  - `DNA业务上报次数` bigint
  - `WiFi可比上报次数` bigint
  - `SSID去重数` bigint
  - `BSSID去重数` bigint
  - `网关存在上报次数` bigint
  - `平均每设备上报次数` numeric
  - `周活跃天数比例` numeric
  - `深夜活动比例` numeric
  - `工作日周末平均比例` numeric
  - `平均每设备重启次数` numeric
  - `平均每设备应用数` numeric
  - `DAA DNA业务比例` numeric
  - `上报应用比例` numeric
  - `低安卓API设备比例` numeric
  - `WiFi设备数量` bigint
  - `WiFi设备比例` numeric
  - `移动网络设备数量` bigint
  - `移动网络设备比例` numeric
  - `VPN设备数量` bigint
  - `VPN设备比例` numeric
  - `空网络状态设备数量` bigint
  - `空网络状态设备比例` numeric
  - `异常网络设备数量` bigint
  - `异常网络设备比例` numeric
  - `有线网络设备数量` bigint
  - `有线网络设备比例` numeric
  - `SIM不可用比例` numeric
  - `无效总流量设备比例` numeric
  - `零移动流量设备比例` numeric
  - `制造商分布风险状态` integer
  - `SDK版本分布异常分数` varchar
  - `开始日期` varchar
  - `结束日期` varchar
  - `创建时间` text
  - `活跃日期列表` varchar
  - `IP稳定性` text

- 工程/派生字段（用于算法与索引）：
  - `is_abnormal` boolean
  - `is_valid` boolean
  - `atom27_id` bigint
  - `bucket64` bigint

## 3) 关键实体表（按主版本 4.1 缺一不可）

### 3.1 基础设施实体

#### 3.1.1 `run_meta`（全局）

- PK：`(run_id)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `status` text（e.g. INIT/RUNNING/DONE/FAILED）
  - `started_at` timestamptz
  - `finished_at` timestamptz
  - `note` text
  - `created_at` timestamptz

#### 3.1.2 `config_kv`（全局）

- PK：`(run_id, key)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `key` text
  - `value_text` text
  - `value_json` jsonb
  - `created_at` timestamptz

#### 3.1.3 `shard_plan`（全局）

- PK：`(run_id, shard_id)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `ip_long_start` bigint（含）
  - `ip_long_end` bigint（不含）
  - `est_rows` bigint（规划时的估计/统计）
  - `plan_round` smallint（0=初始等分；1/2=两次 5% 调整）
  - `created_at` timestamptz

### 3.2 成员母体与异常标记

#### 3.2.1 `abnormal_dedup`（全局）

- PK：`(run_id, ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `ip_long` bigint（来源：异常表 `ipv4_bigint`）
  - `created_at` timestamptz

#### 3.2.2 `source_members`（per-shard）

- PK：`(run_id, shard_id, ip_long)`
- 字段：按 DP-008 选 C，建议采用 `member_attr_mirror_w_v1` 全量字段 + 通用字段

### 3.3 自然块链路实体（CIDR 块链路）

#### 3.3.1 `block_natural`（per-shard）

- PK：`(run_id, shard_id, block_id_natural)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `block_id_natural` text
  - `ip_start` bigint（含）
  - `ip_end` bigint（含）
  - `member_cnt_total` bigint（membership）
  - `created_at` timestamptz

#### 3.3.2 `map_member_block_natural`（per-shard）

- PK：`(run_id, shard_id, ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `ip_long` bigint
  - `block_id_natural` text
  - `created_at` timestamptz

#### 3.3.3 `profile_pre`（per-shard，自然块预画像）

- PK：`(run_id, shard_id, block_id_natural)`
- 字段（必须包含 SIMPLE/network_tier 的输入/中间/输出；主版本 2.5）：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `block_id_natural` text
  - `keep_flag` boolean
  - `drop_reason` text（例如 `ALL_ABNORMAL_BLOCK`）
  - `member_cnt_total` bigint
  - `valid_cnt` bigint
  - `devices_sum_valid` bigint
  - `density` numeric
  - `wA` integer
  - `wD` integer
  - `simple_score` integer
  - `network_tier_pre` text
  - `created_at` timestamptz
- 字段（密度类补充，分母=IP 数量；DP-010 选 A 才启用为固定口径）：
  - `report_density_valid` numeric（`reports_sum_valid / NULLIF(valid_cnt,0)`）
  - （可选，不建议落表；查询时派生）`mobile_device_ratio_valid = mobile_devices_sum_valid / NULLIF(devices_sum_valid,0)`
- 字段（为“移动覆盖/工作时间/周末占比/块统计”补齐的块级聚合 v1，全部建议同时提供 `_sum_total` 与 `_sum_valid`，以便审计异常影响）：
  - `reports_sum_total` bigint
  - `reports_sum_valid` bigint
  - `devices_sum_total` bigint
  - `mobile_devices_sum_total` bigint
  - `mobile_devices_sum_valid` bigint
  - `wifi_devices_sum_total` bigint
  - `wifi_devices_sum_valid` bigint
  - `vpn_devices_sum_total` bigint
  - `vpn_devices_sum_valid` bigint
  - `wired_devices_sum_total` bigint
  - `wired_devices_sum_valid` bigint
  - `abnormal_net_devices_sum_total` bigint
  - `abnormal_net_devices_sum_valid` bigint
  - `empty_net_devices_sum_total` bigint
  - `empty_net_devices_sum_valid` bigint
  - `worktime_reports_sum_total` bigint
  - `worktime_reports_sum_valid` bigint
  - `workday_reports_sum_total` bigint
  - `workday_reports_sum_valid` bigint
  - `weekend_reports_sum_total` bigint
  - `weekend_reports_sum_valid` bigint
  - `late_night_reports_sum_total` bigint
  - `late_night_reports_sum_valid` bigint

> 备注：以上 ratio（例如移动覆盖率、工作时占比、周末占比）建议由 sum 字段在查询/画像层重算；如需固化 ratio 字段需走 DP-010。

#### 3.3.4 `preh_blocks`（per-shard）

- PK：`(run_id, shard_id, block_id_natural)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `block_id_natural` text
  - `created_at` timestamptz

#### 3.3.5 `window_headtail_64`（per-shard，block×bucket64 摘要实体）

- PK：`(run_id, shard_id, block_id_natural, bucket64)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `block_id_natural` text
  - `bucket64` bigint
  - `k` smallint（固定 5）
  - `left_cnt_valid` smallint
  - `right_cnt_valid` smallint
  - `left_reports_sum_valid` bigint
  - `right_reports_sum_valid` bigint
  - `left_mobile_devices_sum_valid` bigint
  - `right_mobile_devices_sum_valid` bigint
  - `left_operator_unique` text（distinct=1 才写入，否则 NULL）
  - `right_operator_unique` text
  - `created_at` timestamptz

#### 3.3.6 `split_events_64`（per-shard，切分事件实体）

- PK：`(run_id, shard_id, block_id_natural, cut_ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `block_id_natural` text
  - `bucket64` bigint
  - `cut_ip_long` bigint
  - `cntL_valid` smallint
  - `cntR_valid` smallint
  - `ratio_report` numeric
  - `cvL` numeric
  - `cvR` numeric
  - `mobile_diff` numeric
  - `mobile_cnt_ratio` numeric
  - `opL` text
  - `opR` text
  - `trigger_report` boolean
  - `trigger_mobile` boolean
  - `trigger_operator` boolean
  - `is_cut` boolean
  - `created_at` timestamptz

> 主版本红线：必须落 `cnt=0` 的审计记录（即使不触发切分）。

#### 3.3.7 `block_final`（per-shard，最终块实体）

- PK：`(run_id, shard_id, block_id_final)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `block_id_final` text（`parent||'_'||lpad(seq,3,'0')`）
  - `block_id_parent` text（自然块 id）
  - `segment_seq` integer
  - `ip_start` bigint（含）
  - `ip_end` bigint（含）
  - `member_cnt_total` bigint
  - `created_at` timestamptz

#### 3.3.8 `map_member_block_final`（per-shard，成员→最终块映射）

- PK：`(run_id, shard_id, ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `ip_long` bigint
  - `block_id_final` text
  - `block_id_parent` text
  - `created_at` timestamptz

#### 3.3.9 `profile_final`（per-shard，最终块画像）

- PK：`(run_id, shard_id, block_id_final)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `block_id_final` text
  - `block_id_parent` text
  - `member_cnt_total` bigint
  - `valid_cnt` bigint
  - `devices_sum_valid` bigint
  - `density` numeric
  - `wA` integer
  - `wD` integer
  - `simple_score` integer
  - `network_tier_final` text
  - （同 `profile_pre` 的块级聚合 v1：`*_sum_total/_sum_valid` 全量字段）
  - `report_density_valid` numeric
  - （可选，不建议落表；查询时派生）`mobile_device_ratio_valid = mobile_devices_sum_valid / NULLIF(devices_sum_valid,0)`
  - `created_at` timestamptz

### 3.4 最终三库交付实体

#### 3.4.1 `h_blocks`（全局/或 per-shard 汇总）

- PK：`(run_id, block_id_final)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `block_id_final` text
  - `block_id_parent` text
  - `network_tier_final` text（固定应为 `中型网络`）
  - `member_cnt_total` bigint
  - `valid_cnt` bigint
  - `devices_sum_valid` bigint
  - `reports_sum_valid` bigint
  - `created_at` timestamptz

#### 3.4.2 `h_members`（全局/或 per-shard 汇总）

- PK：`(run_id, ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `ip_long` bigint
  - `block_id_final` text
  - 说明：为避免在 H/E/F 等集合表重复存储 63 列宽字段，宽字段只在 `source_members` 存一份；
    H/E/F 本体表保留集合归属字段，画像读取使用 `*_members_wide` 视图（见 `03_sql/00_contracts/03_views_rb20_v2.sql`）
  - `created_at` timestamptz

对应宽视图（用于画像，不回读 public 源表）：
- `rb20_v2_5.h_members_wide`

#### 3.4.3 `e_atoms`（per-shard）

- PK：`(run_id, shard_id, atom27_id)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `atom27_id` bigint
  - `ip_start` bigint（= atom27_id*32）
  - `ip_end` bigint（= ip_start+31）
  - `valid_ip_cnt` integer
  - `atom_density` numeric
  - `is_e_atom` boolean
  - `created_at` timestamptz

#### 3.4.4 `e_runs`（per-shard）

- PK：`(run_id, shard_id, e_run_id)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `e_run_id` text
  - `atom27_start` bigint
  - `atom27_end` bigint
  - `run_len` integer
  - `short_run` boolean
  - `ip_start` bigint
  - `ip_end` bigint
  - `created_at` timestamptz

#### 3.4.5 `e_members`（per-shard）

- PK：`(run_id, shard_id, ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `ip_long` bigint
  - `atom27_id` bigint
  - `e_run_id` text
  - 宽字段见视图 `rb20_v2_5.e_members_wide`（来源 `source_members`）
  - `created_at` timestamptz

宽字段一致性要求（你提出的“H/E/F 画像不回读源表”红线落地）：
- `rb20_v2_5.e_members_wide` 必须暴露与 `rb20_v2_5.h_members_wide` 同一套 W 源表全量镜像字段（`member_attr_mirror_w_v1`，63 列）+ 工程字段（`is_abnormal/is_valid/atom27_id/bucket64`）；
- 额外携带 E 集合归属字段（`e_run_id` 等）。

#### 3.4.6 `f_members`（per-shard）

- PK：`(run_id, shard_id, ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `ip_long` bigint
  - `atom27_id` bigint
  - 宽字段见视图 `rb20_v2_5.f_members_wide`（来源 `source_members`）
  - `created_at` timestamptz

宽字段一致性要求：
- `rb20_v2_5.f_members_wide` 必须暴露与 `rb20_v2_5.h_members_wide` 同一套 W 源表全量镜像字段（`member_attr_mirror_w_v1`，63 列）+ 工程字段（`is_abnormal/is_valid/atom27_id/bucket64`）；
- 额外携带 F 集合归属字段（`atom27_id` 等）。

### 3.4.7 `keep_members`（per-shard，守恒口径的 KeepMembers 实体）

> 用途：为 QA“守恒/无幽灵/互斥”提供稳定对账锚点，避免每次都动态 join `profile_pre`+`map_member_block_natural` 计算。

- PK：`(run_id, shard_id, ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `ip_long` bigint
  - `block_id_natural` text
  - `keep_flag` boolean（冗余；应恒为 true）
  - 按 DP-008 选 C：`member_attr_mirror_w_v1`（除去重复字段）
  - `created_at` timestamptz

### 3.4.8 `drop_members`（per-shard，审计口径）

> 主版本要求：Drop 不等于删除；Drop 成员必须保留映射用于审计与对账（见 2.5.5/7.4）。

- PK：`(run_id, shard_id, ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `ip_long` bigint
  - `block_id_natural` text
  - `drop_reason` text
  - 按 DP-008 选 C：`member_attr_mirror_w_v1`（除去重复字段）
  - `created_at` timestamptz

### 3.4.9 `r1_members`（per-shard，R1 Residue）

> 定义：`R1 = KeepMembers \\ H_cov`（主版本 8.4 RB20_06）。后续 E/F 均以 R1 为输入集合。

- PK：`(run_id, shard_id, ip_long)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `shard_id` smallint
  - `ip_long` bigint
  - `atom27_id` bigint
  - `block_id_natural` text
  - `block_id_final` text（若可关联；否则可为 NULL）
  - 按 DP-008 选 C：`member_attr_mirror_w_v1`（除去重复字段）
  - `created_at` timestamptz

### 3.5 审计与验收实体

#### 3.5.1 `step_stats`（全局+per-shard，长表）

全局 vs per-shard：
- 因 `step_stats` 主键包含 `shard_id`（PK 不允许 NULL），全局指标统一写 `shard_id = -1` 作为哨兵值；per-shard 指标写 `0..(shard_cnt-1)`。

- PK：`(run_id, step_id, shard_id, metric_name)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `step_id` text（RB20_01/RB20_02/...）
  - `shard_id` smallint（全局统计可为 NULL）
  - `metric_name` text
  - `metric_value_numeric` numeric
  - `metric_value_text` text
  - `created_at` timestamptz

#### 3.5.2 `rule_impact`（全局+per-shard，长表）

- PK：`(run_id, step_id, shard_id, rule_name)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `step_id` text
  - `shard_id` smallint
  - `rule_name` text
  - `hit_cnt` bigint
  - `impact_cnt` bigint
  - `note` text
  - `created_at` timestamptz

#### 3.5.3 `qa_assert`（全局，STOP 即停）

- PK：`(run_id, assert_name)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `assert_name` text
  - `severity` text（固定包含 STOP）
  - `pass_flag` boolean
  - `details` text
  - `created_at` timestamptz

#### 3.5.4 `core_numbers`（全局，长表）

- PK：`(run_id, metric_name)`
- 字段：
  - `run_id` text
  - `contract_version` text
  - `metric_name` text
  - `metric_value_numeric` numeric
  - `metric_value_text` text
  - `created_at` timestamptz

## 4) 需要你确认的新增点（不写死就不落 DDL）

1. **成员字段投影宽度**：是否采用 `member_attr_profile_v1`，或更宽/更窄（建议新增 DP-008）。
2. **块级 ratio 是否落表**：移动覆盖/工作时占比/周末占比等 ratio 是“查询时由 sum 重算”还是“固化到 profile 表”（建议 DP-010）。
3. **“假日占比”定义**：是否用 `周末` 近似 `假日`，或留空（建议 DP-009）。

> 更新：你已确认 DP-008 选 C、DP-009 选 A、DP-010 选 A、DP-011 选 A；本草案已按这些选项调整表头与口径说明。
