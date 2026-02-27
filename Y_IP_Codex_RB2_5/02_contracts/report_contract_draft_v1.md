# RB20 v2.0 — Report Contract（Draft v1）

唯一规范：`Y_IP_Codex_RB2_5/重构2.md`

> 目标：让每阶段报告“目的导向 + 可复现 + 可验收”，并为退化/漏跑 shard/口径漂移提供快速定位证据。

## 1) 报告输出形态（固定）

1. 每步写 `step_stats`（长表）：核心数、分布统计、异常计数
2. 每步必要时写 `rule_impact`：触发器/规则命中与影响
3. 关键里程碑写 `core_numbers`：可用于对外汇报的核心数字快照
4. 终验收写 `qa_assert`（severity=STOP）

对应表头见：`Y_IP_Codex_RB2_5/02_contracts/schema_contract_draft_v1.md#L1`

## 2) 统一维度与口径（固定）

- 维度：`run_id`（必有）；per-shard 指标必须带 `shard_id`
- `step_stats` 的全局指标：`shard_id = -1`（哨兵值，避免 NULL 与主键冲突）
- membership vs valid：所有“准入/画像/密度/窗口/网络规模评估”指标用 `is_valid=true`；异常只标记不删除
- DP 已确认：
  - DP-009：假日=周末（周末上报次数/总上报次数）
  - DP-010：密度类分母=IP数量；移动设备占比分母=设备量；上报类占比分母=上报量
  - DP-011：块级 `*_sum_valid` 空集标准化为 0；ratio 分母 0 返回 NULL

## 3) 阶段报告清单（v1）

### 3.1 RB20_00D（ShardPlan）

必备指标：
- 每 shard 的 `ip_long_start/ip_long_end/est_rows`
- 断言：无空 shard、无重叠、范围覆盖符合预期（失败即 STOP）
- 迭代信息：`plan_round`（最多 3；允许不均衡）

### 3.2 RB20_01（Source Members）

必备指标：
- `source_members_rows`（按 shard 与全局）
- `abnormal_dedup_rows`（全局）与 `source_members_abnormal_rows`（按 shard）
- 输入字段健康度抽查（可选）：关键字段 NULL/负值/异常关系（例如 `移动网络设备数量<=设备数量`）

### 3.3 RB20_02（Natural Blocks）

必备指标（按 shard 与全局）：
- 自然块数：`natural_block_cnt_total`
- CIDR 块数口径：`natural_block_cnt_ge4`（只计 `member_cnt_total>=4`）
- 自然块规模分布：`member_cnt_total` 的 p50/p90/p99/max
- 自然块平均 IP 数：`avg_member_cnt_total`（用于你要求的“ip平均数量”）
- 自然块“设备密度/上报密度”分布（valid 口径，分母=IP 数量）：
  - `device_density_valid = devices_sum_valid / valid_cnt`
  - `report_density_valid = reports_sum_valid / valid_cnt`

### 3.4 RB20_03（Pre Profile / PreH）

必备指标（按 shard 与全局）：
- Keep/Drop 计数与原因分布（`drop_reason`）
- `valid_cnt=0` 块数（必须单列；且应全部 Drop）
- `network_tier_pre` 分布（必须把 `'无效块'` 单列，不得混入正常 tier 分布）
- PreH 覆盖率：`preh_block_cnt / keep_block_cnt`

一致性检查（抽样/或全量汇总）：
- SIMPLE/network_tier 的阈值表、边界与 `valid_cnt=0` 处理是否生效（以 `devices_sum_valid/density/wA/wD/simple_score` 为证据）

### 3.5 RB20_11（HeadTail Window）

必备指标：
- `window_rows_cnt`（block×bucket64）
- `cntL_valid/cntR_valid` 的分布，必须覆盖 `0`（cnt=0 需要后续 SplitEvents 审计）

### 3.6 RB20_04（SplitEvents / Final Blocks）

必备指标：
- SplitEvents 总行数（按 shard）
- `cnt=0` 事件行数（必须存在）
- 三触发器命中分布（report/mobile/operator）与 `is_cut` 分布
- 切分前后块数变化：
  - `natural_block_cnt` vs `final_block_cnt`
  - 每个 parent 自然块的 segment 数分布（p50/p90/p99/max）

### 3.7 RB20_04P（Final Profile）

必备指标：
- `network_tier_final` 分布（`'无效块'` 单列）
- `network_tier_pre` vs `network_tier_final` 一致性抽样验证（同口径复算一致）
- Final 块“设备密度/上报密度”与“移动设备占比/工作时占比/假日占比”分布（valid 口径，分母按 DP-010/DP-009）：
  - 密度类分母=IP 数量；比例类分母=设备量或上报量

### 3.8 RB20_05（H Blocks / H Members）

必备指标：
- H Blocks 数、H Members 覆盖成员数
- H 画像核心数（例如 `devices_sum_valid/reports_sum_valid` 的分布）
- 断言：`network_tier_final='无效块'` 不得进入 H

### 3.9 RB20_07/08（E/F）

必备指标：
- E Atoms：原子数、通过密度阈值数、run_len 分布、short_run 数
- E Members / F Members：覆盖成员数
- F 反连接审计：确认使用 `atom27_id` 等值 anti-join（禁止 BETWEEN/NOT BETWEEN）

### 3.10 RB20_99（终验收）

QA_Assert（severity=STOP）必须覆盖：
- H/E/F 两两交集=0
- 守恒：KeepMembers = H_cov ∪ E_cov ∪ F
- 无幽灵：上述集合必须是 Source Members 子集
- Drop 成员映射不蒸发：Drop member 也必须进入 Map_All
- 切分不退化：SplitEvents 必须包含 cnt=0；final tier 分布不得全空/全同；`'无效块'` 单独统计

## 4) 可复现 SQL 位置（待生成）

- `Y_IP_Codex_RB2_5/03_sql/00_discovery/`：轻量事实采样（distinct/NULL/分布）
- `Y_IP_Codex_RB2_5/03_sql/00_contracts/`：DDL + 基础设施表（可重跑）
- 其它步骤 SQL：按 RB20_01..99 分目录落 `03_sql/`

## 5) StepStats / CoreNumbers 的 metric_name 约定（v1，固定键名）

目的：你要求“未来画像不要每次回读原始库”，因此阶段汇总必须写入固定键名，方便稳定复用与对账。

约定：
- `rb20_v2_5.step_stats.metric_name`：尽量使用 `snake_case`，并在键名中显式包含口径（`_total/_valid`）与对象（`natural/final/h/e/f`）
- 任何比例类指标按 `DP-010/DP-009` 的命名与分母约定（详见 `metric_contract_v1`）

推荐必写键名（最小集合；其余可扩展但不得复用同名异义）：

- RB20_00D（ShardPlan）
  - `shard_est_rows`（per-shard）
  - `plan_round_max`（global，写入 `core_numbers`）
- RB20_01（Source Members）
  - `source_members_rows`（per-shard）
  - `source_members_abnormal_rows`（per-shard）
  - `abnormal_dedup_rows`（global）
- RB20_02（Natural Blocks）
  - `natural_block_cnt_total`（per-shard + global）
  - `natural_block_cnt_ge4`（per-shard + global）
  - `avg_member_cnt_total`（per-shard + global；对应你要的“CIDR 块平均 IP 数”）
  - `member_cnt_total_p50/p90/p99/max`（per-shard + global）
- RB20_03（Pre Profile）
  - `avg_devices_sum_valid`、`avg_reports_sum_valid`（per-shard + global）
  - `device_density_valid_p50/p90/p99`、`report_density_valid_p50/p90/p99`（per-shard + global）
- RB20_04P（Final Profile）
  - `final_block_cnt`（per-shard + global；来自 RB20_04）
  - `final_profile_block_cnt`（per-shard + global；来自 RB20_04P）
  - `final_avg_member_cnt_total`（per-shard + global）
  - `final_device_density_valid_p50/p90/p99`、`final_report_density_valid_p50/p90/p99`
  - `final_mobile_device_ratio_valid_p50/p90`（移动覆盖：移动设备/设备量）
  - `final_worktime_report_ratio_valid_p50/p90`（工作时间占比：工作时上报/总上报）
  - `final_holiday_report_ratio_valid_p50/p90`（假日占比：周末上报/总上报；DP-009 固化）
- RB20_05（H）
  - `h_block_cnt`（global）
  - `h_member_cnt`（global）
- RB20_07/08（E/F）
  - `e_atom_cnt_total`、`e_atom_cnt_pass`（per-shard + global）
  - `e_run_cnt_total`、`e_run_len_p50/p90/p99/max`、`e_short_run_cnt`（per-shard + global）
  - `e_member_cnt`、`f_member_cnt`（per-shard + global）
