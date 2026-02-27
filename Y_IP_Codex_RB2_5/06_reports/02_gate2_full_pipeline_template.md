# Gate 2（全链路）报告模板

主版本：`Y_IP_Codex_RB2_5/重构2.md`  
可复现入口：`Y_IP_Codex_RB2_5/04_runbook/02_gate2_full_pipeline_run.md`

## 1) 目的

- 完整跑通 H/E/F 三库链路，并给出可验收的守恒/互斥/无幽灵/切分审计证据。

## 2) 输入/输出实体

输入：
- W 源表：`public."ip库构建项目_ip源表_20250811_20250824_v2_1"`
- A 异常表：`public."ip库构建项目_异常ip表_20250811_20250824_v2"`

核心输出：
- 最终块链路：`rb20_v2_5.profile_final` / `rb20_v2_5.block_final` / `rb20_v2_5.map_member_block_final`
- H：`rb20_v2_5.h_blocks` / `rb20_v2_5.h_members`（画像宽字段：`rb20_v2_5.h_members_wide`）
- E：`rb20_v2_5.e_atoms` / `rb20_v2_5.e_runs` / `rb20_v2_5.e_members`（画像宽字段：`rb20_v2_5.e_members_wide`）
- F：`rb20_v2_5.f_members`（画像宽字段：`rb20_v2_5.f_members_wide`）
- 验收：`rb20_v2_5.qa_assert`

## 3) 核心指标与分布（证据）

阶段汇总：
- per-shard：`rb20_v2_5.step_stats`
- global：`rb20_v2_5.core_numbers`

必须覆盖（最小集合，键名见 Report Contract v1 第 5 节）：
- 自然块：块数、`avg_member_cnt_total`、密度分布
- Final 块：块数、tier 分布（`'无效块'` 单列）、密度分布、移动/工作时/假日占比分布
- H/E/F：覆盖成员数、E run_len 分布、short_run 数

## 4) 异常/风险与解释

- `valid_cnt=0`：必须 Drop；不得进入 H/E/F；但实体与映射必须保留用于审计
- 切分审计：`split_events_include_cnt0` 必须 PASS（cnt=0 行必须存在）

## 5) 终验收（QA_Assert）

- 以 `rb20_v2_5.qa_assert` 为唯一验收锚点：所有 `severity=STOP` 必须 `pass_flag=true`
- 失败即 STOP，并按 assert_name 定位回溯对应阶段 SQL

## 6) 可复现 SQL

- `Y_IP_Codex_RB2_5/04_runbook/02_gate2_full_pipeline_run.md`（顺序与依赖）
- `Y_IP_Codex_RB2_5/03_sql/RB20_05/05_h_blocks_and_members.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_06/06_r1_members_shard.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_07/07_e_atoms_runs_members_shard.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_08/08_f_members_shard.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_99/99_qa_assert.sql`

