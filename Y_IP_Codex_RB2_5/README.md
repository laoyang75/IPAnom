# Y_IP_Codex_RB2_5 (RB20 v2.0 重构交付目录)

> **唯一主版本规范**：`Y_IP_Codex_RB2_5/重构2.md`（本目录所有合同/SQL/报告均以其为唯一口径，不引入第二套规范）。
>
> 本目录用于 RB20 v2.0 在 PostgreSQL 内的可复现落地：合同（Contracts）→ 实体链路（Entities）→ 报告（Reports）→ 终验收（QA_Assert）。

## 1. 运行批次（run_id）命名规则

- `run_id`：`rb20v2_{YYYYMMDD}_{HHMMSS}_sg_{seq3}`
  - `YYYYMMDD/HHMMSS`：Asia/Singapore 时间
  - `seq3`：当日递增 3 位序号（001/002/...）
- `shard_id`：逻辑范围 `0..(shard_cnt-1)`（默认 `shard_cnt=64`；字符串或整数均可，但合同中固定一种；建议 `smallint`，并用物理约束 `0..255`）

示例：`rb20v2_20260107_093000_sg_001`

## 2. 输入锚点（只读）

- 输入 schema：`public`
- W 源表：`public."ip库构建项目_ip源表_20250811_20250824_v2_1"`
  - 主键：`ip_long`
  - 关键字段：`ip_address` / `IP归属国家` / `IP归属运营商` / `上报次数` / `设备数量` / `移动网络设备数量`
- A 异常表：`public."ip库构建项目_异常ip表_20250811_20250824_v2"`
  - 字段：`ipv4_bigint`（可空；空值行忽略）

## 3. 输出（由 Schema Contract 定义）

- 输出建议独立 schema：`rb20_v2_5`（最终以 `02_contracts/schema_contract.*` 为准）
- 所有输出表必须包含 `run_id`；per-shard 步骤输出必须包含 `shard_id`

## 4. 目录约定

- `00_discovery/`：探索、风险识别、Work Plan、主版本规则摘录
- `01_decisions/`：Decision Points（人类只需回复选项）
- `02_contracts/`：Schema / Metric / Report 合同（人类确认后锁定）
- `03_sql/`：可直接在 DB 内执行的 SQL（按步骤组织）
- `04_runbook/`：并行/长耗时/高风险操作的 Runbook + SQL 包
- `06_reports/`：每阶段独立报告（证明“目的是否达成”）
- `07_release/`：最终交付物与验收证据快照

## 5. 复现入口（约定）

1) 先跑 `RB20_00A/B/C/D`（合同 + ShardPlan）并获得人类确认  
2) 再按 shard 并行跑 `RB20_01/02/03/11/04/04P/06/07/08`  
3) 最后全局跑 `RB20_05/99`，输出终验收断言与核心数字报告

> 每个 per-shard 脚本必须幂等：执行前清理 `run_id + shard_id` 的旧数据，再写入。

## 6. CHANGELOG

见 `CHANGELOG.md`，任何口径/合同/策略变化必须记录：原因、影响范围、对应 Decision Point。

## 7. 快速入口（当前进度）

- 硬规则摘录：`00_discovery/master_rules_extract.md`
- Work Plan：`00_discovery/work_plan_v1.md`
- 字段研究（基于 MCP 实测）：`00_discovery/field_research_v1.md`
- 合同草案（表头/口径/报告）：`02_contracts/schema_contract_draft_v1.md`、`02_contracts/metric_contract_draft_v1.md`、`02_contracts/report_contract_draft_v1.md`
- 合同 v1（已确认入口）：`02_contracts/schema_contract_v1.md`、`02_contracts/metric_contract_v1.md`、`02_contracts/report_contract_v1.md`
- Decision Points 状态：`01_decisions/DP_STATUS.md`
- Gate-0 SQL（discovery/ddl/shardplan skeleton）：`03_sql/00_discovery/00_source_sanity.sql`、`03_sql/00_contracts/00_ddl_skeleton_rb20_v2.sql`、`03_sql/00_contracts/10_shard_plan_skeleton.sql`
- Gate-0 SQL（full DDL/indexes）：`03_sql/00_contracts/01_ddl_rb20_v2_full.sql`、`03_sql/00_contracts/02_indexes_rb20_v2.sql`
- 画像宽字段视图（H/E/F wide）：`03_sql/00_contracts/03_views_rb20_v2.sql`
- Gate-1 样本 Runbook：`04_runbook/01_gate1_sample_run.md`
- Gate-2 全链路 Runbook：`04_runbook/02_gate2_full_pipeline_run.md`
- 执行 Agent（All-in-one 单文件 Runbook）：`04_runbook/03_exec_agent_runbook_all_in_one.md`
- ShardPlan（DBHub 兼容 SQL-only）：`03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`
- ShardPlan（plpgsql 版本，DBHub 下可能不可用）：`03_sql/00_contracts/10_shard_plan.sql`

关键步骤 SQL（按目录）：
- `03_sql/RB20_04/04_split_and_final_blocks_shard.sql`
- `03_sql/RB20_04P/04P_final_profile_shard.sql`
- `03_sql/RB20_05/05_h_blocks_and_members.sql`
- `03_sql/RB20_06/06_r1_members_shard.sql`
- `03_sql/RB20_07/07_e_atoms_runs_members_shard.sql`
- `03_sql/RB20_08/08_f_members_shard.sql`
- `03_sql/RB20_99/99_qa_assert.sql`

报告模板：
- `06_reports/01_gate1_sample_template.md`
- `06_reports/02_gate2_full_pipeline_template.md`
