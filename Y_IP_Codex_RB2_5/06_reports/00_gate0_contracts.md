# RB20 v2.0 — Gate 0 报告（Contracts + 基础设施落地）

主版本：`Y_IP_Codex_RB2_5/重构2.md`

## 1) 目的

在不跑 RB20 全链路全量的前提下，完成：
- 合同（Schema/Metric/Report）v1 固化
- `rb20_v2_5` schema 与关键空表/索引落地
- 输入表字段研究的证据化输出，支持后续画像字段与聚合口径

## 2) 输入锚点（只读）

- W 源表：`public."ip库构建项目_ip源表_20250811_20250824_v2_1"`
  - 行数（PG 统计）：59,706,088
  - 列数：63
- A 异常表：`public."ip库构建项目_异常ip表_20250811_20250824_v2"`
  - 行数（PG 统计）：78,853
  - `ipv4_bigint` 实测全唯一、无 NULL（不会 join 扩行）

证据与字段研究：`Y_IP_Codex_RB2_5/00_discovery/field_research_v1.md`

## 3) 合同与决策（已确认）

### 3.1 Decision Points 状态

已全部确认：`Y_IP_Codex_RB2_5/01_decisions/DP_STATUS.md`

关键选择摘录：
- 输出 schema：`rb20_v2_5`（DP-006）
- H/E/F Members 属性策略：W 源表全量镜像（DP-008）
- 假日=周末（DP-009）
- 分母规则：密度分母=IP数；移动设备占比分母=设备量；上报类占比分母=上报量（DP-010）
- sum/ratio 空集处理：`*_sum_valid` 标准化为 0；ratio 分母 0→NULL（DP-011）
- ShardPlan：允许不均衡，最多 3 轮；SQL-only 实现按“初始等分 + 两轮 5% 调整”提供（DP-012/实现约束见第 5 节）

### 3.2 Contracts v1

- Schema Contract v1：`Y_IP_Codex_RB2_5/02_contracts/schema_contract_v1.md`
- Metric Contract v1：`Y_IP_Codex_RB2_5/02_contracts/metric_contract_v1.md`
- Report Contract v1：`Y_IP_Codex_RB2_5/02_contracts/report_contract_v1.md`

## 4) 输出实体（已落地）

### 4.1 输出 schema

- `rb20_v2_5`

### 4.2 已创建表（空表）

已在 `rb20_v2_5` 创建主版本要求的关键实体表（不含数据）：Run/Config/ShardPlan、SourceMembers、Natural/Pre/Window/Split/Final、H/E/F、StepStats/RuleImpact/QA_Assert/CoreNumbers。

DDL 入口：
- 全量 DDL：`Y_IP_Codex_RB2_5/03_sql/00_contracts/01_ddl_rb20_v2_full.sql`
- 最小索引：`Y_IP_Codex_RB2_5/03_sql/00_contracts/02_indexes_rb20_v2.sql`
- 画像宽字段视图（避免 H/E/F 重复存宽字段）：`Y_IP_Codex_RB2_5/03_sql/00_contracts/03_views_rb20_v2.sql`

## 5) ShardPlan（实现说明与限制）

主版本要求 ShardPlan “初始等分 + 两次 5% 调整”。在当前执行环境中：
- DBHub `execute_sql` 对 plpgsql（`CREATE PROCEDURE ... $$ ... $$`）支持不稳定，因此 **不依赖存储过程**。
- 提供 DBHub 兼容的 SQL-only 版本：`Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`
  - 会对 W 源表执行 3 次计数扫描（round0/round1/round2）
  - 最终写入 `rb20_v2_5.shard_plan`，`plan_round=2`

如需使用 plpgsql 版本（在 psql 环境/其它执行器下可能可用）：`Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan.sql`

## 6) 可复现入口（本阶段）

- Discovery 轻量事实：`Y_IP_Codex_RB2_5/03_sql/00_discovery/00_source_sanity.sql`
- 合同 DDL 与索引：`Y_IP_Codex_RB2_5/03_sql/00_contracts/01_ddl_rb20_v2_full.sql`、`Y_IP_Codex_RB2_5/03_sql/00_contracts/02_indexes_rb20_v2.sql`

## 7) 下一步（Gate 1：样本跑通）

建议顺序：
1. 选择一个 `run_id`（按 `Y_IP_Codex_RB2_5/README.md` 规则）
2. 生成 ShardPlan（先跑 SQL-only 版本）
3. 从 1~2 个 shard 开始跑 RB20_01 → RB20_02 → RB20_03 → RB20_11 → RB20_04 → RB20_04P 的 SQL（待生成到 `03_sql/`），并输出对应阶段报告到 `06_reports/`
