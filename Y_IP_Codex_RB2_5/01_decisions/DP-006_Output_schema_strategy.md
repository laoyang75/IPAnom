# DP-006：输出 schema 命名与隔离策略

决策：你已指定 `rb20_v2_5`（对应 **选A**：独立 schema）。

背景：主版本允许任务阶段通过 Schema Contract 决定输出落库方式（字段/主键必须合同先行）。这里需要明确：输出是否与 `public` 隔离，便于清理与复现。

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（推荐）
独立 schema（例如 `rb20_v2_5`），所有输出表放在该 schema 下；每次 run 清理更安全。

### B
继续使用 `public`，但所有输出表带统一前缀（例如 `rb20_v2__*`）。

### C
独立 schema + 每个 `run_id` 再按子 schema/分区隔离（更重，通常不需要）。

## 固化位置（被选中后）

- `02_contracts/schema_contract.*`（schema 名、表命名规则、清理策略）
- 并在 `CHANGELOG.md` 记录原因与影响范围
