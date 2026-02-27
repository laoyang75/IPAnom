# DP-002：异常表去重/聚合策略（字段：`ipv4_bigint`）

决策：按你的“其他都按推荐”已确认 **选A**（只做 DISTINCT 去重实体用于 join）。

背景（主版本要求）：异常表可能一对多，任务阶段必须先对 `ipv4_bigint` 去重或聚合，避免 join 扩行；`ipv4_bigint` 为空的行忽略。

MCP 实测证据（异常表）：
- `ipv4_bigint` NULL 行 = 0
- `COUNT(DISTINCT ipv4_bigint)=COUNT(*)`（78,853 行全唯一），直接 join 不会扩行

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（推荐）
只做去重（最小实现、最稳）：
- `abnormal_dedup = SELECT DISTINCT ipv4_bigint AS ip_long FROM 异常表 WHERE ipv4_bigint IS NOT NULL`

### B
做聚合（保留审计强度）：
- `abnormal_dedup = SELECT ipv4_bigint AS ip_long, COUNT(*) AS abnormal_row_cnt FROM 异常表 WHERE ipv4_bigint IS NOT NULL GROUP BY 1`
- join 标记 abnormal 仍以 `ip_long` 等值为准

### C
同时保留两张实体：
- `abnormal_dedup`（DISTINCT 用于 join）
- `abnormal_agg`（GROUP BY 用于审计/报告）

## 固化位置（被选中后）

- `02_contracts/schema_contract.*`（Abnormal Dedup/Abnormal Agg 实体表头）
- `02_contracts/metric_contract.*`（异常只标记不删除 + valid 口径）
- 并在 `CHANGELOG.md` 记录原因与影响范围
