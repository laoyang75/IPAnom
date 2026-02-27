# DP-011：块级聚合的 NULL/空集处理（sum/ratio 标准化）

决策：你已选择 **选A**（sum 标准化为 0；ratio 分母 0 保持 NULL）。

背景：
- 在 PG 中 `SUM(x) FILTER (...)` 当过滤后无行时返回 NULL（不是 0）
- 主版本仅强制要求 `valid_cnt=0` 时 `devices_sum_valid` 标准化为 0（2.5.5），但你新增了大量块级画像 sum/ratio 字段（移动/工作时/周末等）
- 若不统一标准化，会导致画像查询出现大量 NULL，且与“无效块/有效块”边界混淆

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（推荐：sum 标准化为 0，ratio 保持 NULL）
规则：
- 所有 `*_sum_valid`：当过滤后无行（尤其 `valid_cnt=0`）统一 `COALESCE(sum,0)`
- 所有 `*_sum_total`：当成员集合为空统一 `COALESCE(sum,0)`（理论上不应发生，属于防御）
- 所有 ratio：分母为 0 时保持 NULL（例如 `SUM(devices)=0` → `mobile_cover_ratio=NULL`）

### B（严格 SQL 语义）
规则：
- 空集 SUM 保持 NULL；仅对主版本强制项 `devices_sum_valid` 做 0 标准化
- ratio 仍用 `NULLIF(denom,0)`，分母为 0 结果为 NULL

### C（sum 标准化为 0，ratio 也标准化为 0）
规则：
- 所有 sum 同 A
- ratio：分母为 0 时返回 0（可能掩盖“无有效成员/无设备”的异常状态）

## 固化位置（被选中后）

- `02_contracts/metric_contract.*`：sum/ratio 的边界处理
- `02_contracts/report_contract.*`：相关审计（NULL 分布、valid_cnt=0 单独统计）
- `CHANGELOG.md`：记录原因与影响
