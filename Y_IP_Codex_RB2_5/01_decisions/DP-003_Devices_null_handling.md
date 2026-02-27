# DP-003：`设备数量` NULL 处理（影响 `devices_sum_valid` 与 `density`）

决策：按你的“其他都按推荐”已确认 **选A**（画像计算使用 `COALESCE(设备数量,0)`）。

背景（主版本提示）：`devices_sum_valid = SUM(设备数量)`（valid 口径）；若存在 NULL，需在 Metric Contract 明确是否 `COALESCE(设备数量,0)`（主版本默认建议 COALESCE 为 0，避免 NULL 导致低估）。

MCP 实测证据（W 源表）：
- `设备数量` NULL 行 = 0（全量非 NULL）

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（推荐）
画像计算统一使用 `COALESCE(设备数量,0)`：
- `devices_sum_valid = SUM(COALESCE(设备数量,0)) FILTER (WHERE is_valid)`

### B
保持 SQL 默认 SUM 语义（忽略 NULL；若全 NULL 则 SUM 可能为 NULL），并仅对 `valid_cnt=0` 的块按主版本规则标准化为 0：
- `devices_sum_valid = COALESCE(SUM(设备数量) FILTER (WHERE is_valid), 0)`（仅在画像输出层标准化）

### C
若发现 `设备数量` 存在 NULL，则 QA_Assert severity=STOP（强约束，需先修复输入或清洗）

## 固化位置（被选中后）

- `02_contracts/metric_contract.*`（SIMPLE/network_tier 的输入指标定义与边界处理）
- `02_contracts/report_contract.*`（相关审计/分布输出）
- 并在 `CHANGELOG.md` 记录原因与影响范围
