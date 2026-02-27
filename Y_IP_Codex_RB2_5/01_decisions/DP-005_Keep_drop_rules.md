# DP-005：除 `valid_cnt=0` 外的 Keep/Drop 规则

决策：按你的“其他都按推荐”已确认 **选A**（除 `valid_cnt=0` 外全部 Keep）。

背景（主版本说明）：`valid_cnt=0`（全异常块）必须 Drop 且 `drop_reason='ALL_ABNORMAL_BLOCK'`；除此之外的 Keep/Drop 未写死。

本 DP 的含义（帮助你理解）：
- Keep/Drop 是对“自然块实体（block_natural）”的后续处理开关：
  - `Keep`：该自然块进入后续（PreH/切分/最终块画像/H/E/F 分流等）
  - `Drop`：该自然块**不参与准入与分层**（例如不得进入 PreH/H），但其成员仍必须保留在映射/DropMembers 中用于审计与守恒对账（主版本硬要求：Drop 不等于删除）
- 选项 B/C 会改变 KeepMembers 集合，从而影响后续 H/E/F 覆盖范围与守恒口径；因此“最保守不改业务定义”的默认就是选 A。

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（推荐 / 最保守）
除 `valid_cnt=0` 强制 Drop 外，其余自然块全部 Keep（后续由 H/E/F 自然分流）。

### B
在 A 基础上增加 Drop 条件（可能改变下游覆盖范围）：
- `member_cnt_total < 4` 的自然块标记为 Drop（仍需保留 Map 用于审计对账）

### C
在 A 基础上把 `member_cnt_total < 4` 限定为“只进入审计链路，不参与准入/分层”：
- 实体保留 + 映射保留，但不进入后续 H/E/F 与切分/画像准入统计

## 固化位置（被选中后）

- `02_contracts/metric_contract.*`（Keep/Drop 判定与 drop_reason 枚举）
- `02_contracts/report_contract.*`（Drop 分布与对账口径）
- 并在 `CHANGELOG.md` 记录原因与影响范围
