# DP-008：H/E/F Members 成员画像字段投影宽度

决策：你已选择 **选C**（全量镜像投影）。

实现说明（为避免重复存储超宽字段）：
- 全量镜像落在 `rb20_v2_5.source_members`（每个成员只存一份宽字段）
- H/E/F 等集合表本体存“成员集合 + 关键归属字段”，画像读取宽字段时使用视图：
  - `rb20_v2_5.h_members_wide` / `rb20_v2_5.e_members_wide` / `rb20_v2_5.f_members_wide`
  - 宽字段来源统一为 `rb20_v2_5.source_members`（不回读 `public` 源表）

背景：你要求 H/E/F 未来用于画像，尽量不回读 W 源表。由于 `F Members` 很可能接近 KeepMembers 全量（量级约 6000 万），成员字段投影越宽，落库体量与索引成本越高；需要先合同确认。

参考实测（W 源表）：
- 行数约 59,706,088；源表 63 列，约 43.47 GB（见 `Y_IP_Codex_RB2_5/00_discovery/field_research_v1.md`）

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（推荐：画像最小投影 v1）
成员表只固化 `member_attr_profile_v1`（见 `Y_IP_Codex_RB2_5/02_contracts/schema_contract_draft_v1.md` 第 2 节）：
- 覆盖你点名的：移动覆盖（数量）、工作时/周末占比（计数）、以及块统计所需的设备/上报计数
- 其余“比例”字段在块级画像从 sum 重算或按 DP-010 决定是否纳入

### B（扩展投影）
在 A 基础上追加：W 源表中所有与“画像”强相关的 numeric 比例字段（例如 `深夜活动比例/周活跃天数比例/上报应用比例/低安卓API设备比例/...`），以便未来直接用成员级比例做聚合（仍需 DP-010 固化聚合口径）。

### C（全量镜像投影）
H/E/F Members 复制 W 源表全部 63 列（+ abnormal/valid/atom27/bucket64），成员表即“可替代源表”的镜像。

## 固化位置（被选中后）

- `02_contracts/schema_contract.*`：H/E/F Members 表头（以及需要的索引策略）
- `02_contracts/metric_contract.*`：比例字段聚合口径（如选择 B/C）
- `CHANGELOG.md`：记录原因与影响（存储/性能/画像能力）
