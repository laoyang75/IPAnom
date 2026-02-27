# RB20 v2.0 — 字段研究 v1（基于 MCP/PG 实测）

唯一规范：`Y_IP_Codex_RB2_5/重构2.md`

## 1) 输入表体量与基础健康度（实测）

### 1.1 W 源表（成员母体）

- 表：`public."ip库构建项目_ip源表_20250811_20250824_v2_1"`
- 行数：`59,706,088`
- 体量：约 `43.47 GB`
- `ip_long`：NOT NULL，且 `COUNT(DISTINCT ip_long)=COUNT(*)`（实测唯一）
- 关键字段空值（实测均为 0）：`IP归属国家 / 设备数量 / 移动网络设备数量 / 上报次数 / ip_address`
- `IP归属国家` distinct：仅 `中国`（实测 1 个取值，且全量均为中国）
- 运营商：distinct `163`；NULL 行 `29,655`

### 1.2 A 异常表（仅异常标记）

- 表：`public."ip库构建项目_异常ip表_20250811_20250824_v2"`
- 行数：`78,853`；体量约 `22 MB`
- `ipv4_bigint`：NULL 行 `0`；且 `COUNT(DISTINCT ipv4_bigint)=COUNT(*)`（实测唯一，无需聚合也不会扩行）

## 2) 你要的画像字段：源表已经覆盖大部分

你提到的“移动覆盖、工作时间占比、假日占比、CIDR 块的 IP/设备/上报统计”等，在 W 表中已有可直接复用的字段（或可由字段组合推导）：

### 2.1 移动覆盖

W 表已有：
- `移动网络设备数量`（bigint）
- `移动网络设备比例`（numeric）
- 还有：`WiFi设备数量/比例`、`VPN设备数量/比例`、`有线网络设备数量/比例`、`异常网络设备数量/比例`、`空网络状态设备数量/比例`

建议（默认）：在成员表保留 **数量字段**，并在块级画像中用 `SUM(数量)/SUM(设备数量)` 统一重算比例（避免“按 IP 平均比例”带来偏差）。

补充（ratio 字段可用性，pg_stats）：
- `移动网络设备比例/WiFi设备比例/VPN设备比例/...` 的 `null_frac` 约为 0（可直接使用）
- `SIM不可用比例` 有约 `1.08%` NULL
- `工作日周末平均比例` 有约 `24.67%` NULL（若用于画像需先定 NULL 处理）

补充（移动占比语义校验，MCP 抽样验证）：
- `移动网络设备比例 = ROUND(移动网络设备数量::numeric / 设备数量, 2)`（抽样 SYSTEM 0.1% 全量一致）
  - 说明：该字段是“移动设备占比（分母=设备量）”，不是“移动上报占比”

### 2.2 工作时间占比 / 周末（假日）占比

W 表已有：
- `工作时上报次数`（bigint）→ 可推导 `worktime_report_ratio = 工作时上报次数 / 上报次数`
- `工作日上报次数`、`周末上报次数`（bigint）→ 可推导 `weekend_report_ratio = 周末上报次数 / 上报次数`
- `工作日周末平均比例`（numeric）→ 该字段含义需合同锁定：是否等价/可替代上面的 ratio

“假日占比”字段**源表未出现“假日/节假日”字样**；仅有“周末”。是否把“周末”当作“假日”需要 DP 固化（见 DP-009）。

### 2.3 CIDR/块统计（IP 平均数量、设备量、上报量）

这些是块级聚合字段（Natural/Final Block Profile 必须实体化），建议最少固化：
- `member_cnt_total`（membership，含异常）
- `valid_cnt`（valid，仅 `is_valid=true`）
- `devices_sum_valid`、`reports_sum_valid`（valid）
- `devices_sum_total`、`reports_sum_total`（membership，用于审计“异常影响”）
- `mobile_devices_sum_valid`、`wifi_devices_sum_valid`、`vpn_devices_sum_valid` 等（valid）
- 以及 SIMPLE/network_tier 的输入/中间/输出（主版本 2.5）

“平均 IP 数量”这类是 **报告级统计**（例如 `AVG(member_cnt_total)`），应进 `CoreNumbers/StepStats`，不必写入每个块实体。

## 3) H/E/F 三库为何要“类似字段”（落库策略建议）

你要求：H/E/F 未来用于画像，不想每次回读 W 源表；因此需要在 H/E/F 的成员实体中固化“成员画像字段投影”。

但注意：`F Members` 通常接近 KeepMembers 全量，若复制过宽字段会显著增大落库体量与索引成本。这里建议走 DP-008（成员字段投影宽度）来平衡“可画像”与“存储/性能”。

推荐默认（供 DP-008 选项 A/B/C 参考）：

- **最小画像投影**（强推荐作为默认）：保留主版本必需字段 + 你明确点名字段 + 与其强相关的计数/比例字段
  - `ip_long / ip_address / IP归属国家 / IP归属运营商`
  - `上报次数 / 设备数量 / 移动网络设备数量 / WiFi设备数量 / VPN设备数量 / 有线网络设备数量 / 异常网络设备数量 / 空网络状态设备数量`
  - `工作时上报次数 / 工作日上报次数 / 周末上报次数 / 深夜上报次数`
  - 建议同时保留一组 ratio 字段（源表已有，且未来画像常用）：`周活跃天数比例/深夜活动比例/工作日周末平均比例/平均每设备上报次数/移动网络设备比例/...`（是否用于块级聚合需 DP-010 固化）

## 4) 需要明确“哪些字段会因切分变动而必须重算”

### 4.1 不随切分变动（成员级）

只要成员集合不变，成员字段（来自 W 源表的行级字段）不变：
- `ip_long` 行级的各种计数/比例字段（设备/上报/工作时/周末/移动等）
- `is_abnormal` 也不因切分变动（由异常表决定）

### 4.2 随切分变动（块级/窗口级/覆盖级）

凡是依赖“成员→块”映射或块边界的聚合字段，都需要随切分重算：
- Natural/Pre/Final 的 `member_cnt_* / valid_cnt / devices_sum_* / reports_sum_* / 各类网络类型设备 sum/ratio`
- Step64 的窗口摘要（head/tail k=5）与触发器指标
- `network_tier_pre/network_tier_final`（SIMPLE 口径复算）
- H Blocks / E Runs 的块级画像字段（因为块集合会变）

建议在所有块级画像实体中增加：
- `metric_contract_version`（或 `contract_version`）
- `profile_calc_ts`
- `profile_source`（natural/final）

以便审计“切分参数变化导致重算”的版本链路。

## 5) 基于实测的 DP 建议（先不固化，等你确认）

- DP-001（中国谓词）：实测 `IP归属国家` 只有 `中国`，建议直接 `选A（IN ('中国')）`。
- DP-002（异常去重）：异常表 `ipv4_bigint` 实测已唯一且无 NULL，建议 `选A（DISTINCT）`（实现最简单且满足主版本“避免扩行”红线）。
- DP-003（设备 NULL）：实测 `设备数量` 全量非 NULL；仍建议合同固定为 `COALESCE(设备数量,0)`（稳健）。
- DP-009（假日占比）：源表只有“周末”，建议你决定是否用“周末≈假日”作为 v2.0 口径。
