# RB20 v2.0 — Metric Contract（Draft v1：字段口径草案）

唯一规范：`Y_IP_Codex_RB2_5/重构2.md`

> 说明：本草案把“字段研究新增的画像字段（移动覆盖/工作时占比/周末占比等）”补成可复用口径。  
> SIMPLE/network_tier 与 Step64 规则不得改写，仅做引用与落地约束。

## 1) membership vs valid（主版本 2.1）

- `is_abnormal`：`ip_long` 是否命中异常表（异常只标记不删除）
- `is_valid = NOT is_abnormal`
- 所有用于评分/密度/切分窗口/网络规模评估/准入判定的指标：统一使用 `is_valid=true`

## 2) SIMPLE / network_tier（主版本 2.5：唯一口径，pre/final 必须一致）

对任意块实体（自然块/最终块），定义：

- `valid_cnt = COUNT(DISTINCT ip_long) FILTER (WHERE is_valid)`
- `devices_sum_valid = SUM(<设备数量表达式>) FILTER (WHERE is_valid)`
- `density = devices_sum_valid / NULLIF(valid_cnt,0)`
- `wA`：按 `valid_cnt` 分桶（2.5.2）
- `wD`：按 `density` 分桶（2.5.3）
- `simple_score = wA + wD`（仅当 `wA/wD` 均非 NULL）
- `network_tier`：按 `simple_score` 映射（2.5.4）

`valid_cnt=0` 固定规则（2.5.5）：
- `devices_sum_valid` 输出必须标准化为 `0`
- `density/wA/wD/simple_score = NULL`
- `network_tier='无效块'`
- 且不得进入 H；不得参与任何依赖 valid 的准入判定；实体与映射必须保留用于审计

> DP-003 将决定 `<设备数量表达式>` 是否为 `COALESCE(设备数量,0)`（实测设备数量无 NULL，但合同仍建议写死）。

DP-003 已确认选 A：`<设备数量表达式> = COALESCE(设备数量,0)`。

## 3) 新增“画像聚合字段 v1”（用于 Natural/Pre/Final/H）

### 3.1 块级 sum 字段（membership / valid）

对任意块（自然/最终），基于其成员集合 `members(block)`：

- `*_sum_total = SUM(x) over members(block)`（不剔除异常）
- `*_sum_valid = SUM(x) FILTER (WHERE is_valid) over members(block)`

字段映射（x 来自 W 源表）：

- `reports_sum_*`：`上报次数`
- `devices_sum_*`：`设备数量`
- `mobile_devices_sum_*`：`移动网络设备数量`
- `wifi_devices_sum_*`：`WiFi设备数量`
- `vpn_devices_sum_*`：`VPN设备数量`
- `wired_devices_sum_*`：`有线网络设备数量`
- `abnormal_net_devices_sum_*`：`异常网络设备数量`
- `empty_net_devices_sum_*`：`空网络状态设备数量`
- `worktime_reports_sum_*`：`工作时上报次数`
- `workday_reports_sum_*`：`工作日上报次数`
- `weekend_reports_sum_*`：`周末上报次数`
- `late_night_reports_sum_*`：`深夜上报次数`

边界与标准化（建议，需你确认后固化）：
- 当 `valid_cnt=0` 时，所有 `*_sum_valid` 统一 `COALESCE(sum,0)` 标准化为 `0`（DP-011 选 A）
- 当块内 membership 成员数为 0（理论上不应发生），`*_sum_total` 统一 `COALESCE(sum,0)` 标准化为 0（DP-011 选 A）

### 3.2 块级 ratio 字段（由 sum 派生，不建议落表）

为便于画像读取，推荐在查询/视图层计算（不一定落表）。  
注意：密度类与比例类分母不同；你已澄清“密度以 IP 数量为分母；移动设备占比以设备量为分母；移动上报占比（若存在）以总上报量为分母”。下列写法对应 `DP-010` 方案 A。

命名与分母约定（固定规则，避免标题歧义）：
- `*_density_*`：分母固定为 `valid_cnt`（IP 数量）
- `*_device_ratio_*`：分母固定为 `devices_sum_valid`（设备量）
- `*_report_ratio_*`：分母固定为 `reports_sum_valid`（上报量）

- 密度类（分母=IP数量）：
  - `device_density_valid = devices_sum_valid / NULLIF(valid_cnt,0)`（主版本 SIMPLE 的 `density`）
  - `report_density_valid = reports_sum_valid / NULLIF(valid_cnt,0)`
- 行为比例类（分母=行为总量）：
  - `mobile_device_ratio_valid = mobile_devices_sum_valid / NULLIF(devices_sum_valid,0)`
  - `wifi_ratio_valid = wifi_devices_sum_valid / NULLIF(devices_sum_valid,0)`
  - `vpn_ratio_valid = vpn_devices_sum_valid / NULLIF(devices_sum_valid,0)`
  - `wired_ratio_valid = wired_devices_sum_valid / NULLIF(devices_sum_valid,0)`
  - `worktime_report_ratio_valid = worktime_reports_sum_valid / NULLIF(reports_sum_valid,0)`
  - `weekend_report_ratio_valid = weekend_reports_sum_valid / NULLIF(reports_sum_valid,0)`（DP-009 将决定是否同时输出 `holiday_*`）
  - `late_night_report_ratio_valid = late_night_reports_sum_valid / NULLIF(reports_sum_valid,0)`

（可选，若未来需要“移动上报占比”作为画像字段）：
- `mobile_report_ratio_valid = mobile_reports_sum_valid / NULLIF(reports_sum_valid,0)`
  - 前提：W 源表存在可映射的“移动上报次数”字段；当前字段研究未发现该字段，因此本版本不落表、只保留命名槽位（不得自行构造）。

> “假日占比”是否等同于周末占比由 DP-009 固化（你已回复 DP-009 选 A，将在正式合同中写死）。

## 3.3 “假日占比”（DP-009 选 A）

- `holiday_reports_sum_valid = weekend_reports_sum_valid`
- `holiday_report_ratio_valid = weekend_report_ratio_valid`


## 4) 成员级派生字段（工程字段）

- `atom27_id = floor(ip_long/32)`
- `bucket64 = floor(ip_long/64)`

## 5) H/E/F 画像复用策略（你要求的“不要回读源表”）

- 你已选择 DP-008 选 C：W 源表全量镜像落在 `rb20_v2_5.source_members`（每个成员只存一份宽字段），避免任何画像回读 `public` 源表。
- H/E/F Members 等集合表本体存“集合+归属字段”，画像读取宽字段时使用视图（宽字段来源均为 `source_members`）：
  - `rb20_v2_5.h_members_wide` / `rb20_v2_5.e_members_wide` / `rb20_v2_5.f_members_wide`
- H Blocks / profile_pre / profile_final：携带块级 sum 字段 + SIMPLE/network_tier 字段，画像直接读 profile 表即可。

## 6) 何时需要重算（切分/口径变更的影响）

必须重算（依赖块边界或成员→块映射）：
- `profile_pre/profile_final/h_blocks` 的所有块级聚合字段（sum/ratio）与 SIMPLE/network_tier
- `window_headtail_64` 与 `split_events_64`

无需重算（只要成员集合与异常标记不变）：
- 成员级字段（来自 W 的行级字段）与 `is_abnormal/is_valid/atom27_id/bucket64`

建议在所有块级画像表中固化：
- `contract_version`（含 Metric 版本）
- `created_at`（计算时间）

## 7) Step64 指标定义（DP-013 选 A）

对每个候选切点 `cut_ip_long=(bucket64+1)*64`，左右窗口各取 `k=5` 个 valid IP（不足则 cnt<k）。

当任一侧 `cnt=0`：对应指标为 NULL，默认不触发，但必须在 `split_events_64` 写入审计行（主版本红线）。

### 7.1 Report 指标（按“每 IP 上报强度”）

- `meanL = AVG(上报次数)`（左窗口）
- `meanR = AVG(上报次数)`（右窗口）
- `cvL = STDDEV_SAMP(上报次数) / NULLIF(meanL,0)`
- `cvR = STDDEV_SAMP(上报次数) / NULLIF(meanR,0)`
- `ratio_report = GREATEST(meanR/NULLIF(meanL,0), meanL/NULLIF(meanR,0))`

### 7.2 Mobile 指标（按“移动设备占比”，分母=设备量）

- `mratioL = SUM(移动网络设备数量) / NULLIF(SUM(设备数量),0)`（左窗口）
- `mratioR = SUM(移动网络设备数量) / NULLIF(SUM(设备数量),0)`（右窗口）
- `mobile_diff = ABS(mratioR - mratioL)`
- `mobile_cnt_ratio = GREATEST(mobileR/NULLIF(mobileL,0), mobileL/NULLIF(mobileR,0))`
  - `mobileL = SUM(移动网络设备数量)`，`mobileR = SUM(移动网络设备数量)`
