# DP-010：块级画像指标的“分母选择/聚合口径”（密度 vs 比例）

决策：按你的说明已确认 **选A**（密度分母=IP数量；“移动设备占比”分母=设备量；“移动上报占比”若存在则分母=上报量；其它上报类占比分母=上报量）。

背景：块级画像（Natural/Pre/Final/H）里同时存在两类指标，分母不应混用，否则画像解释会漂移：

1) **密度类（以 IP 数量为分母）**：反映“该 IP 段每个 IP 的强度”，例如：
   - 设备密度：`设备量 / IP数量`
   - 上报密度：`上报量 / IP数量`
   -（主版本 SIMPLE 的 `density = devices_sum_valid / valid_cnt` 就属于密度类）

2) **行为比例类（以行为总量为分母）**：反映“该 IP 段的设备/上报行为结构”，例如：
   - 移动占比：`移动相关量 / 总设备相关量`（本期输入字段只有“移动网络设备数量”，没有“移动上报次数”）
   - 工作时占比：`工作时上报次数 / 总上报次数`
   - 周末（假日）占比：`周末上报次数 / 总上报次数`

主版本约束：
- 不能引入第二套阈值/枚举解释
- 口径必须写入 Metric Contract，并在 pre/final 两处一致复用

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（推荐：按“密度/比例”分流分母）
规则（valid 口径为主；membership 仅用于审计）：

- **密度类（分母=IP数量）**
  - `device_density_valid = devices_sum_valid / NULLIF(valid_cnt,0)`
  - `report_density_valid = reports_sum_valid / NULLIF(valid_cnt,0)`

- **行为比例类（分母=行为总量）**
  - 移动设备占比（设备口径；已由源字段 `移动网络设备比例` 语义校验确认）：
    - `mobile_device_ratio_valid = mobile_devices_sum_valid / NULLIF(devices_sum_valid,0)`
  - 工作时占比：
    - `worktime_report_ratio_valid = worktime_reports_sum_valid / NULLIF(reports_sum_valid,0)`
  - 周末/假日占比：
    - `weekend_report_ratio_valid = weekend_reports_sum_valid / NULLIF(reports_sum_valid,0)`

说明：这与“密度是 IP 为基础、移动比例是行为结构”的意图一致。

### B（全部都以 IP 数量为分母）
规则：
- 无论密度还是移动/工作时/周末等比例，一律用 `*_sum_valid / valid_cnt`
- 优点：统一；缺点：移动/工作时/周末会从“结构比例”变成“每 IP 强度”，语义不同

### C（全部都以 行为总量 为分母）
规则：
- 无论设备密度/上报密度也不用 IP 数量，而统一用某个行为总量（例如设备或上报）作为分母
- 不推荐：会破坏主版本 SIMPLE 的 `density` 定义（与 2.5 冲突风险高）

## 固化位置（被选中后）

- `02_contracts/metric_contract.*`：每类 ratio 的聚合规则与边界处理
- `02_contracts/schema_contract.*`：是否在 `profile_pre/profile_final/h_blocks` 固化 ratio 字段或仅固化 sum
- `CHANGELOG.md`：记录原因与影响
