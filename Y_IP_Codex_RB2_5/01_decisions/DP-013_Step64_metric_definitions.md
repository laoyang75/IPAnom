# DP-013：Step64 指标定义（ratio_report / cv / mobile_diff / mobile_cnt_ratio）

背景（主版本 6.2 明确要求必须固化到 Metric Contract）：
- Step64 触发器条件写死为：
  - Report：`ratio_report > 4 AND cvL < 1.1 AND cvR < 1.1`
  - Mobile：`mobile_diff > 0.5 OR mobile_cnt_ratio > 4`
  - Operator：`opL/opR` 不同
- 但 `ratio_report/cv/mobile_diff/mobile_cnt_ratio` 的计算公式未在主版本正文展开，需要在任务阶段合同中补齐并确认。

决策：按你“其他都按推荐/继续执行”的指令，本期固化为 **选A**（与 DP-010 分母语义一致）。

你只需回复：`选A` / `选B` / `选C`

## 统一约束（固定）

- 每个候选切点以 `cut_ip_long=(bucket64+1)*64`
- 左右窗口各取 `k=5` 个 **valid** IP（不足则 cnt<k）
- 当 `cnt=0` 时：对应指标为 NULL，默认不触发，但必须写入 SplitEvents 审计行（主版本红线）

## 选项

### A（推荐，已采纳）

**Report 指标：按“每 IP 上报强度”定义**

- `meanL = AVG(上报次数)`（左窗口 k 个 IP 的均值）
- `meanR = AVG(上报次数)`（右窗口 k 个 IP 的均值）
- `cvL = STDDEV_SAMP(上报次数) / NULLIF(meanL,0)`
- `cvR = STDDEV_SAMP(上报次数) / NULLIF(meanR,0)`
- `ratio_report = GREATEST(meanR/NULLIF(meanL,0), meanL/NULLIF(meanR,0))`

**Mobile 指标：按“移动设备行为结构”定义（分母=设备量）**

- `mratioL = SUM(移动网络设备数量) / NULLIF(SUM(设备数量),0)`（左窗口）
- `mratioR = SUM(移动网络设备数量) / NULLIF(SUM(设备数量),0)`（右窗口）
- `mobile_diff = ABS(mratioR - mratioL)`
- `mobile_cnt_ratio = GREATEST(mobileR/NULLIF(mobileL,0), mobileL/NULLIF(mobileR,0))`
  - 其中 `mobileL = SUM(移动网络设备数量)`，`mobileR = SUM(移动网络设备数量)`

说明：与 DP-010 一致——密度类用 IP 数做分母；“移动设备占比”用设备量做分母；上报类占比用上报量做分母。

### B（备选：Report 用 sum 而非 mean）

- `ratio_report = GREATEST(sumR/NULLIF(sumL,0), sumL/NULLIF(sumR,0))`，其中 `sumL/sumR` 为窗口上报次数之和
- `cvL/cvR` 同 A

### C（备选：Report 用 median）

- `ratio_report = GREATEST(medR/NULLIF(medL,0), medL/NULLIF(medR,0))`
- `cvL/cvR` 同 A

## 固化位置

- `Y_IP_Codex_RB2_5/02_contracts/metric_contract_draft_v1.md`
- `Y_IP_Codex_RB2_5/02_contracts/metric_contract_v1.md`
- `Y_IP_Codex_RB2_5/CHANGELOG.md`

