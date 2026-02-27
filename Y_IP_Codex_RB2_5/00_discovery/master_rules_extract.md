# 主版本硬规则摘录（RB20 v2.0）

> 本文件从唯一主版本《重构2.md》中抽取“不可违反硬规则/红线”，用于后续合同与 SQL 的一致复用。
> **注意**：任何与本清单冲突的实现都必须停下，走 Decision Points（DP）并写入合同后才能继续。

## A. 总体原则红线

1. **唯一规范**：仅以主版本为准，不引入第二套口径。
2. **先合同再执行**：关键口径/字段映射/阈值/枚举/统计方式必须先形成合同并人类确认，才能全量跑数。
3. **实体库优先**：关键阶段必须实体化（自然块、原子、run、切分事件等），可穿透对账。
4. **禁止幽灵成员**：H/E/F 成员必须来自源表真实行（以 `ip_long` 为成员主键），禁止区间展开造行。
5. **membership vs valid**：异常只标记不删除；所有用于评分/密度/窗口/网络规模/准入判定的指标必须使用 valid（`is_valid=true`）。
6. **F 反连接禁止 BETWEEN/NOT BETWEEN**：必须按 `atom27_id` 等值 anti-join（或合同允许的替代方案）。
7. **Step64 审计不可缺失**：必须记录 `cnt=0` 的 SplitEvents（即使不触发切分也要落审计记录）。

## B. 输入锚点（必须写死）

- 输入 schema：`public`
- W 源表：`public."ip库构建项目_ip源表_20250811_20250824_v2_1"`
- A 异常表：`public."ip库构建项目_异常ip表_20250811_20250824_v2"`

> **中国过滤谓词**：使用字段 `IP归属国家`，但“哪些取值算中国”必须走 DP，由采样 distinct 值后人类确认，并写入 Config 合同。

## C. 网络规模评估（SIMPLE / network_tier）红线（最关键，必须锁死并验）

### C1. 输入指标（全部 valid 口径）

对任意块实体（自然块/最终块）：

- `valid_cnt = COUNT(DISTINCT ip_long)`（仅 `is_valid=true`）
- `devices_sum_valid = SUM(设备数量)`（仅 `is_valid=true`；NULL 处理是否 `COALESCE` 必须在 Metric Contract 写死）
- `density = devices_sum_valid / NULLIF(valid_cnt,0)`；当 `valid_cnt=0` 时 `density` 必须为 **NULL**

### C2. 分桶与映射（边界不可歧义）

- `wA`（按 `valid_cnt` 分桶）：1~16→1；17~48→2；49~128→4；129~512→8；≥513→16  
- `wD`（按 `density` 分桶）：≤3.5→1；(3.5,6.5]→2；(6.5,30]→4；(30,200]→16；>200→32  
- `simple_score = wA + wD`
- `network_tier`：≥40→超大网络；[30,40)→大型；[20,30)→中型；[10,20)→小型；[0,10)→微型

### C3. valid_cnt=0 固定处理（必须写死）

当 `valid_cnt=0`：

- `devices_sum_valid` 输出必须标准化为 `0`
- `density=NULL`，`wA/wD/simple_score=NULL`
- `network_tier='无效块'`（固定枚举，不得用 NULL/空串）

并且：
- 不得进入 H（`network_tier_final='无效块'` 禁止）
- 不得参与任何依赖 valid 指标的准入判定（/27 密度、切分窗口、网络规模准入等）
- 实体与映射必须保留用于审计（Drop 不等于删除）

### C4. pre/final 一致性（必须同口径复算）

- `network_tier_pre`：RB20_03（Pre Profile）阶段对自然块计算  
- `network_tier_final`：RB20_04P（Final Profile）阶段对最终块 **按同一口径重新计算**  
- 两处必须复用同一套阈值/边界/NULL 规则；必须通过抽样与断言证明一致性与边界正确性。

## D. Step64 切分红线

- 切分只作用于 PreH 候选块集合（用于裁剪范围提升效率）
- k=5 左右窗口仅取 **valid** IP（不足则 cnt<k；cnt=0 指标为 NULL）
- 三触发器（Report/Mobile/Operator）任一命中即切分
- 必须先构建 block×bucket64 的 head/tail 窗口摘要实体库，避免反复扫描整块
- 切分事件必须全量记录（含不触发与 cnt=0），用于审计与评估

## E. E/F 红线

- `atom27_id = floor(ip_long/32)`，原子密度 `valid_ip_cnt/32 >= 0.2`（等价 valid_ip_cnt>=7）
- 连续 run 最小长度=3；短 run 仍保留并标记 short_run
- F 必须用 `atom27_id` 等值 anti-join（禁止 BETWEEN/NOT BETWEEN），且不得造行

## F. 终验收（QA_Assert STOP）

- H/E/F 两两交集=0
- 守恒：KeepMembers = H_cov ∪ E_cov ∪ F
- 无幽灵：上述集合必须是 Source Members 子集
- Drop 成员映射不蒸发（Drop 的 member 也必须进入 Map_All）
- 切分不退化：SplitEvents 必须包含 cnt=0；final tier 分布不得全空/全同；单独统计 '无效块' 数量用于审计
