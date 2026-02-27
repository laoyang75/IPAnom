# Work Plan v1 — RB20 重构 v2.0（可评估 / 可迭代 / 留有余地）

> **唯一规范**：以主版本《重构2.md》为唯一口径；本 Work Plan 仅做“可落地执行”的模块拆解、评估产物与 Gate 机制设计。  
> **执行纪律**：关键口径/字段映射/阈值/枚举/统计方式 —— 先合同（Contracts）并人类确认，再允许跑全量/长耗时任务。

---

## 0. 目标与成功标准（本计划的“可评估”定义）

### 0.1 最终交付

- 在 PostgreSQL 内可稳定复跑地交付 RB20 v2.0 的关键实体链路与三库：
  - 自然块链路：NaturalBlock → PreProfile/PreH → Window → SplitEvents → FinalBlock → FinalProfile
  - 最终三库：H Blocks + H Members / E Atoms + E Runs + E Members / F Members
  - 审计验收：StepStats / RuleImpact / QA_Assert(STOP) / CoreNumbers

### 0.2 最硬红线（必须验）

- **网络规模评估（SIMPLE/network_tier）**：pre/final 两处同口径复算；`valid_cnt=0` 的“无效块”规则严格执行；并且验证“不准入 H / 不参与依赖 valid 的准入”。  
- **禁止幽灵**：H/E/F 成员只来自源表真实 `ip_long` 行，任何反连接/切分都不得区间展开造行。  
- **Step64 审计**：SplitEvents 必须覆盖 `cnt=0` 记录（即便不触发也要落）。

---

## 1. 目录化项目管理（落盘与迭代纪律）

- 本计划与所有产物落盘在 `Y_IP_Codex_RB2_5/`
- 每完成一个阶段必须：
  1) 输出阶段报告到 `06_reports/`
  2) 更新本 Work Plan 为 v2/v3… 并在 `CHANGELOG.md` 记录变化原因、影响范围、对应 DP

---

## 2. 合同机制（Contracts）— “先合同再执行”的可操作化

### 2.1 合同类型与落盘位置

- **Schema Contract**（`02_contracts/schema_contract_v1.md` + DDL/索引：`03_sql/00_contracts/01_ddl_rb20_v2_full.sql`、`03_sql/00_contracts/02_indexes_rb20_v2.sql`）
  - 覆盖：所有关键实体表的必备字段语义集合、主键/唯一性、最小可用索引策略
- **Metric Contract**（`02_contracts/metric_contract_v1.md`）
  - 覆盖：membership vs valid、SIMPLE/network_tier 全定义（含阈值/边界/NULL 规则/无效块枚举）、Step64 指标与 cnt=0 规则、E 原子密度与 run 合并规则、F anti-join 规则
- **Report Contract**（`02_contracts/report_contract_v1.md` + 报告模板：`06_reports/01_gate1_sample_template.md`、`06_reports/02_gate2_full_pipeline_template.md`）
  - 覆盖：每阶段 StepStats/RuleImpact/CoreNumbers 的指标、SQL 与解释文字（中文）

> **合同锁定策略**：合同文件版本化（带 `contract_version`），所有执行 SQL 必须引用同一个版本（通过 `rb20_config` 表或 SQL 常量），避免“跑着跑着口径漂移”。

### 2.2 合同 Gate（放行条件）

- Gate-0（进入全量前）必须满足：
  1) Schema/Metric/Report 合同已生成并人类确认
  2) 至少用小样/模拟数据验证：SIMPLE/network_tier 边界与 `valid_cnt=0` 规则正确
  3) 明确并固化所有 DP（中国过滤谓词、异常去重策略、devices NULL 处理、PreH/KeepDrop 规则等）

---

## 3. Decision Points（DP）机制（遇到歧义必须停）

### 3.1 触发条件（任何一个满足就必须 DP）

- 主版本明确“未写死、需任务阶段确认”的事项（如中国过滤谓词）
- NULL/重复/空值处理会影响画像、准入、切分或守恒对账
- 性能策略存在多方案且风险/锁表/耗时差异明显（大索引、全表重写、物化策略等）

### 3.2 预置 DP 清单（v1 初版）

> v1 仅列出将要产出的 DP；实际选项将以 DB 实测（distinct 值、行数、NULL 分布、join 扩行风险）形成“可一眼选择”的 A/B/C。

- **DP-001 中国过滤谓词**：`IP归属国家` 的哪些取值视为中国
- **DP-002 异常表去重/聚合策略**：`ipv4_bigint` 去重 vs 聚合（以及 NULL 行处理）
- **DP-003 `设备数量` NULL 处理**：SUM 时是否 `COALESCE(设备数量,0)`
- **DP-004 PreH 候选块选择规则**：默认 `Keep & valid_cnt>0`，是否进一步收缩（仅对某些 tier 切分以提速）
- **DP-005 除 `valid_cnt=0` 外的 Keep/Drop**：默认“其余全 Keep”（最保守），是否增加 Drop 条件
- **DP-006 输出 schema 命名与权限/隔离方式**：独立 schema vs public 下前缀表名
- **DP-007 F 反连接备选实现**：`atom27_id` 等值 anti-join（默认）vs range+GiST（备选）
- **DP-008 H/E/F Members 字段投影宽度**：最小画像投影 vs 扩展投影 vs 全量镜像（存储/性能权衡）
- **DP-009 假日占比口径**：周末≈假日 vs 不提供 vs 占位 NULL
- **DP-010 块级比例字段聚合口径**：从数量重算 vs 简单平均 vs 加权平均
- **DP-011 块级 sum/ratio 空集与 NULL 处理**：sum 标准化为 0 与 ratio 边界策略
- **DP-012 ShardPlan 两轮 5% 调整规则**：如何判定大/小 shard 与如何移动相邻边界

---

## 4. 模块化阶段计划（每阶段：目的/产物/验收/Gate/风险/DB 动作）

> 说明：本节拆成“可并行 per-shard”与“全局”两类；并明确哪些需要 Runbook 交给人类并行/长跑。

### 阶段 00：Discovery + Contracts + ShardPlan（全局）

- **目的**
  - 用最小代价获取“口径决策所需事实”：distinct 值、NULL/重复分布、行数级别、潜在 join 扩行点
  - 生成三类合同（Schema/Metric/Report）并获得人类确认
  - 生成 64 分片 ShardPlan，并完成断言（无空 shard/无重叠）

- **DB 动作类型**
  - 轻量抽样 / 统计（`COUNT`, `approx distinct`, 分布）
  - DDL：创建输出 schema、基础设施表（Run/Config/ShardPlan）
  - 必要索引：仅对极少数高杠杆列建索引（如 `ip_long`, `atom27_id`, `run_id+shard_id` 组合）

- **产物（落表 + 落盘）**
  - `RunMeta / Config / ShardPlan` 实体（落表）
  - 合同：`02_contracts/schema_contract_v1.md`、`02_contracts/metric_contract_v1.md`、`02_contracts/report_contract_v1.md`
  - DDL/索引：`03_sql/00_contracts/01_ddl_rb20_v2_full.sql`、`03_sql/00_contracts/02_indexes_rb20_v2.sql`
  - DP 文件（`01_decisions/` 目录下，DP-001～DP-014）
  - `06_reports/00_gate0_contracts.md`

- **验收 / Gate**
  - 合同被确认并锁定版本
  - ShardPlan 满足：覆盖全范围、无重叠、两次 5% 调整已执行且记录
  - 用小样验证 network_tier 口径（含边界与 `valid_cnt=0`）

- **风险/性能策略**
  - 任何可能全表扫描的统计都先 `LIMIT` 抽样 + `EXPLAIN`，再决定是否做全量
  - 大索引/物化如有必要，先出 Runbook（避免锁表/长耗时）

- **需要 Runbook 的情况**
  - 若源表极大且缺少 `ip_long` 索引：建索引可能长耗时 → 出 Runbook

---

### 阶段 01：RB20_01 Source Members（per-shard）

- **目的**
  - 中国过滤（DP-001 固化后）+ 异常标记（只标记不删除），生成 `is_abnormal` 与 `is_valid`

- **输入 / 输出**
  - 输入：W 源表 + A 异常表（先去重避免 join 扩行）
  - 输出：SourceMembers(run_id, shard_id, ip_long, …, is_abnormal, is_valid)

- **验收**
  - `is_valid = NOT is_abnormal` 一致
  - 异常表 join 不扩行（对比 join 前后行数；异常表去重实体存在）
  - StepStats：每 shard 的成员数、异常数、valid 数

- **风险/性能策略**
  - 必要索引：SourceMembers `(run_id, shard_id, ip_long)`；异常去重表对 `ipv4_bigint` 唯一索引
  - 幂等：每次重跑清理 `run_id+shard_id`

- **Runbook**
  - 该阶段通常 per-shard 并行跑；如单 shard 仍重，可出 Runbook 并发跑 32 shard/批

---

### 阶段 02：RB20_02 Natural Blocks（per-shard）

- **目的**
  - 在同 shard 内按 `ip_long` 连续性识别自然块（相邻差 1），并实体化块与成员映射

- **输出**
  - NaturalBlockEntity（块级）
  - BlockMemberMap（成员→自然块）

- **验收**
  - 禁止幽灵：Map 的 `ip_long` 必须来自 SourceMembers
  - CIDR 块统计口径：member_cnt_total>=4 的自然块数可按合同报表输出
  - 对少量样本验证：块边界与相邻差 1 断点一致

- **风险/性能策略**
  - 排序/窗口函数可能重：确保 per-shard 范围裁剪 + 合理索引
  - 大 shard：必要时拆更小 shard 重跑（但不得突破 ShardPlan 规则；只能重生成 ShardPlan 并重跑下游）

---

### 阶段 03：RB20_03 Pre Profile + PreH（per-shard）

- **目的**
  - 对自然块计算 `network_tier_pre`（SIMPLE 口径）；输出 Keep/Drop 与 drop_reason；生成 PreH 候选块集合用于裁剪切分范围

- **关键硬规则**
  - 画像输入指标必须 valid 口径；`valid_cnt=0` → `network_tier_pre='无效块'` 且强制 Drop，drop_reason=ALL_ABNORMAL_BLOCK

- **验收**
  - `valid_cnt=0` 的块全部 Drop 且不进入 PreH
  - network_tier_pre 字段齐全（valid_cnt/devices_sum_valid/density/wA/wD/simple_score/network_tier_pre）
  - 抽样复算一致：同一块用同一 SQL 复算 network_tier_pre 与落表结果一致

---

### 阶段 11：RB20_11 HeadTail Window Entity（per-shard，聚焦 PreH）

- **目的**
  - 为 Step64 切分准备窗口摘要：block×bucket64 的 head/tail k=5 valid 成员统计与“唯一运营商判定”

- **验收**
  - 对每个评估切点：左右窗口 cnt 允许 <5 或 =0；cnt=0 指标为 NULL
  - 运营商唯一性：distinct=1 才写入 op，否则 NULL

- **风险/性能策略**
  - 该步骤可能最重（窗口/排序/聚合）；默认产出 Runbook 便于并行与分批

---

### 阶段 04：RB20_04 SplitEvents + FinalBlock（per-shard）

- **目的**
  - 基于窗口摘要评估每个 cut 点的三触发器；记录 SplitEvents（含 cnt=0）；对命中 cut 点执行多切点切分，产出最终块实体

- **关键硬规则**
  - SplitEvents 必须记录 cnt=0（即使不触发）
  - FinalBlock id：`block_id_final = block_id_parent || '_' || lpad(segment_seq,3,'0')`

- **验收**
  - 切分数、触发器分布、cnt=0 覆盖率在报告中可解释
  - 切分前后块数变化合理（不退化、不爆炸）

---

### 阶段 04P：RB20_04P Final Profile（per-shard）

- **目的**
  - 对最终块按同一 SIMPLE 口径计算 `network_tier_final`（与 pre 完全一致的阈值/边界/NULL 规则）

- **验收（最关键）**
  - 复用同一计算片段/UDF：pre 与 final 口径同源
  - 抽样对比：对同一最终块，离线复算与落表一致；`valid_cnt=0` 输出固定规则正确
  - tier 分布非全空/全同；'无效块' 单独统计用于审计

---

### 阶段 05：RB20_05 H Blocks + H Members（全局）

- **目的**
  - 以 `network_tier_final='中型网络'` 作为唯一准入定义产出 H；禁止叠加其它过滤；并排除 '无效块'

- **验收**
  - H 块只来自 FinalProfile 且 tier=中型网络
  - H Members 仅来自 SourceMembers（无幽灵）

---

### 阶段 06：RB20_06 R1 Residue（per-shard）

- **目的**
  - R1 = KeepMembers \ H_cov（成员级残差）

- **验收**
  - 守恒准备：KeepMembers 被正确扣除 H_cov（member 层面的 DISTINCT ip_long）

---

### 阶段 07：RB20_07 E Atoms + Runs + Members（per-shard + 汇总）

- **目的**
  - 在 R1 上按 /27 原子密度准入，合并连续原子成 runs；输出 E 覆盖成员

- **验收**
  - 原子密度 valid 口径正确（valid_ip_cnt>=7）
  - run_len<3 的 short_run 保留并标记
  - E Members 只来自 SourceMembers

---

### 阶段 08：RB20_08 F Members（per-shard）

- **目的**
  - F = R1 \ E_cov；必须用 `atom27_id` 等值 anti-join（禁止 BETWEEN/NOT BETWEEN）

- **验收**
  - 反连接实现符合合同；无幽灵；F 与 E_cov/H_cov 互斥

---

### 阶段 99：RB20_99 QA_Assert + CoreNumbers（全局）

- **目的**
  - 终验收：互斥 / 守恒 / 无幽灵 / Drop 映射不蒸发 / 切分不退化

- **验收**
  - QA_Assert 任何 STOP=FAIL 立即阻断；并在报告中给出失败定位 SQL

---

## 5. 并行/长任务拆分策略（Runbook 机制）

### 5.1 何时必须出 Runbook（交给人类/并行执行者）

- 大规模索引、可能长时间锁表/重写
- per-shard 步骤在单会话耗时过长（如窗口摘要、切分评估）
- 需要 64 shard 并发跑数、两批 32 shard 的 orchestrator 调度

### 5.2 Runbook 最小模板（将落在 `04_runbook/`）

- 目标、输入输出、前置条件（合同版本、run_id、shard 范围）
- SQL 包（按 shard_id 参数化）
- 幂等清理语句（DELETE WHERE run_id=? AND shard_id=?）
- 监控与回滚策略（通过 StepStats/进度表）
- 完成后需要回传的结果（StepStats 汇总、关键分布）

---

## 6. network_tier 不丢不做错的验证策略（写死在计划里）

1) **同源实现**：把 SIMPLE/network_tier 的计算实现成“单一 SQL 片段”或“稳定函数/视图”，由 Metric Contract 锁定版本；pre/final 两处只调用它。  
2) **边界单元测试**：用 VALUES 构造覆盖边界（valid_cnt=16/17/48/49/128/129/512/513；density=3.5/3.5001/6.5/6.5001/30/30.0001/200/200.0001；以及 valid_cnt=0）并断言输出 tier。  
3) **pre/final 一致性抽样**：对若干 shard 抽样块，分别用（a）落表结果、（b）同口径复算 SQL，对比差异=0；差异>0 立即出报告并定位。  
4) **valid_cnt=0 边界审计**：单独统计 `network_tier in ('无效块')` 的块数量与成员数；并断言这些块：
   - 不进入 PreH、不进入 H
   - 不参与依赖 valid 的准入（通过 join/断言证明）

---

## 7. v1 → v2 的预计更新点（留有余地）

- 合同确认后补充：具体表名、字段类型、主键/索引细节
- 基于 DB 实测调整：
  - ShardPlan 的分布与热点 shard 的并行策略
  - 窗口摘要/切分评估的物化与索引策略
  - 报告维度（按国家值/运营商/网络 tier/触发器类型/分片等）
- 任何调整必须：
  - 不突破主版本硬规则
  - 通过 DP 固化到合同并记录 CHANGELOG
