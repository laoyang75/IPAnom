## 0. 设计总原则

1. **研究单位=库（H/E/F）**
   默认所有视图都围绕「这个库是什么样」「为什么进这个库」「Pipeline 每步如何塑形」「边界在哪里」「如何修正并沉淀规则」。

2. **宏观→中观→微观的下钻路径固定化**

   * 宏观：库总体画像、对比、漏斗/流转、问题雷达
   * 中观：按 block_final（H）、atom27 / e_run（E）、atom27（F）聚合的结构性分析
   * 微观：回到现有 Explorer（IP / ip_long / block）做溯源复查

3. **可复查=全程留痕 + 可复现实验**
   所有关键行为（筛选、下钻、创建问题、提出假设、运行模拟、输出规则）都进入**研究会话 Session**，形成可审计的“研究档案”。

4. **研究结论必须能变成“规则包 RulePack”并可评估影响**
   规则包不只是文字结论：要能输出结构化 YAML/JSON，并能在 DB 上做“what-if”影响评估（至少对能从现有表直接推导的规则）。

---

## 7.1 可视化界面架构设计

### 7.1.1 与现有 WebUI 的关系：**扩展为新 Tab（最小重构）**

你当前 SPA 有 3 个 Tab：Dashboard / Explorer / QA。建议新增 2 个：

* **🧪 库画像研究（Library Lab）**：本方案核心
* **🗂 研究档案（Research Archive）**：查看/复盘 Session、问题、规则包、实验结果（也可先合并到 Library Lab 的一个子页，后期再拆）

> 不建议独立新站点：你现有暗色主题与数据溯源链路（Explorer）是资产，应该复用。

---

### 7.1.2 顶层导航与布局（ASCII 草图）

**顶层：Run + 库 + 会话 Session 是三大“上下文锚点”**

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ TopBar:  Run [rb20_v2_5_xxx ▼]   Library [H ▼]   Session [S-2026... ▼]        │
│ Tabs :  📊协同看板 | 🧪库画像研究 | 🔍数据探查 | ✅QA断言 | 🗂研究档案         │
├───────────────┬───────────────────────────────────────────────┬──────────────┤
│ Left Panel    │ Main Canvas                                    │ Right Panel  │
│ (Filters)     │ SubTabs: 概览 | 画像 | 流转 | 问题 | 样本 | 规则 │ (Agent+Notes)│
│               │                                               │              │
│ - 维度筛选     │ [charts / tables / drilldowns]                │ - Chat        │
│ - 保存视图     │                                               │ - Action cards│
│ - 快捷下钻     │                                               │ - Log timeline│
└───────────────┴───────────────────────────────────────────────┴──────────────┘
```

#### Left Panel（筛选与视图管理）

* **库级筛选（默认轻量）**：operator、is_valid、is_abnormal、report/device/mobile 的范围、时间（created_at 可选）
* **结构对象筛选（按库不同）**：

  * H：block_id_final / simple_score / density / valid_cnt / split_trigger
  * E：atom_density / atom_valid_cnt / e_run_len / e_run_id
  * F：atom_density / “差一点进 E 的原因”（密度不够 vs run 不够）
* **Saved Views**：保存当前筛选+图表组合（进入 Session 留痕）

#### Main Canvas（研究流程主画布）

* **概览**：库 KPI + 与其他库差异摘要
* **画像**：核心分布图谱（“这个库是什么样”）
* **流转**：Pipeline 影响（“为什么会变成这样”）
* **问题**：自动检测与告警（“哪里不对劲”）
* **样本**：代表性样本集合（“拿得出手的例子”）
* **规则**：结论沉淀与影响评估（“能规模化”）

#### Right Panel（Agent + Notes：研究闭环）

* **对话区（探索/解释/生成实验/生成规则包）**
* **结构化 Action Cards**（一键发起后端实验、创建问题、生成规则草案）
* **Session Timeline**（实时记录筛选、下钻、生成的图表快照、实验结果、Agent 输出摘要）

---

### 7.1.3 信息密度分层（避免“宏观看板”重演）

* **概览**：最多 12 张 KPI 卡 + 3~5 张关键图（非常克制）
* **画像/流转**：每页 6~10 张图，必须支持：

  * 框选/点击下钻（过滤条件写入 Left Panel）
  * 一键“加入样本篮”“创建问题”
* **问题/样本/规则**：以表格 + 详情 Drawer 为主（便于审计、复查）

---

## 7.2 库级画像设计（H/E/F 分别怎么画）

### 7.2.1 统一的“库画像指纹”模块（H/E/F 都有）

不管哪个库，都先回答 4 个问题：

1. **规模与质量**

* members_total（库成员数）
* valid_ratio、abnormal_ratio、invalid_ratio
* report/device/mobile 的整体分位数（P50/P90/P99）
* top operators（运营商 TopN）与集中度（CR3/CR5）

2. **行为强度**

* 上报次数分布（log 分桶）
* 设备数量分布（log 分桶）
* mobile_ratio（移动设备占比）分布（0-1 分桶）

3. **结构性单位**

* H：block_final 粒度（16K block）
* E：atom27 / e_run 粒度（2.8M atoms, 130K runs）
* F：atom27 粒度（1.6M members -> 按 atom27 聚合）

4. **“边界附近”人群**

* 各库都要给出“最容易不稳定的边界带样本”

  * H：simple_score 接近 20/30，density 接近 30（wD 跳变点）
  * E：atom_valid_cnt 接近 7（密度阈值）、run_len 接近 3
  * F：atom_valid_cnt=6、run_len=2 等“差一点进 E”的集合

---

### 7.2.2 H 库画像（重点：网络块画像 + 切分影响）

**H 的核心不是成员，而是“中型网络块”的结构画像。**

建议 H 页面结构：

#### (A) H 概览 KPI

* h_members 数、h_blocks 数（已有表 h_members / h_blocks）
* 每个 block 的成员数分布（member_cnt_total）
* valid_cnt / density / simple_score 在 H block 内分布

#### (B) H Block 画像图谱（推荐图表）

1. **散点图**：`valid_cnt` vs `density`（点大小=member_cnt_total）

   * 目的：直观看到 H 的结构带在哪条区域
2. **直方图**：`simple_score`（重点标注 20 与 30 的阈值线）
3. **箱线图**：按运营商分组的 `density` 或 `valid_cnt`
4. **TopN 条形图**：H block 的运营商占比（从 source_members_slim join h_members 聚合）

#### (C) “切分导致的 tier 漂移”模块（H 必做）

你有 `profile_pre`（自然块预画像）和 `profile_final`（最终块画像）。研究 H 时，最关键是：

* **自然块（pre）是什么 tier？最终块（final）是什么 tier？**
* **切分让哪些块从“大/超大”降到“中型”进 H？**

推荐一个**迁移矩阵**（heatmap）：

* X 轴：network_tier_pre
* Y 轴：network_tier_final
* 值：最终进入 H 的 block_final 数 / 成员数

> 这能把“Phase 04 切分”的影响从黑盒变成可视证据。

#### (D) H 的边界带（稳定性风险）

* density 在 `[29.5, 30.5]` 的 H blocks
* simple_score 在 `[19,21]` 或 `[29,31]` 的 H blocks
* 列表展示：block_id_final、density、wD、wA、simple_score、成员数、Top operator
* 一键跳 Explorer 看块详情 + 一键创建 Issue（见 7.4）

---

### 7.2.3 E 库画像（重点：/27 原子密度 + run 连续性）

E 的画像应该围绕 Phase 07 的两个门槛：

* `atom_density >= 0.2`（等价 `valid_cnt >= 7`）
* 连续 run 长度 `>= 3`

建议 E 页面结构：

#### (A) E 概览 KPI

* e_members 数、e_atoms 数、e_runs 数
* 平均 run 长度、run 长度分布（P50/P90/P99）

#### (B) 原子画像

1. **直方图**：atom_valid_cnt（0~32），重点标注 6/7/8
2. **直方图**：atom_density（0~1），重点标注 0.2
3. **条形图**：run_len 分布（1~N），重点标注 2/3/4
4. **堆叠条形图**：按运营商分组的 E 占比 vs R1（如果你愿意在流转里对比）

#### (C) E 的“连续性结构”

* 展示 **Top suspicious runs**：

  * run_len=3 但内部 atom_valid_cnt 很低（“刚过线”）
  * run_len 很长但 operator 变化异常（如果能算）
* run 详情 Drawer：列出该 run 内的 atom27_id 列表摘要（不需要全列，给头尾 + 统计即可）

---

### 7.2.4 F 库画像（重点：“剩余”到底是什么、为何没进 E）

F 的研究价值在于：它是“规则的候选增量”。

建议把 F 拆成两类做结构画像：

* **F-A：密度不够型**：atom_valid_cnt <= 6（atom_density<0.2）
* **F-B：连续性不够型**：atom_valid_cnt>=7 但 run_len<3（通常 run_len=1/2）

F 页面结构：

#### (A) F 概览 KPI

* f_members 数
* F-A / F-B 成员占比（必做，这是 F 的“解释力”核心）

#### (B) “差一点进 E”漏斗

* 从 R1 → 过密度门槛 atoms → 过 run 门槛 → E
* F-B 就是“过了密度但没过 run”的那部分，应该被直观看到（见 7.3 流转）

#### (C) F 的机会带

* atom_valid_cnt=6（最接近 7）
* run_len=2（最接近 3）
* 这些集合往往是**规则修改的最大收益区**（比如将 run>=2 纳入、或加入额外特征门槛）

---

### 7.2.5 三库对比视图（必须有）

建议在“概览”子页放一个**H/E/F 三栏对比**，每栏 3~4 个关键指标 + 同一套图的三色叠加（或 small multiples）：

* operator 集中度（Top10 share）
* report/device/mobile 的 P50/P90/P99
* valid_ratio、abnormal_ratio
* “边界带占比”（各库自己的边界定义）

> 研究平台最怕“只看单库，不知差异”。对比视图能强制让研究结论更可靠。

---

## 7.3 Pipeline 流程追溯可视化（以库为单位还原全流程）

### 7.3.1 总体建议：**“流程图 + 漏斗 + Sankey”三件套**

* **流程图（Timeline/Phase Map）**：解释每个 Phase 做什么（定性）
* **漏斗（Funnel）**：展示数量如何变化（定量）
* **Sankey（路径贡献）**：展示“哪些原因/分支导致进入该库”（归因）

ECharts 三者都支持，且 CDN 友好。

---

### 7.3.2 全局总漏斗（所有库共享）

在“流转”页顶部放全局漏斗（用于建立共同语境）：

```
source_members (59.5M)
  ├─ drop_members (12K)
  └─ keep_members (57.3M)
        ├─ H (13.0M)
        ├─ E (42.1M)
        └─ F (1.6M)
```

* 这里的数字你已有表行数能直接算
* 对于选中库（比如 H），用高亮展示该路径

---

### 7.3.3 库专属 Sankey（关键：把“为什么”可视化）

#### H 的 Sankey：**切分 + tier 漂移归因**

推荐路径定义（按 block_final 计数与按成员数两套指标都给）：

* 来源：`network_tier_pre`（自然块预画像）
* 经过：是否发生 split（有 split_events_64 命中）
* 去向：`network_tier_final`（最终画像）再到 H（只取中型网络）

Sankey 示例（逻辑）：

```
network_tier_pre
  ├─(split=0)→ network_tier_final → H
  └─(split>0)→ network_tier_final → H
```

> 这能回答：“H 到底是天然中型网络多，还是切分把更大网络切成中型后大量进入？”

#### E 的 Sankey：**密度门槛 vs 连续门槛**

对 R1 的 atoms 分层：

* atom_valid_cnt >=7 ?
* run_len >=3 ?
  最终去向：E 或 F（run 不够）

Sankey：

```
R1 atoms
  ├─ density<0.2 → F
  └─ density>=0.2
        ├─ run_len>=3 → E
        └─ run_len<3  → F
```

#### F 的 Sankey：**“剩余”拆解解释**

F 其实就是上面 E 的反向，直接复用 E 的分支归因即可。

---

### 7.3.4 Phase 节点级 I/O 展示（“每一步做了什么”）

在 Flow 页面中间放一个**Phase Timeline**，每个节点点开弹出 Drawer：

每个 Phase Drawer 统一结构：

* **Input 表**、**Output 表**
* **关键逻辑/阈值**（可写死在前端，也可后端返回）
* **对当前库的影响指标**（核心差异！）

示例：Phase 04（切分）

* Input: preh_blocks + window_headtail_64
* Output: split_events_64 + block_final
* 对 H：

  * H 覆盖的 final blocks 中，有多少来自 split=1 的自然块？
  * split 触发器占比（report/mobile/op）
  * 触发 margin（离阈值有多近，作为可靠性指标）

示例：Phase 07（E）

* Input: r1_members
* Output: e_atoms / e_runs / e_members
* 对 E：

  * atom_valid_cnt 分布（6/7/8）
  * run_len 分布（2/3/4）
  * 过密度但没过 run 的规模（直接指向 F-B）

---

## 7.4 问题发现机制（自动检测 + 严重度 + 下钻）

### 7.4.1 自动检测类型（建议至少 6 大类）

#### A. 阈值边界不稳定（你已知红色问题 wD 跳变）

**检测点：density 分段阈值 + tier 分段阈值**

* density 阶梯边界：3.5 / 6.5 / 30 / 200
* simple_score tier 边界：10 / 20 / 30 / 40
* E 原子阈值：atom_valid_cnt=7、run_len=3

**输出**：边界带的 block/atom 数 + 成员数影响 + Top operators

严重度建议：

* 🔴：边界带成员数占库成员 > X% 或集中于少数 operator（易引发系统性偏差）
* 🟡：边界带规模小，但存在明显聚集或与 split 强相关

#### B. 切分可靠性（HeadTail k=5 的噪声风险）

从 window_headtail_64 / split_events_64 提取：

* 实际左右 valid 样本数是否 < 5（如果表里有样本计数）
* ratio_report、cvL/cvR、mobile_diff、mobile_cnt_ratio 与阈值的 margin
* operator 切换但两侧样本量很小的 split

输出：可疑 split 列表（含触发器、margin、影响成员数）

#### C. 自然块碎片化（gap 不容差）

不用做 shard/block 粒度分析，但可以做**“碎片化指数”**（库级/全局都可）：

* natural block size 分布（size=1/2/3 的占比）
* 小块是否显著集中在某些 operator / report/device 特征
* 相邻自然块 gap=1/2 的数量（提示“单点缺失断块”可能性）

> 这类检测为“研究线索”，不需要立即给出修复实现。

#### D. 异常/无效成员渗透（质量风险）

* 各库 abnormal_ratio、invalid_ratio 极端高的 operator 或结构单元
* 例如：H blocks 中 abnormal_ratio>某阈值的 block 列表

#### E. 守恒/互斥/幽灵（直接对 Phase99 结果做告警）

* 读取 qa_assert 表：失败=🔴，通过=✅
* 将失败断言链接到“问题详情”页（给出相关计数、可能原因）

#### F. “解释缺失”（黑盒风险）

不是数据错误，而是研究可解释性问题：

* 某类成员（比如 F-B）数量很大，但平台没有形成明确解释/规则草案
* 这类可以作为“研究待办”提示

---

### 7.4.2 问题展示方式：Issue Board + Drilldown

在“问题”子页使用三段式：

1. **问题雷达/计数墙**：按类型统计（红/黄/灰）
2. **Issue 列表（表格）**：每行一个 issue candidate

   * type、severity、affected_members、top_operator、evidence_link、status
3. **Issue Detail Drawer**（点开）：

   * 自动生成的证据图（边界带直方图、触发 margin 分布）
   * “样本列表”（block_id_final / atom27_id / e_run_id / ip_long）
   * 按钮：`创建研究问题` / `加入样本篮` / `让 Agent 解释` / `发起模拟实验`

---

## 7.5 Agent 协作交互设计（人 + Agent 的迭代研究）

> 目标不是让 Agent“查数据库”，而是让 Agent 帮你**组织假设、设计验证、结构化产出规则**；执行与取数由平台后端做。

### 7.5.1 交互方式：对话式为主 + 指令式“动作卡片”

**推荐混合模式：**

* 对话式：解释图表、提出假设、撰写结论、生成规则草案
* 指令式：把 Agent 输出落地成可执行操作（实验 / 创建 issue / 生成 rule pack）

**Right Panel 结构建议：**

* 上半：Chat（支持引用当前图表/issue）
* 下半：Action Cards（Agent 输出结构化 JSON → 渲染为卡片）
* 底部：Session Timeline（日志）

### 7.5.2 Agent 输入上下文（自动拼装）

每次与 Agent 交互，平台应自动附带：

* run_id、library、当前筛选条件（operator、阈值范围等）
* 当前选中的 issue / 样本实体（block_id_final / atom27_id 等）
* 当前页面关键图的摘要数据（例如直方图 bins 的计数，而不是原始 60M 明细）

> 这样 Agent 不需要 SQL，也能给出严谨建议。

### 7.5.3 Agent 输出要求：结构化协议（便于 UI 融合）

建议约定 Agent 输出 JSON（示例）：

```json
{
  "summary": "H库在density≈30处出现明显边界堆积，且大量来自split触发的块。",
  "hypotheses": [
    {"id":"H1", "text":"wD在30处跳变导致simple_score跨tier边界，造成H准入不稳定。"}
  ],
  "experiments": [
    {
      "id":"EXP1",
      "type":"what_if_score_mapping",
      "params":{"wd_boundary_shift": -2.0},
      "metrics":["delta_h_members","delta_h_blocks","delta_e_members","qa_checks"]
    }
  ],
  "rule_drafts": [
    {
      "title":"平滑wD阶梯",
      "change_type":"score_mapping_update",
      "scope":{"library":"H","stage":"phase03/04p"}
    }
  ]
}
```

UI 将 `experiments` 渲染为一键执行按钮；`rule_drafts` 渲染为“生成规则草案”入口。

---

### 7.5.4 操作日志留痕（实时记录 + 可复盘）

每个 Session 必须记录：

* 筛选变化（filter diff）
* 点击下钻（从哪个图到哪个列表/实体）
* 创建 issue / note
* 发起实验、实验结果摘要
* Agent 对话摘要（不必存全量 prompt，可存摘要+结构化输出）

日志在右侧以 timeline 展示，在“研究档案”中可导出。

---

## 7.6 标准化输出机制（研究结论 → 可规模化规则）

### 7.6.1 RulePack：结构化规则包（YAML/JSON）

建议定义 RulePack 的 4 层结构：

1. **Metadata**：id、标题、作者、日期、适用 run 范围、适用库
2. **Findings 引用**：关联 issue_id、样本证据
3. **Changes**：规则变更（可执行/可评估）
4. **Validation**：验证方法与预期指标阈值

示例（YAML）：

```yaml
rule_pack:
  id: RP-2026-02-26-001
  title: "H库稳定性：平滑wD在density=30处的跳变"
  scope:
    libraries: ["H"]
    stages: ["phase03", "phase04p"]
  evidence:
    issues: ["ISS-102", "ISS-117"]
    samples:
      blocks_final: ["Fxx_...", "Fyy_..."]
  changes:
    - type: score_mapping_update
      target: wD
      based_on: density
      new_bins:
        - {lte: 3.5, wD: 1}
        - {gt: 3.5, lte: 6.5, wD: 2}
        - {gt: 6.5, lte: 30, wD: 4}
        - {gt: 30, lte: 60, wD: 8}
        - {gt: 60, lte: 200, wD: 16}
        - {gt: 200, wD: 32}
  validation:
    method: what_if_from_profile_final
    metrics:
      - name: delta_h_members_pct
        max_abs: 0.5
      - name: delta_overlap_any
        must_equal: 0
      - name: qa_assert_all_pass
        must_equal: true
```

> 重点：不要只写“建议调整”，要能被后端“验证器”读取并计算影响。

---

### 7.6.2 规则验证与影响评估（最少要做到的 3 类）

你明确目前无增量机制、全量重跑昂贵，所以验证必须分层：

#### Level 1：**可直接从现有表推导的 what-if（优先实现）**

* 调整 wD/wA 分段、tier 边界
  → 基于 profile_final / profile_pre 重算 simple_score 与 tier（不需要重跑 Phase 04）
* 调整 E 的 atom_density / run_len 门槛
  → 基于 e_atoms / e_runs 统计重算覆盖（再与 r1_members/keep_members 做差）

#### Level 2：**需要重算部分阶段但可在样本集上做**

* HeadTail k 从 5 调到 10 的影响
  → 先对抽样 preh_blocks 重算窗口统计并估计 split 稳定性变化

#### Level 3：**必须全链路重跑的（只输出执行计划）**

* 自然块 gap 容差的算法改变
  → 输出影响范围、预估收益、回滚策略，但不在研究工具内强求实时验证

---

### 7.6.3 “规则 → 代码”落地接口（对编码 Agent 友好）

RulePack 的每个 `change.type` 应映射到一个后端执行器模块，例如：

* `score_mapping_update` → Python 配置文件 / SQL CASE 表达式生成
* `threshold_update` → config_kv 写入或 pipeline 参数化
* `qa_assert_add` → 新增断言 SQL 模板

研究平台应支持：

* 导出 RulePack（YAML/JSON）
* 导出研究报告（Markdown/PDF 可后期做）

---

## 7.7 技术建议（前端库、后端 API、性能优化、路线图）

### 7.7.1 前端可视化库选择：**ECharts（强烈推荐）**

理由（落地角度）：

* CDN 直接可用，契合你“无构建工具”的约束
* 原生支持：Sankey、Funnel、Heatmap、Boxplot、Scatter、DataZoom
* 大量交互（click/brush）易做，下钻联动方便

补充：

* 列表大数据：实现简单的“虚拟滚动”（或只分页）
* 代码编辑器（规则 YAML）：可用 CodeMirror CDN（后期再加）

---

### 7.7.2 需要新增的后端 API 设计（建议一组“研究专用”路由）

> 目标：前端不写复杂 SQL；后端提供“稳定可复用的聚合接口”。

#### 1) 库概览与对比

* `GET /api/research/runs`（复用现有 runs 也行）
* `GET /api/research/runs/{run_id}/libraries/overview`

  * 返回 H/E/F：members_total、valid_ratio、abnormal_ratio、top_operator、report/device/mobile 分位数

#### 2) 画像分布（统一接口，按 field + bins）

* `GET /api/research/runs/{run_id}/library/{lib}/distribution?field=report_cnt&bins=log10`
* `GET /api/research/runs/{run_id}/library/{lib}/top?field=operator&limit=20`

> 实现：基于 `{lib}_members` join `source_members_slim`，再做 `width_bucket` 或 log 分桶聚合。

#### 3) 流转与 Sankey

* `GET /api/research/runs/{run_id}/flow/global-funnel`
* `GET /api/research/runs/{run_id}/library/{lib}/flow/sankey`

  * H：pre_tier → split? → final_tier → H
  * E/F：density_pass? → run_pass? → E/F

#### 4) 问题检测

* `GET /api/research/runs/{run_id}/library/{lib}/issues?severity=...&type=...`
* `GET /api/research/runs/{run_id}/library/{lib}/issues/{issue_id}`

  * 返回证据聚合 + 样本实体列表（block/atom/run/ip_long）

#### 5) 研究会话与留痕

* `POST /api/research/sessions`（创建 session：run_id、library、title）
* `GET /api/research/sessions/{session_id}`
* `POST /api/research/sessions/{session_id}/events`（日志写入：filter_change / drilldown / note / experiment）
* `POST /api/research/sessions/{session_id}/notes`

#### 6) 实验与 what-if 评估

* `POST /api/research/experiments`

  * body：{session_id, type, params}
  * return：{result_summary, charts_data, delta_metrics}

#### 7) 规则包

* `POST /api/research/rule-packs`（保存草案）
* `GET /api/research/rule-packs/{id}`
* `POST /api/research/rule-packs/{id}/validate`（调用 what-if 验证器）

---

### 7.7.3 数据查询优化建议（60M+ / 95GB 的现实打法）

#### A. 优先使用 slim 表

画像类查询尽量用 `source_members_slim`（14GB）而不是 `source_members`（95GB）。

#### B. 典型 join 路径（按库）

* H：`h_members` → join `source_members_slim`（ip_long, run_id, shard_id）
* E：`e_members` → join `source_members_slim`
* F：`f_members` → join `source_members_slim`
* H block 画像：`profile_final` / `h_blocks` 为主（block 粒度小）

#### C. 建议索引（不改分区也能显著提速）

结合你的查询模式（run_id + ip_long / block_id / atom27）：

* `source_members_slim (run_id, ip_long)` BTree 或 BRIN(ip_long)+BTree(run_id)
* `h_members (run_id, ip_long)` / `e_members (run_id, ip_long)` / `f_members (run_id, ip_long)`
* `map_member_block_final (run_id, ip_long)`、`map_member_block_final (run_id, block_id_final)`
* `profile_final (run_id, block_id_final)`
* `e_members (run_id, atom27_id)`、`f_members (run_id, atom27_id)`
* `e_runs (run_id, e_run_id)`（如果常查 run）

#### D. 做“研究聚合表/物化视图”（第二阶段就做）

为了让图表秒开，建议引入按 run_id 预计算的聚合结果：

* `research_lib_kpi`（每库一行）
* `research_lib_dist`（field + bin → count）
* `research_h_tier_drift`（pre_tier×final_tier）
* `research_e_density_run_funnel`（density_pass/run_pass 的计数）
* `research_issue_candidates`（问题候选清单）

这些可以：

* pipeline 跑完后一次性生成
* 或研究平台首次打开某 run 时惰性生成并缓存

---

### 7.7.4 分阶段实施路线图（务实可交付）

#### Phase 1（1~2 周量级）：先把“库研究”跑起来

* 新 Tab：🧪库画像研究
* H/E/F 基础 KPI、operator TopN、report/device/mobile 分布
* 全局漏斗 + E/F 的 density/run 漏斗
* Issue（边界带 + E 阈值边界）最小集
* Session 留痕先做轻量：后端表 + events 写入

#### Phase 2：补齐“解释力”与“切分研究”

* H 的 tier 漂移矩阵 + split 触发器占比
* split 可疑性检测（margin、样本不足）
* 样本篮 + 一键跳 Explorer

#### Phase 3：规则包 + what-if 验证器（规模化关键）

* RulePack 编辑/保存/导出
* what-if：wD/tier、E 阈值、run_len 的影响评估
* 规则验证报告：delta(H/E/F)、QA 断言结果复算（能做的先做）

#### Phase 4：Agent 深度协作（可选但强价值）

* Agent 输出结构化 JSON + Action Cards
* 一键把 Agent 的 hypothesis/experiment/rule 草案落到 Session

---

## 最后给你一个“研究流程模板”（平台里可以做成向导）

对任意库（H/E/F）默认走这条：

1. **概览**：规模/质量/对比差异（确认研究对象）
2. **画像**：核心分布 + 指纹图（知道“长什么样”）
3. **流转**：看漏斗/Sankey/关键 Phase（知道“为何如此”）
4. **问题**：看自动检测（知道“哪里不稳/不合理”）
5. **样本**：挑代表性样本，跳 Explorer 复核（拿证据）
6. **规则**：生成 RulePack 草案 → what-if 评估 → 固化

