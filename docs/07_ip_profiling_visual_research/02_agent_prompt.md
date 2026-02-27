# IP 库画像可视化研究平台 — 深度思考 Prompt

> **说明**: 本文档是给外部深度思考 Agent 的完整上下文包。它不需要执行代码或访问数据库，只需要基于以下所有信息进行综合分析，输出一份**详细的可视化研究平台方案设计**。

---

## 一、你的任务

请基于以下完整的项目背景、数据库结构、现有代码和用户需求，设计一个 **IP 库画像可视化研究平台** 的完整方案。方案需要覆盖：

1. **可视化架构设计** — 用什么界面结构来呈现研究过程
2. **以库为单位的画像研究流程** — 如何对 H/E/F 每个库进行系统性画像
3. **数据流转可视化** — 如何展示每个 Pipeline 阶段的输入输出变化
4. **问题发现与修正机制** — 如何帮助分析师发现问题并留痕
5. **Agent 协作交互设计** — 人与 Agent 如何配合进行迭代研究
6. **标准化输出** — 研究结论如何变成可大规模执行的规则

请注意以下约束：
- 方案需要**落地可执行**，我会拿着你的方案用代码实现
- 现有 WebUI 是 FastAPI + Vue3 SPA（详见下方代码），方案应在此基础上扩展或重构
- 数据库是 PostgreSQL，schema 和表结构详见下方，请充分利用已有数据
- **不需要做 IP 分块/Shard 粒度的分析**，用户明确表示那对他没意义
- **研究单位是 IP 库（H/E/F）**，而非 Shard 或 Block

---

## 二、项目背景

### 2.1 业务目标

这是一个 **IP 地址分类画像系统**。核心流程是：

```
中国 IP 地址 (~59.7M) → 清洗/聚类 Pipeline → 分为 H/E/F 三个库 → 线上自动化处理
```

- **H 库 (~13M)**: 中型网络块成员，结构化程度最高
- **E 库 (~45M)**: 密集原子成员，基于 /27 网段密度识别
- **F 库 (~1.7M)**: 剩余散点 IP

### 2.2 当前阶段

第一阶段（IP 大分类）已完成，Pipeline 已跑完全部 59.7M IP 的分类。下一步是：

> **对每个库进行深度画像研究**：理解每个库的数据特征、Pipeline 处理逻辑对它的影响、边界情况和问题数据，然后形成可自动化执行的标准化处理规则。

### 2.3 核心痛点

| 痛点 | 说明 |
|------|------|
| **过程黑盒** | 分析师用经验处理 IP，逻辑不透明，其他人无法检查 |
| **想法遗漏** | 复杂的分析思路在过程中丢失，无法追溯 |
| **无法复查** | 没有可视化手段还原"为什么这个 IP 被这样分类" |
| **无法规模化** | 手工研究无法积累为自动化流程 |
| **现有 WebUI 偏宏观** | 当前看板关注 Shard 矩阵和 run 概览，不是以库为单位的研究 |

---

## 三、Pipeline 全链路逻辑

### 3.1 架构总览

```
输入层 (public)              →  处理层 (rb20_v2_5)              →  交付层
┌──────────────────┐      ┌─────────────────────────────┐    ┌────────────────┐
│ ip源表 (59.7M)    │      │ Phase 00: 基础设施            │    │ H Members (13M)│
│ 异常ip表          │      │ Phase 01-02: 源成员+自然块     │    │ E Members (45M)│
└──────────────────┘      │ Phase 03: 预画像+网络规模       │    │ F Members (1.7M│
                          │ Phase 11: HeadTail Window      │    │ QA Assert      │
                          │ Phase 04: 切分+最终块           │    └────────────────┘
                          │ Phase 04P: 最终画像             │
                          │ Phase 05: H库 (全局)            │
                          │ Phase 06-08: R1/E/F (per-shard) │
                          │ Phase 99: QA终验收 (全局)       │
                          └─────────────────────────────────┘
```

### 3.2 各阶段详解

#### Phase 01: 源成员过滤 + 异常标记
- **输入**: public.ip源表 + public.异常ip表
- **输出**: source_members (~59.7M 行)
- **逻辑**: 过滤只保留中国 IP → LEFT JOIN 异常表标记 is_abnormal → 异常只标记不删除

#### Phase 02: 自然块识别
- **输入**: source_members
- **输出**: block_natural (~13.2M 块) + map_member_block_natural (成员→块映射)
- **算法**: 按 ip_long 升序排列，相邻差=1 归入同块，否则断开
- **块 ID 格式**: `N{shard_id}_{ip_start}_{ip_end}`

#### Phase 03: 预画像 (SIMPLE 评分)
- **输入**: block_natural + source_members
- **输出**: profile_pre (预画像) + preh_blocks (PreH 候选块)
- **评分函数**:
  ```
  valid_cnt → wA (5档: 1~16→1, 17~48→2, 49~128→4, 129~512→8, ≥513→16)
  density   → wD (5档: ≤3.5→1, 3.5~6.5→2, 6.5~30→4, 30~200→16, >200→32)
  simple_score = wA + wD → network_tier
  ```
  | network_tier | 阈值 |
  |---|---|
  | 超大网络 | ≥40 |
  | 大型网络 | 30~39 |
  | 中型网络 | 20~29 |
  | 小型网络 | 10~19 |
  | 微型网络 | <10 |
- **Keep/Drop**: valid_cnt=0 → Drop (ALL_ABNORMAL_BLOCK)；其余 Keep

#### Phase 11: HeadTail 窗口摘要
- **输入**: preh_blocks + source_members
- **输出**: window_headtail_64
- **算法**: 对每个 PreH 块，按 bucket64=floor(ip_long/64) 定义切点，每个切点左右各取 k=5 个 valid IP，计算窗口统计

#### Phase 04: 切分与最终块
- **输入**: window_headtail_64 + preh_blocks
- **输出**: split_events + block_final
- **三触发器** (任一命中即切):
  1. Report 触发: ratio_report > 4 AND cvL < 1.1 AND cvR < 1.1
  2. Mobile 触发: mobile_diff > 0.5 OR mobile_cnt_ratio > 4
  3. Operator 触发: opL ≠ opR

#### Phase 04P: 最终画像
- 对最终块重新计算 network_tier_final（与 Phase 03 同口径）
- 切分后块变小，network_tier 可能降级

#### Phase 05: H 库
- **准入**: network_tier_final = '中型网络'
- **输出**: h_blocks + h_members (~13M)

#### Phase 06: R1 残余集
- R1 = Keep \ H_cov

#### Phase 07: E 库
- /27 原子: atom27_id = floor(ip_long/32)
- 密度准入: atom_density = valid_ip_cnt / 32.0 ≥ 0.2（即 valid ≥ 7）
- 连续 run: 最小 3 个原子
- **输出**: e_atoms + e_runs + e_members (~45M)

#### Phase 08: F 库
- F = R1 \ E_cov (atom27 等值 anti-join，禁止 BETWEEN)
- **输出**: f_members (~1.7M)

#### Phase 99: QA 断言 (11 条 STOP)
- no_overlap_h_e, no_overlap_h_f, no_overlap_e_f (互斥)
- conservation_keep_equals_hef (守恒)
- no_ghost_hef_outside_source, drop_members_have_natural_map 等

### 3.3 H/E/F 分类决策树

```
Source Member (ip_long)
│
├─ is_valid = false? → 保留在成员集合中（标记不删除）
│
├─ 归属自然块 valid_cnt = 0? → Drop (ALL_ABNORMAL_BLOCK)
│
├─ Keep
│  ├─ 归属最终块 network_tier_final = '中型网络'?
│  │  └─ YES → H 类
│  ├─ R1 = Keep \ H_cov
│  │  ├─ /27 原子 atom_density ≥ 0.2?
│  │  │  └─ YES → 连续 run ≥ 3?
│  │  │         ├─ YES → E 类
│  │  │         └─ NO  → F 类
│  │  └─ NO → F 类
│
└─ Drop (不参与 H/E/F 分类)
```

### 3.4 已知问题 (合理性审计发现)

| 级别 | 问题 | 详情 |
|------|------|------|
| 🔴 | wD 阶梯跳跃过大 | density 30→30.1 导致 wD 从 4 跳到 16，score 增加 12，可能导致边界 IP 块分类不稳定 |
| 🟡 | 无增量更新机制 | 全量重跑 Pipeline 是唯一路径 |
| 🟡 | 自然块 gap 不容差 | 单个 IP 缺失即断块，可能产生过多碎片块 |
| 🟡 | HeadTail k=5 样本量 | 切分窗口仅 5 个 valid IP，边界判定可能受噪声干扰 |

---

## 四、数据库结构

### 4.1 环境
- PostgreSQL, Host: 192.168.200.217:5432, DB: ip_loc2, Schema: rb20_v2_5

### 4.2 表清单与规模

| 表名 | 行数 | 大小 | 说明 |
|------|------|------|------|
| source_members | 59.5M | 95 GB | 源成员表 (含全部中国IP字段) |
| source_members_slim | 59.2M | 14 GB | 源成员精简版 |
| map_member_block_natural | 59.2M | 30 GB | 成员→自然块映射 |
| keep_members | 57.3M | 10 GB | Keep 集合 |
| map_member_block_final | 55.5M | 19 GB | 成员→最终块映射 |
| r1_members | 43.7M | 11 GB | R1 残余集 |
| e_members | 42.1M | 10 GB | E 库成员 |
| block_natural | 13.2M | 3.1 GB | 自然块 |
| profile_pre | 13.2M | 7.5 GB | 预画像 |
| profile_pre_stage | 13.2M | 4.9 GB | 预画像中间态 |
| h_members | 13.0M | 2.9 GB | H 库成员 |
| block_final | 12.8M | 3.9 GB | 最终块 |
| step03_block_bucket | 12.8M | 4.5 GB | Phase 03 bucket |
| profile_final | 12.3M | 7.1 GB | 最终画像 |
| e_atoms | 2.8M | 565 MB | /27 原子 |
| f_members | 1.6M | 413 MB | F 库成员 |
| split_events_64 | 635K | 274 MB | 切分事件 |
| window_headtail_64 | 605K | 335 MB | HeadTail 窗口 |
| preh_blocks | 312K | 73 MB | PreH 候选块 |
| e_runs | 130K | 32 MB | E 连续 run |
| abnormal_dedup | 79K | 24 MB | 去重异常表 |
| h_blocks | 16K | 8.5 MB | H 库块 |
| drop_members | 12K | 2.6 MB | Drop 集合 |
| step_stats | 3K | 1 MB | 步骤统计指标 |
| step03_task_plan | 318 | 160 KB | Phase 03 任务计划 |
| shard_plan | 65 | 64 KB | 分片计划 |
| config_kv | 13 | 32 KB | 配置键值 |
| qa_assert | 10 | 32 KB | QA 断言结果 |

### 4.3 核心表字段

**source_members** (源成员，最宽的表):
```
ip_long, ip_address, shard_id, run_id,
"IP归属国家", "IP归属运营商", "上报次数", "设备数量", "移动网络设备数量",
is_abnormal, is_valid, created_at
```

**profile_pre / profile_final** (画像表):
```
run_id, shard_id, block_id_natural/block_id_final,
valid_cnt, density, "wA", "wD", simple_score,
network_tier_pre/network_tier_final,
keep_flag, drop_reason (仅 profile_pre),
member_cnt_total, created_at
```

**h_members / e_members / f_members**:
```
run_id, shard_id, ip_long, ip_address,
block_id_final (h) / atom27_id + e_run_id (e) / atom27_id (f),
created_at
```

---

## 五、现有 WebUI 代码

### 5.1 技术栈
- 后端: FastAPI (Python), SQLAlchemy async, PostgreSQL
- 前端: Vue 3 (CDN), 单文件 SPA (index.html 612行 + style.css)
- 无构建工具，无 Node.js

### 5.2 现有 Tab 结构

| Tab | 功能 | 对应后端 |
|-----|------|---------|
| 📊 协同看板 | KPI 卡片(Source/H/E/F/QA)、Shard 矩阵热力图、网络规模分布、守恒公式 | /api/dashboard/* |
| 🔍 数据探查 | 输入 IP/ip_long/block_id → 全链路溯源 | /api/explore/* |
| ✅ QA 断言 | 11 条断言结果展示、守恒验证 | /api/dashboard/qa |

### 5.3 后端 API 清单

```
GET /api/dashboard/runs                           → run_id 列表
GET /api/dashboard/runs/{run_id}/overview          → 单次运行总览统计
GET /api/dashboard/runs/{run_id}/shards            → 65 Shard 状态矩阵
GET /api/dashboard/runs/{run_id}/qa                → QA 断言结果
GET /api/dashboard/runs/{run_id}/step-stats        → 步骤统计指标
GET /api/dashboard/runs/{run_id}/network-tier-distribution → 网络规模分布
GET /api/dashboard/runs/{run_id}/classification-summary    → H/E/F 分类汇总

GET /api/explore/ip/{ip_address}                   → IP 全链路溯源
GET /api/explore/ip-long/{ip_long}                 → ip_long 溯源
GET /api/explore/block/{block_id}                  → 块详情 (natural/final)
GET /api/explore/shard/{shard_id}/blocks           → Shard 下最终块列表 (分页)
```

### 5.4 前端代码概要

Vue 3 SPA，使用 Composition API (setup)。状态管理：

```javascript
// 核心 state
currentTab: 'dashboard' | 'explorer' | 'qa'
selectedRunId, overview, shards, qaResults, tierDist
searchQuery, traceResult  // Explorer tab

// 核心方法
loadRuns() → loadRunData() → 加载 overview/shards/qa/tierDist
doSearch() → 根据输入类型自动选 /ip/ 或 /ip-long/ 或 /block/ 接口
```

前端 UI 设计：暗色主题，毛玻璃面板，动画卡片。

---

## 六、用户核心需求 (请重点理解)

### 6.1 总目标

> 以 **单个 IP 库 (H/E/F)** 为研究单位，通过可视化方式**还原并研究** Pipeline 的处理过程，**发现问题后与 Agent 协作修正**，最终形成**可规模化执行的标准化处理规则**。

### 6.2 关键需求拆解

1. **对每个库进行画像** (不是看 Shard 矩阵或块统计)
   - 这个库里有什么样的 IP？特征分布如何？
   - 和其他库有什么差异？为什么会被分到这个库？

2. **还原处理过程** (每个 Phase 的数据变化)
   - 从 source_members 到最终分类，每一步做了什么筛选/转换？
   - 每一步有多少成员被排除/保留？漏斗怎么变化的？

3. **发现问题数据**
   - 阈值边界的 IP（分类不稳定区域）
   - 异常聚集 / 数据质量问题
   - 切分是否合理

4. **修正与留痕**
   - 分析师发现问题 → 提出假设 → Agent 帮忙验证/模拟 → 确认修正方案
   - 全过程操作日志留痕

5. **标准化输出**
   - 研究完成后的规则可以变成代码自动执行
   - 为下一阶段大规模处理提供基础

### 6.3 反面需求 (明确不要的)

- ❌ 不需要 Shard 矩阵 / IP 分块分析 — 那是基础设施层面的，对分析师没有直接意义
- ❌ 不需要 Agent 自己去执行 SQL — Agent 只负责出方案，代码层由另一个编码 Agent 实现
- ❌ 不需要实时在线系统 — 这是研究工具，不是线上查询服务

---

## 七、请输出的方案内容

请你综合以上所有信息，输出一份**详细的、可落地执行的方案设计**，至少覆盖以下内容：

### 7.1 可视化界面架构
- 整体页面结构、导航逻辑
- 各层级的信息密度和交互方式
- 与现有 WebUI 的关系（扩展 / 重构 / 独立新页面？）

### 7.2 库级画像设计
- 对 H/E/F 每个库，应该展示哪些维度的信息？
- 如何组织图表和统计面板？
- 三个库之间的对比视图应该怎么设计？

### 7.3 Pipeline 流程追溯可视化
- 如何展示一个库从 Source 到最终分类的全流程？
- 每个 Phase 的输入/输出/关键决策点如何呈现？
- 漏斗图 / Sankey 图 / Timeline 等可视化形式的选择建议

### 7.4 问题发现机制
- 自动检测哪些类型的问题？
- 问题的展示方式和严重程度标记
- 如何引导分析师从宏观到微观下钻？

### 7.5 Agent 协作交互设计
- 人与 Agent 的交互界面布局
- 对话式 vs 指令式 的交互方式选择
- 操作日志如何实时记录和展示？
- 如何将 Agent 的分析结果融入可视化界面？

### 7.6 标准化输出机制
- 研究结论 → 规则文档的转化流程
- 规则文档的格式设计
- 如何验证规则的正确性和影响范围？

### 7.7 技术建议
- 前端可视化库的选择建议（ECharts / D3 / Chart.js 等）
- 需要新增的后端 API 设计
- 数据查询优化建议（考虑到最大表有 60M+ 行 / 95GB）
- 分阶段实施路线图

请给出**具体的、详细的方案**，包括界面布局草图（用 ASCII/文字描述即可）、API 设计、图表类型选择理由等。方案将由编码 Agent 直接实施。
