# IP 库画像研究平台 — 后续开发计划

> 本文档是交接文档，供后续开发者（或切换模型后）直接阅读并接续开发。
> 最后更新：2026-02-26

---

## 一、当前项目状态总结

### 1.1 技术栈
- **后端**: FastAPI + SQLAlchemy (async) + PostgreSQL
- **前端**: Vue 3 (CDN, 无构建) + ECharts (CDN) + 原生 CSS
- **数据库**: PostgreSQL, schema = `rb20_v2_5`, 60M+ 成员, 95GB
- **启动**: `cd /Users/yangcongan/cursor/IP && python -m uvicorn webui.main:app --reload --host 0.0.0.0 --port 8000`

### 1.2 关键文件位置

```
webui/
├── main.py                          # FastAPI 入口, 已注册 3 个 router
├── api/
│   ├── dashboard.py                 # 协同看板 API (原有)
│   ├── explorer.py                  # 数据探查 API (原有)
│   └── research.py                  # ★ 库画像研究 API (新增, 10 个端点)
├── models/
│   └── database.py                  # fetch_all / fetch_one 工具函数
├── static/
│   ├── index.html                   # ★ 单页应用 (Vue 3 SPA, ~1340 行)
│   └── assets/style.css             # 全局 CSS (~750 行)
└── config.py                        # DB 连接配置

docs/07_ip_profiling_visual_research/
├── codex.md                         # ★ 总设计蓝图 (7.1~7.7, 736 行)
├── 00_requirements_understanding.md # 需求理解
├── 01_visualization_scheme.md       # 可视化方案
├── 02_agent_prompt.md               # Agent 协作提示词
└── 03_continuation_plan.md          # ← 本文档
```

### 1.3 数据库核心表 (schema: rb20_v2_5)

| 表 | 用途 | 行数级别 |
|----|------|----------|
| `source_members_slim` | 全量成员瘦表 | 60M |
| `h_members` / `e_members` / `f_members` | H/E/F 库成员 | 13M / 45M / 1.6M |
| `keep_members` / `drop_members` | Keep/Drop 分组 | 59M / 12K |
| `r1_members` | R1 轮次成员 | ~45M |
| `profile_pre` | 自然块预画像 (network_tier_pre, density, simple_score) | ~250K 块 |
| `profile_final` | 最终块画像 (network_tier_final, block_id_parent) | ~13M 块 |
| `block_final` | 最终块定义 (ip_start/end, parent) | ~13M |
| `split_events_64` | 切分事件 (trigger_*, cvl/cvr, is_cut) | ~18K |
| `e_atoms` | E 库 /27 原子 (atom_density, valid_ip_cnt) | ~2.8M |
| `e_runs` | E 库连续 run (run_len, short_run) | ~130K |
| `qa_assert` | QA 断言结果 | 11 行 |
| `shard_plan` | 分片计划 | 65 行 |

### 1.4 已完成的 10 个 API 端点

| # | 端点 | 功能 |
|---|------|------|
| 1 | `GET /api/research/runs/{run_id}/libraries/overview` | 三库 KPI + CR3 + P50/P90 + 漏斗 |
| 2 | `GET /api/research/runs/{run_id}/library/{lib}/distribution` | 字段分布直方图 |
| 3 | `GET /api/research/runs/{run_id}/library/{lib}/top` | 运营商 TopN |
| 4 | `GET /api/research/runs/{run_id}/flow/global-funnel` | 全局漏斗 |
| 5 | `GET /api/research/runs/{run_id}/library/{lib}/flow/sankey` | 库专属流转 |
| 6 | `GET /api/research/runs/{run_id}/library/{lib}/issues` | 边界带检测 |
| 7 | `GET /api/research/runs/{run_id}/h/tier-drift` | Tier 漂移矩阵 |
| 8 | `GET /api/research/runs/{run_id}/h/split-analysis` | Split 触发器分析 |
| 9 | `GET /api/research/runs/{run_id}/h/suspicious-splits` | 可疑 Split 列表 |
| 10 | `GET /api/research/runs/{run_id}/library/{lib}/samples` | IP 样本篮 |

### 1.5 前端已有的 Tab 和 Sub-Tab

**主 Tab**: 📊协同看板 | 🔍数据探查 | ✅QA断言 | 🧪库画像研究

**🧪库画像研究 子页**:
- 📋 概览 — KPI卡片、三库对比表、全局漏斗(ECharts)、运营商饼图
- 📊 画像 — reports/devices/operator/score 四张分布图
- 🔀 流转 — 全局漏斗 + 库专属分析(H:tier漂移条形图, E/F:密度/run饼图)
- ⚠️ 问题 — 边界带检测结果(issue cards)
- 🔬 H深度 — Tier漂移热力图 + 触发器饼图 + 可疑切分Top30表
- 🧬 样本 — 边界带/切分块/随机 IP样本 + 一键跳Explorer

---

## 二、后续开发任务清单 (按优先级排序)

### Phase 3A: what-if 评估器 (最高优先级, 价值最大)

> codex.md 7.6.2 Level 1: "可直接从现有表推导的 what-if"

#### 任务 3A-1: wD 分段 what-if (H 库)

**目标**: 调整 wD 权重映射 → 基于 `profile_final` 重算 `simple_score` → 预测 H 成员数变化

**后端实现 (research.py 新增端点)**:
```
POST /api/research/runs/{run_id}/experiments/whatif-wd
Body: {
  "wd_bins": [
    {"lte": 3.5, "wd": 1},
    {"gt": 3.5, "lte": 6.5, "wd": 2},
    {"gt": 6.5, "lte": 30, "wd": 4},
    {"gt": 30, "lte": 60, "wd": 8},   // 原来 >30 直接 wD=16
    {"gt": 60, "lte": 200, "wd": 16},
    {"gt": 200, "wd": 32}
  ]
}
```

**计算逻辑**:
1. 从 `profile_final` 读取所有 block 的 `density`, `wa`, `wd`, `simple_score`, `network_tier_final`, `member_cnt_total`
2. 根据新 wd_bins 重算每个 block 的 `new_wd`
3. `new_simple_score = wa + new_wd` (simple_score = wa + wd)
4. 根据 new_simple_score 重新判定 network_tier:
   - < 10: 微型网络
   - 10~19: 小型网络
   - 20~29: 中型网络 (H 准入)
   - 30~39: 大型网络
   - >= 40: 超大网络
5. 对比原 tier vs 新 tier, 统计:
   - `delta_h_blocks`: 净增/减 H 块数
   - `delta_h_members`: 净增/减 H 成员数
   - `gained_blocks`: 新进入 H 的块列表(Top20)
   - `lost_blocks`: 离开 H 的块列表(Top20)
   - `tier_migration_matrix`: 原tier → 新tier 迁移矩阵

**前端实现**: 在 H深度 子页下方新增 "🧪 what-if 实验" 面板:
- 可编辑的 wD 分段表格 (每行: density 范围 + wD 值)
- "运行评估" 按钮 → 调用 API
- 结果展示: delta KPI 卡片 + 迁移矩阵热力图 + 影响块列表

**关键 SQL 参考** (profile_final 字段):
```sql
SELECT density, wa, wd, simple_score, network_tier_final, member_cnt_total
FROM rb20_v2_5.profile_final
WHERE run_id = :run_id
```

#### 任务 3A-2: E 阈值 what-if (E/F 库)

**目标**: 调整 atom_density 阈值 / run_len 阈值 → 预测 E/F 成员交换

**后端实现**:
```
POST /api/research/runs/{run_id}/experiments/whatif-e-threshold
Body: {
  "min_valid_cnt": 6,     // 原值 7 (atom_density >= 0.2 等价 valid >= 7)
  "min_run_len": 2        // 原值 3
}
```

**计算逻辑**:
1. 从 `e_atoms` 读取 `atom27_id`, `valid_ip_cnt`, `is_e_atom`
2. 从 `e_runs` 读取 `e_run_id`, `run_len`, `short_run`
3. 按新阈值判定: 新 is_e_atom = (valid_ip_cnt >= new_min_valid_cnt)
4. 重算 run: 连续原子中 new_is_e_atom=true 的 run 长度 (这一步较复杂, 可简化为只计算现有 run 中包含几个 pass/fail 原子)
5. 简化版: 直接基于现有 `e_atoms` 和 `e_runs` 统计:
   - 原 E atoms 数 vs 新 E atoms 数
   - 估算 delta_e_members 和 delta_f_members

**前端实现**: 在 概览/流转 子页添加 "🧪 E阈值实验" 面板

---

### Phase 3B: 规则包 RulePack (次高优先级)

> codex.md 7.6.1

#### 任务 3B-1: RulePack 数据模型

**新建数据库表** (在 rb20_v2_5 schema 下):
```sql
CREATE TABLE rb20_v2_5.research_rule_packs (
    id TEXT PRIMARY KEY,            -- 'RP-2026-02-26-001'
    run_id TEXT NOT NULL,
    title TEXT NOT NULL,
    scope_libraries TEXT[],          -- ['H'] or ['E','F']
    scope_stages TEXT[],             -- ['phase03','phase04p']
    status TEXT DEFAULT 'draft',     -- draft/validated/applied
    changes JSONB NOT NULL,          -- 规则变更内容
    evidence JSONB,                  -- 关联 issue/样本
    validation_result JSONB,         -- what-if 结果
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### 任务 3B-2: RulePack API

```
POST   /api/research/rule-packs                   # 创建草案
GET    /api/research/rule-packs                    # 列表
GET    /api/research/rule-packs/{id}               # 详情
PUT    /api/research/rule-packs/{id}               # 更新
POST   /api/research/rule-packs/{id}/validate      # 执行 what-if 验证
DELETE /api/research/rule-packs/{id}               # 删除
```

#### 任务 3B-3: RulePack 前端 (新 sub-tab: 📐 规则)

- 规则列表 (表格: id, title, scope, status, delta 摘要)
- 新建规则 Drawer:
  - 选择类型: score_mapping_update / threshold_update
  - 编辑参数 (JSON 编辑器或表格)
  - 关联 issue/样本
- 一键验证按钮 → 调用 what-if → 显示影响报告

---

### Phase 3C: 画像增强 (中优先级)

#### 任务 3C-1: H Block 散点图 (codex 7.2.2-B)

在 画像 子页添加: `valid_cnt` vs `density` 散点图, 点大小=member_cnt_total

**后端**:
```
GET /api/research/runs/{run_id}/h/block-scatter?sample=500
```
返回 500 个采样块的 `{valid_cnt, density, member_cnt_total, simple_score, network_tier_final}`

**前端**: ECharts scatter, 颜色=network_tier_final, 大小=member_cnt

#### 任务 3C-2: 运营商箱线图 (codex 7.2.2-B)

按运营商 Top5 分组的 density 箱线图

**后端**:
```
GET /api/research/runs/{run_id}/h/operator-boxplot?field=density&top=5
```
返回每个运营商的 5 分位数 `{min, Q1, median, Q3, max}`

**前端**: ECharts boxplot

#### 任务 3C-3: E 连续性结构 (codex 7.2.3-C)

Top suspicious E runs: run_len=3 但内部原子质量低

**后端**:
```
GET /api/research/runs/{run_id}/e/suspicious-runs?limit=20
```

#### 任务 3C-4: F 机会带分析 (codex 7.2.4-C)

atom_valid_cnt=6 和 run_len=2 的 F 成员: 规则修改最大收益区

**后端**: 可复用 `/library/f/issues` 或新增:
```
GET /api/research/runs/{run_id}/f/opportunity-zone
```

---

### Phase 3D: 问题系统增强 (中低优先级)

#### 任务 3D-1: Issue Board 三段式 (codex 7.4.2)

当前问题页是简单卡片列表. 升级为:
1. **顶部雷达**: 按类型统计 🔴/🟡/💡 数量
2. **Issue 列表**: 表格, 支持排序/筛选
3. **Detail Drawer**: 点击展开证据图 + 样本列表 + 操作按钮

#### 任务 3D-2: 新增检测类型 (codex 7.4.1-C/D/F)

- C: 自然块碎片化指数 (size=1/2/3 占比)
- D: 异常/无效成员渗透 (abnormal_ratio 极端高的 operator)
- F: "解释缺失" (F-B 数量大但无规则草案)

---

### Phase 4: 研究会话与留痕 (低优先级)

> codex.md 7.7.2 API 第5组

#### 任务 4-1: Session 数据模型

```sql
CREATE TABLE rb20_v2_5.research_sessions (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    library TEXT,
    title TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE rb20_v2_5.research_session_events (
    id SERIAL PRIMARY KEY,
    session_id TEXT REFERENCES research_sessions(id),
    event_type TEXT,  -- filter_change/drilldown/note/experiment/issue
    payload JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

#### 任务 4-2: Session API
```
POST /api/research/sessions
GET  /api/research/sessions
GET  /api/research/sessions/{id}
POST /api/research/sessions/{id}/events
```

#### 任务 4-3: 前端 Session Timeline
Right Panel 或底部 Timeline, 实时显示操作日志

---

### Phase 5: Agent 协作 (最低优先级, 可选)

> codex.md 7.5

- Right Panel Chat 区域
- Agent 输出结构化 JSON + Action Cards
- 自动拼装上下文 (当前筛选 + 选中 issue/样本)

---

## 三、通用开发约定

### 3.1 后端约定

- 所有新端点加在 `webui/api/research.py` 中
- router 前缀: `/api/research`
- 使用 `fetch_all(sql, params)` 和 `fetch_one(sql, params)` 的 async 函数
- SQL 参数用 `:param_name` 格式 (SQLAlchemy text binding)
- SCHEMA 常量: `SCHEMA = "rb20_v2_5"`
- 所有 run_id 都通过 `_resolve_run(run_id)` 处理

### 3.2 前端约定

- 单文件 SPA: `webui/static/index.html`
- Vue 3 Composition API (`setup()` 函数)
- ECharts 初始化: 使用 `_ec(id)` 辅助函数
- 库颜色: `LIB_COLORS = {h:'#f0883e', e:'#bc8cff', f:'#39d2c0'}`
- 数字格式化: `formatNum(n)` (K/M 缩写)
- 新状态变量: 在 `setup()` 内 `const xxx = ref(...)` 然后加到 `return { ... }`
- 新函数: 在 `setup()` 内定义然后加到 `return { ... }`
- **注意**: Vue 模板内不能出现跨行字符串, 否则会导致模板编译失败整个 SPA 空白

### 3.3 样式约定

- 暗色主题, CSS 变量定义在 `style.css` 顶部
- 关键 class: `.panel`, `.panel-header`, `.panel-body`, `.kpi-card`, `.badge`, `.compare-table`, `.chart-container`, `.chart-container-lg`, `.sub-tab`, `.btn`, `.issue-card`

### 3.4 API URL 模式

前端使用 `const API = window.location.origin` 拼接 API URL:
```js
fetch(`${API}/api/research/runs/${rid}/...`)
```

---

## 四、验证方法

### 4.1 启动验证
```bash
cd /Users/yangcongan/cursor/IP
python -m uvicorn webui.main:app --reload --host 0.0.0.0 --port 8000
```
打开 `http://localhost:8000`, 应能看到所有 4 个 Tab

### 4.2 API 验证
```bash
# 获取 run_id
curl http://localhost:8000/api/dashboard/runs | python3 -m json.tool | head

# 测试研究 API (替换实际 run_id)
RID="rb20v2_20260202_191900_sg_001"
curl "http://localhost:8000/api/research/runs/$RID/libraries/overview" | python3 -m json.tool | head
curl "http://localhost:8000/api/research/runs/$RID/h/tier-drift" | python3 -m json.tool | head
```

### 4.3 浏览器验证
- 协同看板: 无 Shard 矩阵, QA 有中文说明列
- 库画像研究 → 概览: 三库 KPI + 漏斗 + 饼图
- 库画像研究 → H深度: KPI + 热力图 + 触发器饼图 + 可疑切分表
- 库画像研究 → 样本: 点击样本跳 Explorer

---

## 五、开发顺序建议

```
3A-1 (wD what-if)  →  3A-2 (E what-if)  →  3B-1~3 (RulePack)
         ↓ 可并行
3C-1~4 (画像增强)  →  3D-1~2 (问题增强)  →  4-1~3 (Session)  →  5 (Agent)
```

**推荐优先做 3A-1 (wD what-if)**, 因为:
1. 纯后端计算, 不依赖新表
2. 是 codex 定义的"规模化关键"能力
3. 能立即产生研究价值 (回答"如果调 wD 分段, H 会怎样变")
