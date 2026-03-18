# H/E/F 库质量修复与画像补全 — 深度分析 Prompt

## 背景

全量重跑 `sg_004`（RUN_ID: `rb20v2_20260202_191900_sg_004`）已完成，H∩E=0, F∩E=0 交叉验证通过。但经检查发现以下质量和功能问题需要修复。

## 当前 sg_004 数据规模

| 表 | 行数 |
|---|---|
| source_members | 59,706,088 |
| h_members | 16,267,013 |
| e_members | 16,289,975 |
| f_members | 641,438 |
| h_blocks | 29,014 |
| e_cidr_summary | **0**（未构建） |
| f_ip_summary | **0**（未构建） |

---

## 问题清单

### 问题 1：H 库含大量单 IP 块（违反 CIDR 连续块定义）

H 库（Hub Library）定义为核心的**连续 CIDR 段**——其入库标准应要求 block 至少包含一定数量的连续 IP（如 ≥4 IP），**不应该有单个 IP**。但实际数据：

```
H blocks size 分布 (via block_final JOIN h_blocks):
1 (single IP)  →  5,966 (20.6%)
2-3 IP         →  1,564
4-15 IP        →  1,506
16-63 IP       →  2,068
64-255 IP      →  5,561
256+ IP        → 12,349
```

**5,966 个单 IP 块**，占 H 库总块数 20.6%，这违反了 H 库的定义。

**需要分析**：
- Step 05 (`05_h_blocks_and_members.sql`) 的 H 库准入逻辑——是否缺少最小 block 尺寸过滤条件
- block_final 中单 IP 块的来源——Step 04 分割是否过度（将 natural block 切割成了单 IP 碎片）
- 确定合理的最小 H 块尺寸阈值（建议 ≥4 IP 或 ≥/30 CIDR）

**检查方法**：
```sql
-- 查看单 IP H blocks 的画像（density/network_tier）
SELECT hb.network_tier_final, COUNT(*) 
FROM rb20_v2_5.h_blocks hb
JOIN rb20_v2_5.block_final bf ON bf.run_id=hb.run_id AND bf.block_id_final=hb.block_id_final
WHERE hb.run_id='rb20v2_20260202_191900_sg_004' AND bf.ip_end=bf.ip_start
GROUP BY 1;

-- 查看这些单 IP 块在 Step 03 profile_pre 中的原始属性
-- 它们可能是 split 后的碎片
```

---

### 问题 2：E 库摘要画像未构建（e_cidr_summary = 0）

E 库（Extended Library）的摘要画像表 `e_cidr_summary` 对 sg_004 为空。这个表是 `build_e_cidr_summary.py` 脚本生成的，不是 Pipeline SQL 的一部分。

**需要做的**：
- 运行 `build_e_cidr_summary.py` 为 sg_004 构建 E 库摘要
- 脚本位置：`/Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/04_runbook/build_e_cidr_summary.py`
- 注意：该脚本可能硬编码了旧的 run_id，需检查

**检查方法**：
```bash
# 查看脚本中的 run_id 配置
grep -n 'run_id' /Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/04_runbook/build_e_cidr_summary.py
```

---

### 问题 3：F 库缺画像标签和摘要画像

F 库（Filtrate Library）定义为**不在 H 库也不在 E 库**中的 IP。目前 F 库只有 `f_members`（641,438 IP）和 `f_ip_summary`（空），缺少：

1. **F 库画像标签**（类似 E 库的 `e_profile_tags.json`）
2. **F 库摘要画像**（类似 E 库的 `e_cidr_summary`，没有对应的 `f_cidr_summary` 或等效表）

这意味着无法在 WebUI 中研究 F 库 IP 的行为特征。

**需要做的**：
- 参照 E 库的画像标签体系（`e_profile_tags.json`），为 F 库设计合适的画像标签
- 构建 F 库摘要画像表（可能是 `f_cidr_summary` 或复用 `f_ip_summary`）
- 在 WebUI 中添加 F 库的标签漏斗和数据展示

**参考文件**：
- E 库标签定义：`/Users/yangcongan/cursor/IP/webui/config/e_profile_tags.json`
- E 库摘要构建：`/Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/04_runbook/build_e_cidr_summary.py`
- WebUI profiling API：`/Users/yangcongan/cursor/IP/webui/api/profiling.py`

---

### 问题 4：WebUI 中 H 库数据不显示

用户打开 H 库页面看不到数据。可能原因：

1. **run_id 不匹配**：`config_kv` 中只有 sg_002 的配置，WebUI 的 `_resolve_run()` 函数（profiling.py L127-134）会取 latest run_id，但如果查询的表为空或 run_id 格式不匹配，可能返回空
2. **e_cidr_summary 为空**：如果 WebUI 的 H 库展示依赖 `e_cidr_summary` 或类似表，sg_004 的数据为空会导致页面无数据
3. **profiling API 数据源**：H 库的 profiling 使用 `h_blocks` 表（有数据）但画像漏斗可能依赖 `e_cidr_summary`

**需要分析**：
- WebUI profiling.py 的 `_resolve_run()` 和 `compute_funnel()` 对 H 库的查询逻辑
- 前端页面加载时请求了哪些 API endpoint
- 检查浏览器 Network 面板看哪些请求返回空数据

**关键文件**：
- API：`/Users/yangcongan/cursor/IP/webui/api/profiling.py` — `compute_funnel`, `remaining_stats`
- API：`/Users/yangcongan/cursor/IP/webui/api/dashboard.py` — `run_overview`, `list_runs`
- 前端：`/Users/yangcongan/cursor/IP/webui/` 下的 HTML/JS 文件

---

## 数据库优化上下文（已完成）

本次重跑过程中已做了以下 PG 优化，新对话中可直接使用：

| 优化 | 状态 |
|---|---|
| max_connections = 200 | ✅ 已生效 |
| work_mem = 128MB（全局） | ✅ |
| SET enable_nestloop = off（session 级） | ✅ 关键修复 |
| VACUUM e_members/r1_members/block_final | ✅ |
| Step 04 ON CONFLICT DO UPDATE | ✅ 已修补服务器上的 SQL |

---

## 服务器信息

- 服务器：`192.168.200.217`（SSH root / 111111）
- PG：PostgreSQL 15.13，DB `ip_loc2`，schema `rb20_v2_5`
- 当前 RUN_ID：`rb20v2_20260202_191900_sg_004`
- SQL 文件目录（服务器）：`/tmp/rb20_sql/`
- 本地 SQL 目录：`/Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/03_sql/`
- WebUI 目录：`/Users/yangcongan/cursor/IP/webui/`

---

## 执行要求

1. **先诊断再修复**——对每个问题先用 SQL 查证据，确认根因后再改代码
2. **修改 SQL 后在小样本上验证**——不要直接全量跑，先对 1-2 个 shard 做测试
3. **建立自动化测试**——每个修复完成后写验证 SQL 确认效果
4. **F 库画像设计需参照 E 库**——保持一致的标签体系和 UI 交互
5. **WebUI 修改后用浏览器验证**——确保前端能正确展示 H/E/F 三个库的数据
