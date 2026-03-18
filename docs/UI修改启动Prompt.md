# UI 修改启动 Prompt

这份文档是下一次“UI 修改”新对话的启动入口。

使用方式：

1. 新对话开始时先读完本文件
2. 再读这里列出的核心文档与代码
3. 不要直接改 UI，先确认数据库主线和摘要层当前状态
4. 如果本次数据库工作又发现新信息，继续把新信息补回本文件

---

## 1. 当前项目状态

### 1.1 已完成

- IP 库主线数据库流程已经跑通
- 主线顺序已明确：
  - 统一 IP 成员
  - 连续块识别
  - 切分纯化
  - H/E/F 分流
  - QA 验收
- 服务器主线恢复 run 已完成：
  - `run_id = rb20v2_20260313_200300_sg_dynamic_fix04`
- Phase 7 恢复日志显示：
  - `2026-03-17 03:23:39 === Phase 7 Resume Success ===`
- 当前主线 QA 结果：
  - `RB20_06 = 244/244`
  - `RB20_07 = 244/244`
  - `RB20_08 = 244/244`
  - `QA STOP fail = 0`

### 1.2 未完成

- UI 还没有按最新主线口径做联动修正
- 当前数据库侧的主要未完成项已经从“主线/摘要”转为“UI 联动修正”

### 1.3 当前已知摘要层状态

以主线 run `rb20v2_20260313_200300_sg_dynamic_fix04` 为准：

- 主线表当前规模：
  - `h_blocks = 21365`
  - `e_runs = 130048`
  - `f_members = 2652717`
- 摘要层当前状态：
  - `h_block_summary = 21365`，已完成
  - `e_cidr_summary = 67682`，已完成
  - `f_ip_summary = 2652717`，已完成

说明：

- 当前 H 页面已经可以优先考虑走 `h_block_summary`
- 当前 F 页面已经可以优先考虑走 `f_ip_summary`
- 当前 E 页面已经可以优先考虑走 `e_cidr_summary`
- UI 修改启动时，必须先检查当前摘要层完成状态，再决定页面走摘要表还是退回主线表

注意：

- 对 run `rb20v2_20260313_200300_sg_dynamic_fix04`，`h_block_summary` 曾出现“行数已齐，但 `start_ip_text`、`avg_apps_per_ip` 等派生列未回填”的问题。
- 该问题已在 2026-03-17 修复，当前这个 run 的 H 摘要关键派生字段已经补齐。
- 对同一个 run，H 库曾发现 `valid_cnt < 4` 的块被误保留进 H，导致 UI 中出现看起来像“1 IP H 块”的现象。
- 该问题也已在 2026-03-17 修复，当前这个 run 下：
  - `h_blocks.valid_cnt < 4 = 0`
  - `h_block_summary.ip_count < 4 = 0`
- 这意味着“摘要表有数据”不等于“摘要表一定已经完全可直接驱动 UI”，未来 run 仍然要做字段完整性检查。
- 尤其 H 页面，进入 UI 修改前，除了确认 `COUNT(*)`，还要确认关键展示字段是否已补齐。

---

## 2. UI 修改前必须理解的事实

### 2.1 这个项目不是普通“列表展示”

UI 不是简单展示 IP 分类结果，而是要承接：

- H/E/F 的对象化结果
- 对象画像
- 标签展示
- 研究和问题定位过程

所以 UI 的基础认知必须是：

- 不是“单 IP 分类系统”
- 而是“对象构建 + 对象画像 + 标签研究系统”

### 2.2 H / E / F 的中文语义

- H 库：核心连续网络库
- E 库：稀疏聚合网络库
- F 库：零散剩余 IP 库

后续 UI 中不要继续只用字母名，至少要允许出现中文解释。

### 2.3 当前 UI 不能默认相信旧口径

之前数据库逻辑已经修过以下关键点，UI 不能继续按旧理解展示：

- `valid_cnt=0` 不进入 H
- `<4 IP` 小块不进入 H
- `short_run` 不进入 E
- F 不是 `R1 \\ is_e_atom`，而是 `R1 \\ actual E coverage`
- `keep = H ∪ E ∪ F`

如果 UI 里仍按旧 SQL、旧假设或旧字段解释写死，会再次出现“页面看不到数据”或“数量对不上”的问题。

---

## 3. UI 修改前必须先检查什么

### 3.1 先检查数据库状态

进入 UI 修改前，先确认：

1. 主线 run 使用哪个 `run_id`
2. H/E/F 主线表是否已经有数据
3. 三张摘要表是否已经有数据
4. UI 当前查询的是主线表还是摘要表
5. UI 当前默认 `run_id` 是否还是旧值

最低限度要查：

- `h_members`
- `e_members`
- `f_members`
- `h_block_summary`
- `e_cidr_summary`
- `f_ip_summary`
- `qa_assert`

最低限度还要补一层字段完整性检查：

- `h_block_summary.start_ip_text`
- `h_block_summary.avg_apps_per_ip`
- `e_cidr_summary.start_ip_text`
- `f_ip_summary` 中 UI 实际依赖的主展示字段

### 3.2 先检查 UI 数据链路

UI 修改时，不要只看前端页面，要把链路走通：

- 前端页面请求了哪个 endpoint
- endpoint 调了哪个 handler
- handler 对应哪段 SQL
- SQL 查的是哪张表
- 这个表在当前 `run_id` 下是否有数据

### 3.3 先确认展示单位

UI 修改必须明确每个页面的研究单位：

- H 页面：应优先按块级对象展示
- E 页面：应优先按 E run / CIDR 对象展示
- F 页面：应按零散 IP 展示，但仍需有画像摘要支持

不要把 H/E/F 都硬做成同一种列表。

---

## 4. 已知 UI 风险点

### 4.1 旧 run_id 风险

之前很多代码、文档和页面默认指向旧 run。

UI 修改时必须确认：

- 当前页面默认使用哪个 run
- 是否允许用户切 run
- 如果默认 run 没有摘要表，页面应该怎么提示

### 4.2 摘要层虽然已完成，但 UI 仍要保留状态感知

当前主线 run 的三张摘要表都已完成：

- `h_block_summary`
- `e_cidr_summary`
- `f_ip_summary`

但 UI 仍然不应把“摘要一定存在”写死到所有未来 run。
同时也不应把“摘要行数已经生成”直接等同于“关键派生字段已经全部可用”。

必须设计：

- 有摘要时展示摘要
- 无摘要时给出明确状态提示
- 摘要存在但关键派生字段未补齐时，给出明确状态提示或降级查询
- 不要返回空白页

### 4.3 UI 不应反向定义业务

UI 只能消费主线结果和摘要层结果，不能自己发明分类逻辑。

尤其不能：

- 用标签反推 H/E/F
- 用展示统计反过来定义对象边界
- 用前端便捷性替代数据库主线定义

---

## 5. UI 修改时必须阅读的文档

优先顺序如下：

1. [01_业务方案版.md](/Users/yangcongan/cursor/IP/docs/IP库核心逻辑/01_业务方案版.md)
2. [02_实现规范版.md](/Users/yangcongan/cursor/IP/docs/IP库核心逻辑/02_实现规范版.md)
3. [03_核心表清单.md](/Users/yangcongan/cursor/IP/docs/IP库核心逻辑/03_核心表清单.md)
4. [04_切分算法清单.md](/Users/yangcongan/cursor/IP/docs/IP库核心逻辑/04_切分算法清单.md)
5. [05_代码依据索引.md](/Users/yangcongan/cursor/IP/docs/IP库核心逻辑/05_代码依据索引.md)
6. [09_20260313服务器执行记录.md](/Users/yangcongan/cursor/IP/docs/IP库核心逻辑/09_20260313服务器执行记录.md)

补充阅读：

7. [03_continuation_plan.md](/Users/yangcongan/cursor/IP/docs/07_ip_profiling_visual_research/03_continuation_plan.md)
8. [02_agent_prompt.md](/Users/yangcongan/cursor/IP/docs/07_ip_profiling_visual_research/02_agent_prompt.md)
9. [prompt_ui_fix.md](/Users/yangcongan/cursor/IP/docs/11_sg004_data_audit/prompt_ui_fix.md)

---

## 6. UI 修改时必须检查的代码

后端：

- [webui/launcher.py](/Users/yangcongan/cursor/IP/webui/launcher.py)
- [webui/main.py](/Users/yangcongan/cursor/IP/webui/main.py)
- [webui/api/profiling.py](/Users/yangcongan/cursor/IP/webui/api/profiling.py)
- [webui/api/dashboard.py](/Users/yangcongan/cursor/IP/webui/api/dashboard.py)
- [webui/api/explorer.py](/Users/yangcongan/cursor/IP/webui/api/explorer.py)
- [webui/api/research.py](/Users/yangcongan/cursor/IP/webui/api/research.py)
- [webui/models/database.py](/Users/yangcongan/cursor/IP/webui/models/database.py)
- [webui/models/schemas.py](/Users/yangcongan/cursor/IP/webui/models/schemas.py)

前端：

- [webui/static/index.html](/Users/yangcongan/cursor/IP/webui/static/index.html)
- [webui/static/assets/style.css](/Users/yangcongan/cursor/IP/webui/static/assets/style.css)

配置：

- [webui/config/profile_tags.json](/Users/yangcongan/cursor/IP/webui/config/profile_tags.json)
- [webui/config/e_profile_tags.json](/Users/yangcongan/cursor/IP/webui/config/e_profile_tags.json)
- [webui/config/f_profile_tags.json](/Users/yangcongan/cursor/IP/webui/config/f_profile_tags.json)

---

## 7. 建议的 UI 修改启动步骤

新对话开始后，按这个顺序做：

1. 先确认本文件里的状态是否仍然最新
2. 再确认当前摘要层实际完成到哪一张表
3. 画出现有 UI 的数据流图：
   - 页面
   - endpoint
   - SQL
   - 表
   - run_id
4. 找出 H/E/F 页面当前分别依赖哪张表
5. 判断哪些页面可以立刻修
6. 判断哪些页面必须等摘要层补完
7. 最后再动代码

---

## 8. 当前对 UI 修改的建议边界

建议优先级：

1. 先修“能否正确显示当前主线 run”
2. 再修“页面字段和中文语义是否一致”
3. 再修“研究型可视化体验”
4. 最后才做风格层优化

原因：

- 现在最核心的问题不是视觉，而是口径和数据链路
- UI 必须先对齐数据库主线，才值得做更深的交互和美化

---

## 9. 后续补充规则

如果数据库工作又出现新信息，补充时优先写入：

- 当前有效 `run_id`
- 摘要层完成状态
- 新修复的主线逻辑
- UI 不能再沿用的旧口径
- 哪些页面依赖的表已经确认稳定
- 哪些页面仍应标记为“待摘要层完成”
