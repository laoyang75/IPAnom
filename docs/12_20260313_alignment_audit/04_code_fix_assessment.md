# 数据代码问题与修复顺序初评

这部分不是立即动手修，而是给你一个对齐后的执行顺序。

## 1. 先修“定义”，不要先修“代码”

当前最危险的不是代码写错一行，而是三套对象没对齐：

- 业务口径里的 `D库`
- 代码里的 `H/E/F`
- 页面看的 `summary + 标签`

如果不先定对象，越修越乱。

## 2. 按影响面排序的修复顺序

### P0. 冻结库定义

先定以下规则：

- `D库` 是否等于 `H库`
- 大网络库最小块尺寸是否固定为 `>=4`
- 是否允许 `valid_cnt=0` 全异常块进主库
- `E` 是否要求 `run_len>=3`
- 标签是否允许多主标签

不做这一步，任何代码修复都可能返工。

### P1. 修 H/E/F 边界代码

受影响文件：

- `Y_IP_Codex_RB2_5/03_sql/RB20_05/05_h_blocks_and_members.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_07/07_e_atoms_runs_members_shard.sql`
- 可能还要连带 `RB20_06` / `RB20_08`

重点：

- H 的最终准入条件
- E 的短 run 归属
- F 的兜底边界

### P2. 修摘要层镜像关系

受影响文件：

- `Y_IP_Codex_RB2_5/04_runbook/build_h_block_summary.py`
- `Y_IP_Codex_RB2_5/04_runbook/build_e_cidr_summary.py`
- `Y_IP_Codex_RB2_5/04_runbook/rebuild_f_and_summary.py`

重点：

- summary 是否完整覆盖分类表
- `valid_cnt=0` 对象如何摘要
- 页面所用 summary 是否能反映真实库对象

### P3. 修 UI / API 的口径消费

受影响文件：

- `webui/api/research.py`
- `webui/api/explorer.py`
- `webui/api/profiling.py`
- `webui/api/dashboard.py`
- `webui/static/index.html`

重点：

- 页面是否仍把 H 当成“仅中型网络”
- 页面默认 run 是否指向未验收版本
- 标签页是否明确在看 summary 还是 raw classification

### P4. 再决定是否回头修上游切分

受影响文件：

- `Y_IP_Codex_RB2_5/03_sql/RB20_04/04_split_and_final_blocks_shard.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_04P/04P_final_profile_shard.sql`

重点：

- 是否在切分层引入最小业务块尺寸
- 是否允许小碎块继续存在于 raw 层，但不进入业务层

## 3. 当前最可能的修复策略

如果按我当前理解，最稳妥的执行路径是：

1. 先把 `D/H` 对齐成一个明确定义
2. 冻结 H 的 `member_cnt_total >= 4`
3. 明确 `valid_cnt=0` 的处理去向
4. 明确 `E` 是否接受短 run
5. 修 summary 镜像问题
6. 修页面口径和默认 run
7. 最后再决定是否从 Step 04 根修碎块生成

## 4. 修复前必须先做的小样本验证

无论选哪条方案，都不要直接全量重跑。建议顺序：

1. 选 1 到 2 个 shard 做 H/E/F 边界验证
2. 验证守恒和互斥
3. 验证 summary 与 classification 是否一一对应
4. 验证标签结果是否符合你选定的标签模型
5. 最后再决定是否全量重建
