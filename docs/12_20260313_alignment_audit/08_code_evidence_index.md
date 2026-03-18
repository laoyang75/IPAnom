# 代码依据

本文件按主题列出支撑结论的直接证据与合理推断。

说明：

- `直接证据`：代码、SQL、表结构或数据库记录中明确存在。
- `合理推断`：代码没有直接写成一句话，但可以由实现顺序和依赖关系稳定推出。

## 1. 主流程入口与调度顺序

直接证据：

- `Y_IP_Codex_RB2_5/04_runbook/orchestrate_fresh_start_v2.py:15-18`
  - 主编排默认 `RUN_ID/CONTRACT_VERSION/SHARD_CNT/CONCURRENCY`
- `Y_IP_Codex_RB2_5/04_runbook/orchestrate_fresh_start_v2.py:39-42`
  - Step03/Step11 使用优化脚本与 `03_post_process.sql`
- `Y_IP_Codex_RB2_5/04_runbook/orchestrate_fresh_start_v2.py:221-325`
  - 主流程顺序：Run Init -> ShardPlan -> 01A -> 01/02 -> Step03 -> 03_post -> Step11 -> 04/04P -> 05 -> 06/07/08 -> 99
- `docs/05_final_pipeline_code.md:21-40`
  - 文档版阶段顺序与主编排一致

合理推断：

- 当前主分类流程的权威顺序应以 `orchestrate_fresh_start_v2.py` 为主，而不是以零散辅助脚本为主。

## 2. 原始输入与统一 IP 成员表

直接证据：

- `Y_IP_Codex_RB2_5/README.md:18-23`
  - 原始输入锚点：W 源表 + A 异常表
- `Y_IP_Codex_RB2_5/03_sql/RB20_01/01A_abnormal_dedup.sql:7-14`
  - 异常表写入 `abnormal_dedup`
- `Y_IP_Codex_RB2_5/03_sql/RB20_01/01_source_members_shard.sql:9-118`
  - `source_members` 来源、字段镜像、中国过滤、异常标记、`atom27_id`、`bucket64`
- `Y_IP_Codex_RB2_5/03_sql/00_contracts/01_ddl_rb20_v2_full.sql:41-113`
  - `source_members` 正式表结构

合理推断：

- `source_members` 是主流程的统一输入层，而不是简单的镜像备份表。

## 3. 异常 IP 的参与方式

直接证据：

- `01_source_members_shard.sql:102-110`
  - 异常通过 LEFT JOIN 标记，不在成员层删除
- `03_pre_profile_shard.sql:141-146`
  - 新版 SQL 把 `ALL_ABNORMAL_BLOCK` 当标记，不丢弃
- `orchestrate_step03_bucket_full.py:308-310`
  - 优化版 Step03 仍把 `valid_cnt=0` 设为 `keep_flag=false`
- `03_post_process.sql:11-36`
  - 后处理按 `keep_flag` 派生 Keep/Drop
- `build_h_block_summary.py:189-236`
  - H 摘要聚合时排除异常 IP，但单独统计异常 IP 数

数据库直接证据：

- `profile_pre` 当前 `sg_001` 与 `sg_004` 的 keep/drop 行为不同
- `config_kv` 的 `dp_005_keep_drop_rule` 仍写 `drop_only_all_abnormal_else_keep`

合理推断：

- 仓库当前同时存在“异常只标记”和“全异常块剔除”两套 Step03 语义。

## 4. 连续地址段识别

直接证据：

- `02_natural_blocks_shard.sql:17-64`
  - 用 `LAG(ip_long)` 判断连续性
- `01_ddl_rb20_v2_full.sql:114-139`
  - `block_natural` 与 `map_member_block_natural` 表结构

合理推断：

- 连续地址段定义只依赖地址连续性，不依赖运营商或行为特征。

## 5. 切分规则与顺序

直接证据：

- `11_window_headtail_shard.sql:1-136`
  - `/64` 边界窗口、`k=5`、valid-only、唯一运营商条件
- `04_split_and_final_blocks_shard.sql:96-183`
  - Report/Mobile/Operator/Density 4 类边界触发
- `04_split_and_final_blocks_shard.sql:185-223`
  - Void Zone 强制切分
- `04_split_and_final_blocks_shard.sql:225-287`
  - 根据切点生成 `block_final`
- `04_split_and_final_blocks_shard.sql:289-380`
  - `16-IP` 子窗口密度补充切分

合理推断：

- 当前切分的主顺序是：
  - 候选筛选
  - `/64` 边界评估
  - 空洞补丁
  - 生成 final block
  - `16-IP` 二次切分

## 6. H/E/F 分流规则

直接证据：

- `05_h_blocks_and_members.sql:14-49`
  - H 准入：`tier in 中/大/超大 + member_cnt_total >= 4`
- `06_r1_members_shard.sql:13-31`
  - `R1 = Keep \ H`
- `07_e_atoms_runs_members_shard.sql:24-58`
  - E 原子规则：`valid_ip_cnt >= 7`
- `07_e_atoms_runs_members_shard.sql:60-95`
  - E runs 规则：连续 `is_e_atom` 原子，`run_len < 3` 仅标记 `short_run`
- `07_e_atoms_runs_members_shard.sql:97-130`
  - `e_members` 实际包含 short_run
- `08_f_members_shard.sql:13-32`
  - F 主流程：`R1 \ is_e_atom`

辅助路径直接证据：

- `rebuild_f_and_summary.py:1-4, 71-102`
  - F 修复脚本按 `source \ H \ E \ Drop` 重建

合理推断：

- 主流程定义应以 `06/07/08` 为准。
- `rebuild_f_and_summary.py` 属于修复/补建路径，不应覆盖主流程定义。

## 7. 画像摘要与标签位置

直接证据：

- `build_h_block_summary.py:1-16, 169-236, 299-361`
  - H 摘要来自 H + 原始表；聚合时排除异常 IP
- `build_e_cidr_summary.py:1-16, 181-385`
  - E 摘要来自 E + 原始表；结构信息来自 `e_runs`
- `rebuild_f_and_summary.py:1-4, 132-233`
  - F 摘要来自 F + 原始表
- `webui/api/profiling.py:19-23`
  - 标签数据源只接受摘要表
- `webui/api/profiling.py:152-227`
  - 标签按顺序在摘要表上做漏斗计算
- `webui/config/profile_tags.json`
- `webui/config/e_profile_tags.json`
- `webui/config/f_profile_tags.json`
  - 各库主标签配置

合理推断：

- 标签是后置解释层，不是主分类层。
- 当前代码没有独立附加标签持久化机制。

## 8. 参数、写死项与隐式规则

直接证据：

- `00_run_init.sql:13-25`
  - `config_kv` 写入 DP 选择
- `10_shard_plan_generate_sql_only.sql:1-159`
  - shard_plan 为实现层参数化逻辑，`shard_cnt` 可配置
- `orchestrate_step03_bucket_full.py:28-30, 196-198`
  - `TARGET_ROWS_PER_BUCKET`、`CONCURRENCY`、`enable_nestloop=off`、`jit=off`
- `orchestrate_step11_chunked.py:27-29`
  - `BLOCK_CHUNK_SIZE`、`CONCURRENCY`
- `04_split_and_final_blocks_shard.sql:155-159, 188-209, 309-335`
  - 切分阈值写死在 SQL 中
- `07_e_atoms_runs_members_shard.sql:7, 57, 92`
  - E 关键阈值写死

合理推断：

- 并发、分桶、`work_mem` 属于实现优化参数。
- 切分阈值、H/E/F 边界阈值属于业务规则，不能被“优化”名义偷偷改变。

## 9. 代码与业务理解的主要差异

直接证据：

- `03_pre_profile_shard.sql:141-146` vs `orchestrate_step03_bucket_full.py:308-310`
  - Step03 keep/drop 口径冲突
- `05_h_blocks_and_members.sql:31-34`
  - H 已扩展为中/大/超大 + `>=4`
- `webui/api/research.py:163-167, 267-278, 335-337`
  - 页面局部仍按“仅中型网络”消费 H
- `webui/static/index.html:76-80`
  - Dashboard 文案仍写 “H 类 (中型网络)”
- 数据库：
  - `sg_001` shard 数 `65`
  - `sg_004` shard 数 `242`
- `build_e_cidr_summary.py:52-62`
  - 新脚本已改为动态读取 shard

合理推断：

- 仓库正处于“业务口径更新快于主编排收敛”的状态。
- 正式规范必须显式区分“主流程语义”和“辅助修复脚本语义”。
