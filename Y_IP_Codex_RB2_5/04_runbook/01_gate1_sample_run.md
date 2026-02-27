# Gate 1（样本跑通）Runbook

目的：先用少量 shard 跑通 RB20_01 → RB20_04P（含 Step64 切分与 FinalProfile），验证幂等、口径与画像字段落表正确。

## 0) 前置条件

- Gate 0 已完成：
  - `rb20_v2_5` schema 与空表已创建（`Y_IP_Codex_RB2_5/03_sql/00_contracts/01_ddl_rb20_v2_full.sql`）
  - 索引已创建（`Y_IP_Codex_RB2_5/03_sql/00_contracts/02_indexes_rb20_v2.sql`）
- 合同 v1 已固化（`Y_IP_Codex_RB2_5/02_contracts/schema_contract_v1.md`、`Y_IP_Codex_RB2_5/02_contracts/metric_contract_v1.md`、`Y_IP_Codex_RB2_5/02_contracts/report_contract_v1.md`）
  - 画像宽字段视图已更新（如本地文件有更新，重跑一次）：`Y_IP_Codex_RB2_5/03_sql/00_contracts/03_views_rb20_v2.sql`

## 1) 选择 run_id 与 contract_version

- `run_id`：按 `Y_IP_Codex_RB2_5/README.md` 规则，例如：`rb20v2_20260107_093000_sg_001`
- `contract_version`：建议固定 `contract_v1`

## 2) 初始化 Run/Config

- 执行：`Y_IP_Codex_RB2_5/03_sql/00_contracts/00_run_init.sql`（替换 `{{run_id}}/{{contract_version}}`）

## 3) 生成 ShardPlan

- 执行：`Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`
  - 替换 `{{run_id}}/{{contract_version}}/{{eps}}`（eps 默认 0.10）
  - 该脚本会对 W 源表做一次扫描+聚合（/16 直方图），不需要全量排序；建议低峰运行

验证：
- `SELECT COUNT(*) FROM rb20_v2_5.shard_plan WHERE run_id='{{run_id}}';` 应为 64
- `SELECT COUNT(*) FROM rb20_v2_5.shard_plan WHERE run_id='{{run_id}}' AND est_rows=0;` 应为 0

## 4) 全局异常去重（RB20_01A）

- 执行：`Y_IP_Codex_RB2_5/03_sql/RB20_01/01A_abnormal_dedup.sql`

验证：
- `SELECT COUNT(*) FROM rb20_v2_5.abnormal_dedup WHERE run_id='{{run_id}}';`

## 5) 样本 shard 执行（建议 shard_id=0、1）

对每个 shard：

1) RB20_01（Source Members）
- 执行：`Y_IP_Codex_RB2_5/03_sql/RB20_01/01_source_members_shard.sql`（替换 `{{shard_id}}`）

2) RB20_02（Natural Blocks）
- 执行：`Y_IP_Codex_RB2_5/03_sql/RB20_02/02_natural_blocks_shard.sql`

3) RB20_03（Pre Profile / PreH / KeepDrop Members）
- 执行：`Y_IP_Codex_RB2_5/03_sql/RB20_03/03_pre_profile_shard.sql`

4) RB20_11（HeadTail Window）
- 执行：`Y_IP_Codex_RB2_5/03_sql/RB20_11/11_window_headtail_shard.sql`

5) RB20_04（SplitEvents + Final Blocks + Final Map）
- 执行：`Y_IP_Codex_RB2_5/03_sql/RB20_04/04_split_and_final_blocks_shard.sql`

6) RB20_04P（Final Profile）
- 执行：`Y_IP_Codex_RB2_5/03_sql/RB20_04P/04P_final_profile_shard.sql`

验证（每 shard）：
- `SELECT COUNT(*) FROM rb20_v2_5.source_members WHERE run_id='{{run_id}}' AND shard_id={{shard_id}};`
- `SELECT COUNT(*) FROM rb20_v2_5.block_natural WHERE run_id='{{run_id}}' AND shard_id={{shard_id}};`
- `SELECT COUNT(*) FROM rb20_v2_5.profile_pre WHERE run_id='{{run_id}}' AND shard_id={{shard_id}};`
- `SELECT COUNT(*) FROM rb20_v2_5.profile_pre WHERE run_id='{{run_id}}' AND shard_id={{shard_id}} AND valid_cnt=0;`（应存在则这些块应 Drop）
- `SELECT COUNT(*) FROM rb20_v2_5.preh_blocks WHERE run_id='{{run_id}}' AND shard_id={{shard_id}};`
- `SELECT COUNT(*) FROM rb20_v2_5.window_headtail_64 WHERE run_id='{{run_id}}' AND shard_id={{shard_id}};`
- `SELECT COUNT(*) FROM rb20_v2_5.split_events_64 WHERE run_id='{{run_id}}' AND shard_id={{shard_id}};`
- `SELECT COUNT(*) FROM rb20_v2_5.block_final WHERE run_id='{{run_id}}' AND shard_id={{shard_id}};`
- `SELECT COUNT(*) FROM rb20_v2_5.profile_final WHERE run_id='{{run_id}}' AND shard_id={{shard_id}};`

幂等重跑：
- 直接重复执行同一 shard 的 6 步 SQL，不应出现重复行（因为每步都先 DELETE 再 INSERT）

## 6) 阶段报告

样本跑通后，生成 Gate 1 报告到 `Y_IP_Codex_RB2_5/06_reports/`（下一步我会补齐模板与统计查询）。
