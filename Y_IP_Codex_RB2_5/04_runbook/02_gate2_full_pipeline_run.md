# Gate 2（全链路）Runbook（并行 ≤32；按 shard_cnt 分批）

主版本：`Y_IP_Codex_RB2_5/重构2.md`

目的：以可并行、可重跑、可验收的方式跑通 RB20_00D → RB20_99 全链路，并为阶段报告提供可复现 SQL 锚点。

## 0) 选择 run_id / contract_version

- `run_id`：按 `Y_IP_Codex_RB2_5/README.md` 规则
- `contract_version`：建议固定 `contract_v1`

## 1) 初始化（全局）

1) 初始化 Run/Config
- 执行：`Y_IP_Codex_RB2_5/03_sql/00_contracts/00_run_init.sql`

2) 更新宽视图（如本地文件有更新）
- 执行：`Y_IP_Codex_RB2_5/03_sql/00_contracts/03_views_rb20_v2.sql`

3) 生成 ShardPlan（全局）
- 执行：`Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`

4) 异常去重（全局）
- 执行：`Y_IP_Codex_RB2_5/03_sql/RB20_01/01A_abnormal_dedup.sql`

## 2) 第一段并行（per-shard：01/02/03/11/04/04P）

一次并行跑 ≤32 个 shard（按 shard_cnt 分批/队列跑完），每个 shard 串行执行：

1) `Y_IP_Codex_RB2_5/03_sql/RB20_01/01_source_members_shard.sql`
2) `Y_IP_Codex_RB2_5/03_sql/RB20_02/02_natural_blocks_shard.sql`
3) `Y_IP_Codex_RB2_5/03_sql/RB20_03/03_pre_profile_shard.sql`
4) `Y_IP_Codex_RB2_5/03_sql/RB20_11/11_window_headtail_shard.sql`
5) `Y_IP_Codex_RB2_5/03_sql/RB20_04/04_split_and_final_blocks_shard.sql`
6) `Y_IP_Codex_RB2_5/03_sql/RB20_04P/04P_final_profile_shard.sql`

幂等重跑：
- 任意 shard 失败：只重跑该 shard 的 6 步（每步均为 DELETE+INSERT）

## 3) H（全局：RB20_05）

等待所有 shard 完成 RB20_04P 后执行：
- `Y_IP_Codex_RB2_5/03_sql/RB20_05/05_h_blocks_and_members.sql`

## 4) 第二段并行（per-shard：06/07/08）

每个 shard 串行执行：

1) `Y_IP_Codex_RB2_5/03_sql/RB20_06/06_r1_members_shard.sql`
2) `Y_IP_Codex_RB2_5/03_sql/RB20_07/07_e_atoms_runs_members_shard.sql`
3) `Y_IP_Codex_RB2_5/03_sql/RB20_08/08_f_members_shard.sql`

## 5) 终验收（全局：RB20_99）

- `Y_IP_Codex_RB2_5/03_sql/RB20_99/99_qa_assert.sql`
  - 注意：需替换 `{{run_id}}/{{contract_version}}/{{shard_cnt}}`

验收：
- `SELECT * FROM rb20_v2_5.qa_assert WHERE run_id='{{run_id}}' AND NOT pass_flag;` 应返回 0 行（否则 STOP）
