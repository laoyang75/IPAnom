# Gate 1（样本跑通）报告模板

主版本：`Y_IP_Codex_RB2_5/重构2.md`  
可复现入口：`Y_IP_Codex_RB2_5/04_runbook/01_gate1_sample_run.md`

## 1) 目的

- 用少量 shard 验证 RB20_01→RB20_04P 的幂等性、口径一致性与关键实体落表。

## 2) 输入/输出实体

输入：
- W 源表：`public."ip库构建项目_ip源表_20250811_20250824_v2_1"`
- A 异常表：`public."ip库构建项目_异常ip表_20250811_20250824_v2"`

输出（按 run_id + shard_id）：
- `rb20_v2_5.source_members`
- `rb20_v2_5.block_natural` / `rb20_v2_5.map_member_block_natural`
- `rb20_v2_5.profile_pre` / `rb20_v2_5.preh_blocks` / `rb20_v2_5.keep_members` / `rb20_v2_5.drop_members`
- `rb20_v2_5.window_headtail_64` / `rb20_v2_5.split_events_64`
- `rb20_v2_5.block_final` / `rb20_v2_5.map_member_block_final` / `rb20_v2_5.profile_final`

## 3) 核心指标（证据）

按 shard 查看（均来自 `rb20_v2_5.step_stats`）：
- RB20_01：`source_members_rows`、`source_members_abnormal_rows`
- RB20_02：`natural_block_cnt_total`、`natural_block_cnt_ge4`
- RB20_03：`keep_block_cnt`、`drop_block_cnt`、`valid_cnt_eq_0_block_cnt`
- RB20_11：`window_rows_cnt`、`window_cnt0_rows_cnt`
- RB20_04：`split_events_cnt`、`split_events_cnt0_cnt`、`cut_cnt`、`final_block_cnt`
- RB20_04P：`final_profile_block_cnt`、`final_profile_invalid_block_cnt`

## 4) 异常/风险与解释

- `valid_cnt=0`：必须全部 Drop；且不得进入 H/E/F
- Step64：`cntL_valid=0 OR cntR_valid=0` 的事件必须落表（不触发但要审计）

## 5) 可复现 SQL

- `Y_IP_Codex_RB2_5/03_sql/RB20_01/01_source_members_shard.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_02/02_natural_blocks_shard.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_03/03_pre_profile_shard.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_11/11_window_headtail_shard.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_04/04_split_and_final_blocks_shard.sql`
- `Y_IP_Codex_RB2_5/03_sql/RB20_04P/04P_final_profile_shard.sql`
