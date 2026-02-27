# RB20 v2.0（Y_IP_Codex_RB2_5）全量重建策略（可直接执行）

目标：在 **新输出 schema `rb20_v2_5`** 内，从零全量重建 RB20 v2.0（Natural → Pre → Step64 → Final → H/E/F → QA），并把“慢/不正常”的瓶颈用 micro-bench 快速量化定位。

## 0) 已完成（本次我已落地）

- 已创建新输出 schema 与空表/索引/视图：`rb20_v2_5`
  - DDL：`Y_IP_Codex_RB2_5/03_sql/00_contracts/01_ddl_rb20_v2_full.sql`
  - 索引：`Y_IP_Codex_RB2_5/03_sql/00_contracts/02_indexes_rb20_v2.sql`
  - 视图：`Y_IP_Codex_RB2_5/03_sql/00_contracts/03_views_rb20_v2.sql`

## 1) 核心“堵点”与本版本的处理方式

1) ShardPlan 生成过慢（全量排序/NTILE）
- 处理：改为按 `/16`（`ip_long >> 16`）直方图累计近似分位切分，避免 5,000 万级排序
- 文件：`Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`

2) RB20_11 Window 计算重复 join 退化
- 处理：改为“一次性展开 PreH valid 成员，再按 (block,bucket64) 聚合”，避免 cand×map 回扫
- 文件：`Y_IP_Codex_RB2_5/03_sql/RB20_11/11_window_headtail_shard.sql`

3) 历史 run 的卡点本质：漏跑 shard / shard_cnt 不一致
- 处理：新 run **必须在开跑前写死 `shard_cnt`**，并在 QA_Assert 里强校验全覆盖
- QA：`Y_IP_Codex_RB2_5/03_sql/RB20_99/99_qa_assert.sql`

## 2) 推荐参数（第一次全量）

- `shard_cnt=64`（先跑通；若仍有单 shard 明显过慢，再按 DP-014 提升到 128/192）
- 并发：`CONCURRENCY=16~32`（按 DB 资源；不要超过 32）

## 3) 执行方式（推荐：统一用 orchestrator）

用 `Y_IP_Codex_RB2_5/04_runbook/orchestrate_rb20_v2.py` 跑全链路（会按 shard_plan 拉 shard 列表，并执行校验与 QA）：

```bash
export RUN_ID='rb20v2_YYYYMMDD_HHMMSS_sg_001'
export SHARD_CNT='64'
export CONCURRENCY='32'
python3 Y_IP_Codex_RB2_5/04_runbook/orchestrate_rb20_v2.py
```

## 4) 执行前/中/后如何定位“慢”

1) 先做 micro-bench（强烈建议）
- 任务单：`Y_IP_Codex_RB2_5/04_runbook/04_perf_eval_and_fix_tasks_v1.md`

2) 旧库基准（已做，可对照）
- 报告：`Y_IP_Codex_RB2_5/06_reports/05_perf_eval_rb20_v2_20260128.md`

2) 运行中发现异常/漏跑 shard
- 诊断 SQL（不扫大表）：`Y_IP_Codex_RB2_5/04_runbook/diagnose_run_status.sql`

3) 终验收（必须）
- 执行：`Y_IP_Codex_RB2_5/03_sql/RB20_99/99_qa_assert.sql`（注意替换 `{{shard_cnt}}`）
- 要求：`rb20_v2_5.qa_assert` 中所有 `severity=STOP` 均 `pass_flag=true`
