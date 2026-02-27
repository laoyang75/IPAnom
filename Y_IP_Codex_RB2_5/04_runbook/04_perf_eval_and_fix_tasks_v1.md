# RB20 v2.0 — 性能评估 & 修复任务单（v1）

目标：先“低成本定位卡点”，再用“小数据/少 shard”做 micro-bench，最后把优化固化为可复跑版本（新 run_id）。

> 本任务单面向你安排的执行 agent：每个任务都可以独立执行并回传结果；不需要理解全部业务细节。

---

## A. 现状核验（不扫大表，5 分钟内完成）

1) 运行状态诊断（读 step_stats + shard_plan）
- SQL：`Y_IP_Codex_RB2_5/04_runbook/diagnose_run_status.sql`
- 替换：`{{run_id}}`
- 回传：输出全文（summary + missing_shards）

2) 缺失 shard 的“是哪一步没跑”
- 用上一步结果即可：按缺失模式分类（例如：缺 RB20_01 / 只缺 RB20_03 / 只缺 RB20_11…）
- 回传：每类 shard_id 列表

3) shard_cnt 合同一致性
- SQL：
  ```sql
  SELECT run_id, COUNT(*) AS shard_cnt, MIN(shard_id) AS min_id, MAX(shard_id) AS max_id
  FROM rb20_v2_5.shard_plan
  WHERE run_id='{{run_id}}'
  GROUP BY 1;
  ```
- 回传：一行结果

结论判断：
- 若存在 missing_shards：本 run **不可验收**，优先走 “D. 新 run_id 修复路径”

---

## B. 性能 micro-bench（核心：Step64 与 ShardPlan）

### B1. ShardPlan 生成耗时（建议在低峰）

目的：验证 ShardPlan 不再被 “全量排序 NTILE” 卡住。

- 执行：`Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`
- 参数：`run_id=<NEW_RUN_ID>`，`contract_version=contract_v1`，`shard_cnt=64`，`eps=0.10`
- 记录：开始/结束时间（psql 可用 `\\timing on`）
- 回传：
  - 耗时（秒）
  - `SELECT MIN(est_rows), MAX(est_rows) FROM rb20_v2_5.shard_plan WHERE run_id='<NEW_RUN_ID>';`

验收（经验阈值）：
- 该步骤应主要是一次 scan+group by；若仍 >30min，说明 DB 资源/IO 或统计信息异常，需要进一步排查（VACUUM/统计/并行度）。

### B2. Step64 核心链路 micro-bench（选 1 个 shard）

目的：只用 1 个 shard，评估 “window_headtail_64 + split_events_64” 的速度是否正常。

选 shard 方法（不用扫大表）：
- 用 `rb20_v2_5.shard_plan.est_rows` 选：
  - `small`：est_rows 最小 shard
  - `mid`：est_rows 接近中位数 shard
  - `large`：est_rows 最大 shard
  - 先只跑 `mid`（最快拿到结论）

对选定 shard（例如 `shard_id=X`）执行并计时：
1) RB20_01 → 02 → 03 → 11 → 04 → 04P
2) 记录每步耗时（开始/结束时间）
3) 回传 step_stats 关键指标（用于判断是否退化）：
   ```sql
   SELECT step_id, metric_name, metric_value_numeric
   FROM rb20_v2_5.step_stats
   WHERE run_id='<NEW_RUN_ID>' AND shard_id=X
     AND metric_name IN ('source_members_rows','natural_block_cnt_total','window_rows_cnt','split_events_cnt','cut_cnt','final_profile_block_cnt')
   ORDER BY step_id, metric_name;
   ```

异常判定（需要立刻 STOP 并回传日志）：
- `window_rows_cnt` 或 `split_events_cnt` 为 0 但 `preh_blocks` > 0（说明 RB20_11/RB20_04 没跑完或 SQL 退化）
- 单步耗时显著超过其它 shard（强倾斜或执行计划退化）

---

## C. 优化固化检查点（开发侧）

本仓库已做两处“高收益优化”，执行侧只需验证：

1) ShardPlan：改为 /16 直方图近似分位（避免全量排序）
- 文件：`Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`

2) RB20_11：改为“一次性展开 PreH valid 成员再聚合”，避免 cand×map 的重复 join
- 文件：`Y_IP_Codex_RB2_5/03_sql/RB20_11/11_window_headtail_shard.sql`

回传：
- B1/B2 的耗时与 step_stats 指标（即可判断优化是否生效）

---

## D. 新 run_id 修复路径（建议唯一合规方式）

适用：当前 run 存在 missing_shards / shard_cnt 不一致 / QA 未完整执行。

1) 生成新 run_id（按 README 规则）
2) 先跑全局：RunInit → ShardPlan → AbnormalDedup
3) 再跑 per-shard 第一段（01/02/03/11/04/04P），并强制校验缺失 shard=0 行
4) 再跑全局 H
5) 再跑 per-shard 第二段（06/07/08）
6) 最后跑 QA_Assert（必须替换 `{{shard_cnt}}`），STOP 断言全 PASS 才能进入 release

建议使用（可选）：
- `Y_IP_Codex_RB2_5/04_runbook/orchestrate_rb20_v2.py`（已支持 `SHARD_CNT/CONCURRENCY`，并从 shard_plan 取 shard 列表）
