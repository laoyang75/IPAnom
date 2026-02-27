# RB20 v2.0（rb20_v2_5）性能压测 Runbook（给执行 agent 跑报告）

目标：在**不全量重跑**的前提下，用“更大的样本量 + 多个 shard 档位 + 可复现报告”验证是否会卡死，并定位卡点发生在哪一步/哪些 shard。

输出：执行 agent 跑完后，回传一份 Markdown 报告（按本文件第 6 节模板），并附上关键 SQL 输出/耗时。

---

## 0) 前置条件

- 数据库：`ip_loc2`
- 源表（只读）：
  - `public."ip库构建项目_ip源表_20250811_20250824_v2_1"`
  - `public."ip库构建项目_异常ip表_20250811_20250824_v2"`
- 输出 schema：`rb20_v2_5`
- 你需要能运行 `psql`

建议在低峰执行（至少 Phase A 的 ShardPlan 会扫全量中国成员）。

---

## 1) 设定本次试跑参数（必须写死）

1) 选择 `run_id`
- 格式：`rb20v2_{YYYYMMDD}_{HHMMSS}_sg_{seq3}`
- 例：`rb20v2_20260128_210000_sg_001`

2) 选择 `shard_cnt`
- 首次建议：`64`

3) 选择并发策略（本次试跑强制更保守）
- 本 runbook 只做**少量 shard**，全部**串行**跑（避免并发把 IO 打满导致误判）。
- 全量跑之前再把并发提升到 `16~32`。

---

## 2) Phase 0：建表/索引/视图（幂等，可跳过但建议跑一次）

```bash
export PGHOST=192.168.200.217 PGPORT=5432 PGUSER=postgres PGPASSWORD=123456 PGDATABASE=ip_loc2 PGCLIENTENCODING=UTF8
psql -v ON_ERROR_STOP=1 -f Y_IP_Codex_RB2_5/03_sql/00_contracts/01_ddl_rb20_v2_full.sql
psql -v ON_ERROR_STOP=1 -f Y_IP_Codex_RB2_5/03_sql/00_contracts/02_indexes_rb20_v2.sql
psql -v ON_ERROR_STOP=1 -f Y_IP_Codex_RB2_5/03_sql/00_contracts/03_views_rb20_v2.sql
```

---

## 3) Phase A：全局初始化（必须）

### A1. Run Init + Config

把 `Y_IP_Codex_RB2_5/03_sql/00_contracts/00_run_init.sql` 里的：
- `{{run_id}}` 替换为你的 `run_id`
- `{{contract_version}}` 替换为 `contract_v1`

执行：
```bash
psql -v ON_ERROR_STOP=1 -f /tmp/rb20_v2_5_run_init.sql
```

### A2. ShardPlan（重任务，必须计时）

把 `Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql` 里的：
- `{{run_id}}`、`{{contract_version}}`、`{{shard_cnt}}`、`{{eps}}` 替换好（`eps=0.10` 即可）

> [!IMPORTANT]
> 禁止人工写 `rb20_v2_5.shard_plan`（例如“按 min/max 数值等分”），那会掩盖真实倾斜，后续必卡；必须使用本仓库脚本生成。

执行并计时：
```bash
psql -v ON_ERROR_STOP=1 -c "\\timing on" -f /tmp/rb20_v2_5_shard_plan.sql
```

执行后回传（直接复制输出即可）：
```sql
SELECT COUNT(*) AS shard_cnt, MIN(shard_id) AS min_id, MAX(shard_id) AS max_id,
       MIN(est_rows) AS min_est, MAX(est_rows) AS max_est
FROM rb20_v2_5.shard_plan
WHERE run_id='{{run_id}}';
```

ShardPlan 强校验（若任一项不符合，直接停，换新 `run_id` 重新跑 Phase A）：
```sql
-- 1) shard_id 必须连续覆盖 0..shard_cnt-1（本次 shard_cnt=64）
WITH expect AS (
  SELECT gs::int AS shard_id
  FROM generate_series(0, 63) gs
),
got AS (
  SELECT shard_id::int AS shard_id
  FROM rb20_v2_5.shard_plan
  WHERE run_id='{{run_id}}'
)
SELECT COUNT(*) AS missing_cnt
FROM expect e
LEFT JOIN got g USING (shard_id)
WHERE g.shard_id IS NULL;

-- 2) 若“所有 shard 的 range 宽度完全相同且 est_rows 完全相同”，高度可疑（通常意味着按数值等分）
SELECT
  COUNT(DISTINCT (ip_long_end - ip_long_start)) AS distinct_range_width_cnt,
  MIN(est_rows) AS min_est, MAX(est_rows) AS max_est
FROM rb20_v2_5.shard_plan
WHERE run_id='{{run_id}}';
```

### A3. Abnormal Dedup（全局）

把 `Y_IP_Codex_RB2_5/03_sql/RB20_01/01A_abnormal_dedup.sql` 里的 `{{run_id}}/{{contract_version}}` 替换好并执行。

---

## 4) Phase B：挑选 shard（覆盖小/中/大/极端）

执行以下 SQL 得到 5 个 shard_id（回传这 5 个 id）。如果你担心仍不够覆盖，可自行扩展为 9 个：`min/p05/p10/p25/p50/p75/p90/p95/max`。

```sql
WITH sp AS (
  SELECT shard_id::int, est_rows::bigint
  FROM rb20_v2_5.shard_plan
  WHERE run_id='{{run_id}}'
),
ranked AS (
  SELECT
    shard_id,
    est_rows,
    row_number() OVER (ORDER BY est_rows ASC, shard_id) AS rn,
    count(*) OVER () AS n
  FROM sp
)
SELECT 'min' AS tag, shard_id, est_rows FROM ranked WHERE rn=1
UNION ALL
SELECT 'p10', shard_id, est_rows FROM ranked WHERE rn=GREATEST(1, (n*10)/100)
UNION ALL
SELECT 'p50', shard_id, est_rows FROM ranked WHERE rn=GREATEST(1, (n*50)/100)
UNION ALL
SELECT 'p90', shard_id, est_rows FROM ranked WHERE rn=GREATEST(1, (n*90)/100)
UNION ALL
SELECT 'max', shard_id, est_rows FROM ranked WHERE rn=n
ORDER BY tag;
```

---

## 5) Phase C：对这 5 个 shard 串行跑 01→04P（必须计时）

对每个 shard（按 `min→p10→p50→p90→max`），依次执行：
1) `RB20_01`：`Y_IP_Codex_RB2_5/03_sql/RB20_01/01_source_members_shard.sql`
2) `RB20_02`：`Y_IP_Codex_RB2_5/03_sql/RB20_02/02_natural_blocks_shard.sql`
3) `RB20_03`：`Y_IP_Codex_RB2_5/03_sql/RB20_03/03_pre_profile_shard.sql`
4) `RB20_11`：`Y_IP_Codex_RB2_5/03_sql/RB20_11/11_window_headtail_shard.sql`
5) `RB20_04`：`Y_IP_Codex_RB2_5/03_sql/RB20_04/04_split_and_final_blocks_shard.sql`
6) `RB20_04P`：`Y_IP_Codex_RB2_5/03_sql/RB20_04P/04P_final_profile_shard.sql`

要求：
- 每步都替换 `{{run_id}}/{{contract_version}}/{{shard_id}}`
- 每步都记录耗时（`psql` 的 `\\timing on` 或用 `time psql ...`）

每个 shard 跑完后，执行并回传下面这段（用于确认没有“跑了但空表”）：
```sql
WITH p AS (SELECT '{{run_id}}'::text AS run_id, {{shard_id}}::smallint AS shard_id)
SELECT
  (SELECT MAX(metric_value_numeric) FROM rb20_v2_5.step_stats s, p WHERE s.run_id=p.run_id AND s.shard_id=p.shard_id AND s.step_id='RB20_01' AND s.metric_name='source_members_rows') AS source_rows,
  (SELECT MAX(metric_value_numeric) FROM rb20_v2_5.step_stats s, p WHERE s.run_id=p.run_id AND s.shard_id=p.shard_id AND s.step_id='RB20_02' AND s.metric_name='natural_block_cnt_total') AS natural_blocks,
  (SELECT MAX(metric_value_numeric) FROM rb20_v2_5.step_stats s, p WHERE s.run_id=p.run_id AND s.shard_id=p.shard_id AND s.step_id='RB20_11' AND s.metric_name='window_rows_cnt') AS window_rows,
  (SELECT MAX(metric_value_numeric) FROM rb20_v2_5.step_stats s, p WHERE s.run_id=p.run_id AND s.shard_id=p.shard_id AND s.step_id='RB20_04' AND s.metric_name='split_events_cnt') AS split_events,
  (SELECT MAX(metric_value_numeric) FROM rb20_v2_5.step_stats s, p WHERE s.run_id=p.run_id AND s.shard_id=p.shard_id AND s.step_id='RB20_04' AND s.metric_name='cut_cnt') AS cut_cnt,
  (SELECT MAX(metric_value_numeric) FROM rb20_v2_5.step_stats s, p WHERE s.run_id=p.run_id AND s.shard_id=p.shard_id AND s.step_id='RB20_04P' AND s.metric_name='final_profile_block_cnt') AS final_profile_blocks;
```

---

## 6) Phase D：生成汇总报告（必须）

执行：
```bash
psql -v ON_ERROR_STOP=1 -f Y_IP_Codex_RB2_5/04_runbook/collect_perf_report.sql -v run_id='{{run_id}}'
```

然后把输出内容粘贴进你的报告里。

---

## 7) 回传报告模板（请直接按此格式回传）

标题：RB20_v2_5_perf_trial_{run_id}.md

必填内容：
1) run_id / shard_cnt / 执行时间窗口
2) Phase A：ShardPlan 耗时 + shard_plan 统计结果
3) Phase C：5 个 shard 的每步耗时表（01/02/03/11/04/04P）
4) Phase C：每个 shard 的关键 step_stats 汇总（source_rows/natural_blocks/window_rows/split_events/cut_cnt/final_profile_blocks）
5) Phase D：`Y_IP_Codex_RB2_5/04_runbook/collect_perf_report.sql` 输出全文
6) 你观察到的“是否卡死/是否异常慢”的结论 + 你认为最慢的一步是哪一步
