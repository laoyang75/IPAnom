# RB20 v2.0 — 执行 Agent Runbook（单文件、可并发、失败即停）

唯一主版本：`/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/重构2.md`

本 Runbook 目标：
- 你（执行 agent）不需要打开任何其他文档；只需要按本文件顺序执行指定 SQL 文件（绝对路径）。
- 并发策略：PG 内置并行不可控，因此用“外部脚本/外部 orchestrator”启动 **≤32 个并发会话** 来跑 per-shard 流水线；同一个批次必须使用 **同一个 `run_id`**（不要拆多个 run_id）。
- 任何 SQL 报错或校验失败：**立刻 STOP**，把 `run_id + shard_id(如有) + step_id + 报错信息/日志` 发给负责人（我）修复；不要自行重试多次。

---

## 0) 输入锚点（必须写死；只读）

输入 schema：`public`

W 源表（唯一成员母体）：
- `public."ip库构建项目_ip源表_20250811_20250824_v2_1"`
- 成员主键：`ip_long`
- 中国过滤：脚本内固定谓词 `w."IP归属国家" IN ('中国')`（DP-001 已确认）

A 异常表（仅用于异常标记，不删除）：
- `public."ip库构建项目_异常ip表_20250811_20250824_v2"`
- 字段：`ipv4_bigint`（NULL 忽略；RB20_01A 会去重写入 `rb20_v2_5.abnormal_dedup`）

（可选只读快速确认：执行成功即可；失败即 STOP）
```sql
SELECT COUNT(*) AS w_rows FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1";
SELECT COUNT(*) AS a_rows FROM public."ip库构建项目_异常ip表_20250811_20250824_v2";
```

---

## 0) 你需要先确定的常量

### 0.1 contract_version（固定）

- `contract_version = contract_v1`

### 0.2 run_id（必须按规则生成且全程一致）

规则（Asia/Singapore 时间）：
- `rb20v2_{YYYYMMDD}_{HHMMSS}_sg_{seq3}`
  - `seq3` 建议从 `001` 开始（当日递增）

例子（你执行时应替换为“当前时间”的值）：
- `run_id = rb20v2_20260107_200000_sg_001`

**强约束（不要做）**：
- 不要为了并发把一个批次拆成多个 `run_id`（会导致 `RB20_05` / `RB20_99` 无法作为单批次验收锚点）。

### 0.3 shard_cnt（必须在跑 ShardPlan 前写死）

- `shard_cnt`：本次 run 的分片数量
  - 默认：`64`
  - 若遇到严重倾斜导致 64 分片无法完成：必须先走 DP-014 并在合同/Runbook 中明确 `shard_cnt`（禁止执行中途改 shard_cnt 或改 shard_id 约束“补丁继续跑”）。

---

## 1) 执行方式（外部并发 ≤32）

你需要用你自己的方式（脚本/调度器/任务系统均可）实现：
- 最多 32 个并发进程/会话
- 每个会话绑定一个 `shard_id`（来自 `rb20_v2_5.shard_plan`；范围 `0..(shard_cnt-1)`）
- 每个 shard 会话严格按“RB20_01→02→03→11→04→04P”的顺序串行执行（同一个 `run_id`）
- 分批完成全部 `shard_cnt` 个 shard（队列/动态调度更佳）

如果你能记录日志，请至少记录：
- `run_id`、`shard_id`、`step_id`、开始/结束时间、SQL 文件路径、失败的错误消息

---

## 2) 预检查（只需做一次；顺序不可变）

### 2.1 确认输出 schema 与关键表存在（只读检查）

执行以下 SQL（直接复制到你的 SQL 执行器即可）：

```sql
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name='rb20_v2_5';
```

若无返回：STOP（说明 DDL 未落地）。

### 2.2 （可选）若你不确定 DDL/索引是否已跑过：补跑一次（幂等）

- DDL（幂等）：`/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/00_contracts/01_ddl_rb20_v2_full.sql`
- 索引（幂等）：`/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/00_contracts/02_indexes_rb20_v2.sql`

### 2.3 更新宽字段视图（必须做一次；幂等）

- 视图（幂等，DROP+CREATE）：`/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/00_contracts/03_views_rb20_v2.sql`

若视图执行报错：STOP。

---

## 3) 全局步骤（只跑一次；全部使用同一个 run_id）

### 3.1 RB20_00A：Run Init + Config（只跑一次）

SQL 文件：
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/00_contracts/00_run_init.sql`

执行前必须做文本替换：
- 把文件中的 `{{run_id}}` 全部替换成你的 `run_id`
- 把文件中的 `{{contract_version}}` 全部替换成 `contract_v1`

执行完成后检查（复制执行）：
```sql
SELECT run_id, contract_version, status
FROM rb20_v2_5.run_meta
WHERE run_id='<YOUR_RUN_ID>';
```
若无返回：STOP。

### 3.2 RB20_00D：ShardPlan（SQL-only，重任务，只跑一次）

SQL 文件：
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`

执行前必须做文本替换：
- `{{run_id}}` → 你的 `run_id`
- `{{contract_version}}` → `contract_v1`
- `{{shard_cnt}}` → 你的 `shard_cnt`（默认 64）
- `{{eps}}` → `0.10`

说明（避免你再次踩坑）：
- 本 ShardPlan 已改为 **/16（`ip_long>>16`）直方图的近似分位切分**，避免“全量排序 NTILE”与“按全 IPv4 min/max 等分导致空 shard”两类风险。
- 输出为 `shard_cnt` 个不重叠的 `ip_long` 区间，供后续 per-shard 脚本按范围拉取成员。

执行完成后检查（失败即 STOP）：
```sql
SELECT COUNT(*) AS shard_cnt
FROM rb20_v2_5.shard_plan
WHERE run_id='<YOUR_RUN_ID>';

SELECT min(shard_id) AS min_shard, max(shard_id) AS max_shard
FROM rb20_v2_5.shard_plan
WHERE run_id='<YOUR_RUN_ID>';

SELECT COUNT(*) AS empty_shard_cnt
FROM rb20_v2_5.shard_plan
WHERE run_id='<YOUR_RUN_ID>' AND COALESCE(est_rows,0)=0;
```
期望：`shard_cnt=<YOUR_SHARD_CNT>` 且 `min_shard=0 max_shard=<YOUR_SHARD_CNT-1>` 且 `empty_shard_cnt=0`。

### 3.3 RB20_01A：异常去重（全局，只跑一次）

SQL 文件：
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_01/01A_abnormal_dedup.sql`

执行前必须做文本替换：
- `{{run_id}}` → 你的 `run_id`
- `{{contract_version}}` → `contract_v1`

执行完成后检查（注意：全局 StepStats 用 `shard_id=-1`）：
```sql
SELECT metric_value_numeric AS abnormal_dedup_rows
FROM rb20_v2_5.step_stats
WHERE run_id='<YOUR_RUN_ID>' AND step_id='RB20_01A' AND shard_id=-1 AND metric_name='abnormal_dedup_rows';
```
若无返回：STOP。

---

## 4) Per-shard 第一段流水线（并发 ≤32；每个 shard 串行 6 步）

对每个 `shard_id`（从 ShardPlan 取列表），按顺序执行以下 6 个 SQL 文件（每个文件都要替换 `{{run_id}}/{{contract_version}}/{{shard_id}}`）：

1) RB20_01（Source Members）
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_01/01_source_members_shard.sql`

2) RB20_02（Natural Blocks）
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_02/02_natural_blocks_shard.sql`

3) RB20_03（Pre Profile + PreH + Keep/Drop Members）
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_03/03_pre_profile_shard.sql`

4) RB20_11（HeadTail Window）
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_11/11_window_headtail_shard.sql`

5) RB20_04（SplitEvents + Final Blocks + Final Map）
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_04/04_split_and_final_blocks_shard.sql`

6) RB20_04P（Final Profile）
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_04P/04P_final_profile_shard.sql`

### 4.1 本段完成校验（必须做；失败即 STOP）

执行以下 SQL，找出“缺失的 shard”：

```sql
-- Source Members missing shards
SELECT sp.shard_id
FROM rb20_v2_5.shard_plan sp
WHERE sp.run_id='<YOUR_RUN_ID>'
  AND NOT EXISTS (
    SELECT 1 FROM rb20_v2_5.source_members sm
    WHERE sm.run_id=sp.run_id AND sm.shard_id=sp.shard_id
  )
ORDER BY 1;

-- Final Profile missing shards
SELECT sp.shard_id
FROM rb20_v2_5.shard_plan sp
WHERE sp.run_id='<YOUR_RUN_ID>'
  AND NOT EXISTS (
    SELECT 1 FROM rb20_v2_5.profile_final pf
    WHERE pf.run_id=sp.run_id AND pf.shard_id=sp.shard_id
  )
ORDER BY 1;
```

期望：两段查询均返回 0 行。若有返回：STOP，并把缺失 shard_id 列表发回负责人。

---

## 5) 全局 H（只跑一次；必须在 4) 全部 shard 完成后）

### 5.1 RB20_05：H Blocks + H Members

SQL 文件：
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_05/05_h_blocks_and_members.sql`

执行前必须替换：
- `{{run_id}}` → 你的 `run_id`
- `{{contract_version}}` → `contract_v1`
- `{{shard_cnt}}` → 你的 `shard_cnt`（默认 64；或按 DP-014 选定）

执行完成后检查：
```sql
SELECT metric_name, metric_value_numeric
FROM rb20_v2_5.core_numbers
WHERE run_id='<YOUR_RUN_ID>' AND metric_name IN ('h_block_cnt','h_member_cnt')
ORDER BY metric_name;
```
若无返回：STOP。

---

## 6) Per-shard 第二段流水线（并发 ≤32；每个 shard 串行 3 步）

必须在 5) 完成后再启动。

对每个 `shard_id`（从 ShardPlan 取列表），按顺序执行：

1) RB20_06（R1 = KeepMembers \\ H_cov）
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_06/06_r1_members_shard.sql`

2) RB20_07（E Atoms + E Runs + E Members）
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_07/07_e_atoms_runs_members_shard.sql`

3) RB20_08（F Members = R1 \\ E_cov；等值 anti-join，禁止 BETWEEN）
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_08/08_f_members_shard.sql`

### 6.1 本段完成校验（必须做；失败即 STOP）

```sql
-- E Members missing shards
SELECT sp.shard_id
FROM rb20_v2_5.shard_plan sp
WHERE sp.run_id='<YOUR_RUN_ID>'
  AND NOT EXISTS (
    SELECT 1 FROM rb20_v2_5.e_members em
    WHERE em.run_id=sp.run_id AND em.shard_id=sp.shard_id
  )
ORDER BY 1;

-- F Members missing shards
SELECT sp.shard_id
FROM rb20_v2_5.shard_plan sp
WHERE sp.run_id='<YOUR_RUN_ID>'
  AND NOT EXISTS (
    SELECT 1 FROM rb20_v2_5.f_members fm
    WHERE fm.run_id=sp.run_id AND fm.shard_id=sp.shard_id
  )
ORDER BY 1;
```
期望：两段查询均返回 0 行。否则 STOP。

---

## 7) 终验收（只跑一次；必须在 6) 全部 shard 完成后）

### 7.1 RB20_99：QA_Assert（severity=STOP）

SQL 文件：
- `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_99/99_qa_assert.sql`

执行前必须替换：
- `{{run_id}}` → 你的 `run_id`
- `{{contract_version}}` → `contract_v1`

执行完成后立即检查（返回 0 行才算 PASS）：
```sql
SELECT *
FROM rb20_v2_5.qa_assert
WHERE run_id='<YOUR_RUN_ID>' AND NOT pass_flag
ORDER BY assert_name;
```

若返回非 0 行：STOP，直接把 `run_id` 发回负责人（我）修复，不要自行修改口径或重跑全量。

---

## 8) 你执行完成后要交付给负责人（我）检查的内容

你只需要发我：
- `run_id`
- 若过程中有失败：失败的 `shard_id`（如有）、失败的 `step_id`、完整报错信息/日志片段

我会用以下只读查询做最终检查（你不用执行）：
- `rb20_v2_5.qa_assert` 全 PASS
- `rb20_v2_5.step_stats` 覆盖 shard `0..(shard_cnt-1)` + 全局 shard_id=-1
- H/E/F 覆盖与守恒一致

---

## 9) 已完成的小范围 smoke（你无需重复）

负责人已做过一次单 shard 小范围 smoke（用于验证 SQL 可执行与断言链路可跑通）：
- 报告：`/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/06_reports/03_smoke_test_rb20v2_20260107_183400_sg_990.md`
