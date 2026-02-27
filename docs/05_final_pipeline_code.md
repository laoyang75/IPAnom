# 最终 Pipeline 代码说明与执行指南

> 本文档描述最终成功完成 RB20 v2.5 全链路 Pipeline 的代码版本及其执行方式。

## 1. 最终成功运行信息

| 项目 | 值 |
|------|-----|
| Run ID | `rb20v2_20260202_191900_sg_001` |
| Shard 数 | 65 (shard_id: 0~64) |
| 源成员数 | 59,719,461 |
| H 成员 | 13,024,618 (21.8%) |
| E 成员 | 45,008,754 (75.3%) |
| F 成员 | 1,733,614 (2.9%) |
| QA 断言 | 11/11 ALL PASS |
| 守恒 | Keep = H + E + F ✅ 差值 = 0 |

---

## 2. 核心编排脚本

最终版本使用 **3 个 Python 编排脚本** + **13 个 SQL 脚本** 完成全链路。

### 2.1 主编排脚本

**文件**: [`orchestrate_fresh_start_v2.py`](file:///Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/04_runbook/orchestrate_fresh_start_v2.py) (331 行)

8 个阶段顺序执行：

| 阶段 | 类型 | 并发 | 内容 | 调用方式 |
|------|------|------|------|---------|
| Phase 0 | 全局 | 1 | 清理旧数据 | `run_cleanup.py` |
| Phase 1 | 全局 | 1 | Run Init + ShardPlan + 异常去重 | psql 执行 SQL |
| Phase 2 | per-shard | 32 | Step01(源成员) + Step02(自然块) | Pool(32).map |
| Phase 3 | 全局 | 8 | Step03(预画像) **优化版** | 子进程调用 `orchestrate_step03_bucket_full.py` |
| Phase 4 | 全局 | 8 | Step11(HeadTail) **优化版** | 子进程调用 `orchestrate_step11_chunked.py` |
| Phase 5 | per-shard | 32 | Step04(切分) + Step04P(最终画像) | Pool(32).map |
| Phase 6 | 全局 | 1 | Step05(H 库) | psql 执行 SQL |
| Phase 7 | per-shard | 32 | Step06(R1) + Step07(E) + Step08(F) | Pool(32).map |
| Phase 8 | 全局 | 1 | Step99(QA 终验收) | psql 执行 SQL |

### 2.2 Step03 优化编排

**文件**: [`orchestrate_step03_bucket_full.py`](file:///Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/04_runbook/orchestrate_step03_bucket_full.py) (433 行)

**三阶段架构** (这是解决长时间卡死的关键):

```
Phase A: 计划生成
  ├─ 构建 source_members_slim (UNLOGGED, 15列, 减少读放大)
  ├─ 统计 block_sizes (每 shard 每 block 的成员数)
  └─ 生成 step03_block_bucket + step03_task_plan (width_bucket 分桶)

Phase B: 分桶执行 (Pool(8))
  ├─ 每个 worker 处理 1 个 (shard_id, bucket_id)
  ├─ JOIN: block_bucket → map → slim (3表 JOIN)
  ├─ 聚合: CTE m → agg → score → tier
  └─ 写入 staging: profile_pre_stage (UNLOGGED, 无索引)

Phase C: 收敛落表
  ├─ DELETE profile_pre WHERE run_id
  ├─ INSERT INTO profile_pre SELECT * FROM profile_pre_stage
  └─ 记录 StepStats
```

**关键参数**:
- `TARGET_ROWS_PER_BUCKET = 200,000` (每个任务处理 20 万行)
- `CONCURRENCY = 8` (8 并发，非 32)
- `work_mem = '256MB'`
- `enable_nestloop = off` (强制 Hash Join)
- `jit = off` (关闭 JIT)

### 2.3 Step11 分块编排

**文件**: [`orchestrate_step11_chunked.py`](file:///Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/04_runbook/orchestrate_step11_chunked.py) (245 行)

**核心优化**: 将 `preh_blocks` 按 500 块为一组分批处理，避免单次 JOIN 过大。

- `BLOCK_CHUNK_SIZE = 500`
- `CONCURRENCY = 8`
- slim 表增加 `operator` 列 (Step11 需要运营商判定)

---

## 3. SQL 脚本清单

| 步骤 | SQL 路径 | 核心操作 |
|------|----------|---------|
| 00 Run Init | `03_sql/00_contracts/00_run_init.sql` | run_meta + 清理 |
| 00D ShardPlan | `03_sql/00_contracts/10_shard_plan_generate_sql_only.sql` | NTILE 分位切分 |
| 01A Dedup | `03_sql/RB20_01/01A_abnormal_dedup.sql` | 异常去重 |
| 01 Source | `03_sql/RB20_01/01_source_members_shard.sql` | 中国过滤 + 异常标记 |
| 02 Natural | `03_sql/RB20_02/02_natural_blocks_shard.sql` | 连续区间识别 |
| 03 Profile | (Python 内嵌 SQL) | bucket 化预画像 |
| 03 Post | `03_sql/RB20_03/03_post_process.sql` | PreH 候选生成 |
| 11 Window | (Python 内嵌 SQL) | 分块 HeadTail |
| 04 Split | `03_sql/RB20_04/04_split_and_final_blocks_shard.sql` | 切分 + 最终块 |
| 04P Profile | `03_sql/RB20_04P/04P_final_profile_shard.sql` | 最终画像 |
| 05 H | `03_sql/RB20_05/05_h_blocks_and_members.sql` | H 块 + H 成员 |
| 06 R1 | `03_sql/RB20_06/06_r1_members_shard.sql` | 残余集 |
| 07 E | `03_sql/RB20_07/07_e_atoms_runs_members_shard.sql` | 原子/run/E 成员 |
| 08 F | `03_sql/RB20_08/08_f_members_shard.sql` | F 成员 |
| 99 QA | `03_sql/RB20_99/99_qa_assert.sql` | 11 条 STOP 断言 |

---

## 4. 执行命令

```bash
cd Y_IP_Codex_RB2_5/04_runbook

# 一键全链路执行（约 6~14 小时）
python3 orchestrate_fresh_start_v2.py

# 如果只需重跑 Step03（最常卡死的步骤）
python3 orchestrate_step03_bucket_full.py

# 如果只需重跑 Step11
python3 orchestrate_step11_chunked.py
```

> ⚠️ 执行前需确认 `BASE_DIR` 和 `RUNBOOK_DIR` 路径正确指向当前环境。

---

## 5. 曾遇到的问题与修复历史

从 CHANGELOG.md 和实际执行经验中总结：

| # | 问题 | 原因 | 修复 |
|---|------|------|------|
| 1 | Step03 hang >14h | 64 shard 32 并发大聚合 + 宽表读放大 | bucket 三阶段 + slim 表 + 降并发到 8 |
| 2 | Step11 超时 | generate_series 按点扫描爆炸 | 改为 bucket_set 集合方式生成候选切点 |
| 3 | 后续步骤聚合莫名变慢 | Step02 残留 `enable_hashagg=off` 污染会话 | 移除 Step02 设置 + Step03 显式 `SET on` |
| 4 | ShardPlan 空 shard | 全 IPv4 等分在稀疏区产生空 shard | 改为 NTILE 分位切分 |
| 5 | RB20_07 E 成员超时 | BETWEEN 范围 JOIN 膨胀 | 改为 atom_to_run 等值 join |
| 6 | ShardPlan 倾斜超大 /16 | 单个 /16 占比过大 | 自适应细分大 /16 为 /24 |
