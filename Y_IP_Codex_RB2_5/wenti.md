
## 0. 目标与现状（为什么要重构）

现状 Step03 的特征是：

* **64 shard，32 并发**跑每 shard 一条“宽表 JOIN + 大 GROUP BY”聚合，出现 **>14 小时 hang**。
* JOIN 逻辑：`map_member_block_natural` ↔ `source_members` 按 `(run_id, shard_id, ip_long)`。
* `source_members` 是 **100+ 列宽表**，聚合列多，核心瓶颈是 CTE `m` + `agg` 的大聚合。

你提出的关键点“**分片极度不均匀**导致少数 shard 成为长尾瓶颈”——这在现实 IP 分布下非常常见。因此这套新方案的第一原则是：

> **不要假设 shard 天然均匀。必须先评估，再生成“均衡任务切片计划”，再执行。**

---

## 1) 新方案总览：三阶段架构（评估→执行→收敛）

### 阶段 A：切片评估与计划生成（agent 做）

对当前 `run_id` 做统计，计算每个 shard 的“工作量”，并进一步在 shard 内按 `block_id_natural` 做二级切片（bucket），生成一个**任务计划表**，确保每个任务的行数/成本接近目标值，从而避免倾斜。

### 阶段 B：按计划执行（worker pool）

不再是 64 个 shard 任务，而是 **N 个“(shard_id, bucket_id)”任务**。worker 按计划调度，先跑最重任务，保证整体无长尾。

### 阶段 C：收敛/落表（一次性）

用 staging（UNLOGGED/无索引）吸收写入，再统一建索引/ANALYZE 或 merge 到正式表，显著降低并发插入索引争用与 WAL 压力。

---

## 2) 阶段 A：切片评估统计 + 生成“均衡任务计划”

### 2.1 必做统计（3 张“判断倾斜”的报表）

> agent 先跑这些 SQL，把结果带回来（top/bottom 就够），用于生成 plan。

**(A) shard 行数分布（决定 shard 是否倾斜）**

```sql
SELECT shard_id, COUNT(*) AS member_rows
FROM rb20_v2_5.map_member_block_natural
WHERE run_id='{{run_id}}'
GROUP BY shard_id
ORDER BY member_rows DESC;
```

**(B) shard 内 block 大小分布（决定 bucket 怎么切）**

```sql
SELECT shard_id, block_id_natural, COUNT(*) AS cnt
FROM rb20_v2_5.map_member_block_natural
WHERE run_id='{{run_id}}'
GROUP BY shard_id, block_id_natural;
```

**(C) 超级大 block 检测（决定是否需要“二段聚合”兜底）**

```sql
WITH block_sizes AS (
  SELECT shard_id, block_id_natural, COUNT(*) AS cnt
  FROM rb20_v2_5.map_member_block_natural
  WHERE run_id='{{run_id}}'
  GROUP BY shard_id, block_id_natural
)
SELECT shard_id,
       MAX(cnt) AS max_block_members,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY cnt) AS p99_block_members
FROM block_sizes
GROUP BY shard_id
ORDER BY max_block_members DESC;
```

> 经验阈值（用于自动策略）：
>
> * `max(shard_rows)/avg(shard_rows) > 5`：明显倾斜，必须二级切片
> * `max_block_members` 远大于 p99（例如 >10 倍）：存在超级 block，需要“兜底策略”（见 2.4）

---

### 2.2 计划表设计（数据库里落两张表）

这两张表是新方案的核心契约：**什么任务跑什么 block**。

```sql
-- 任务计划：每个 (shard_id, bucket_id) 是一个可执行单元
CREATE TABLE IF NOT EXISTS rb20_v2_5.step03_task_plan (
  run_id           text     NOT NULL,
  shard_id         smallint NOT NULL,
  bucket_id        int      NOT NULL,
  est_member_rows  bigint   NOT NULL,  -- 估算：bucket 内成员行数（map 行数）
  est_block_cnt    int      NOT NULL,
  status           text     NOT NULL DEFAULT 'PENDING',
  started_at       timestamptz,
  finished_at      timestamptz,
  PRIMARY KEY (run_id, shard_id, bucket_id)
);

-- block -> bucket 映射：保证一个 block 只属于一个 bucket
CREATE TABLE IF NOT EXISTS rb20_v2_5.step03_block_bucket (
  run_id          text     NOT NULL,
  shard_id        smallint NOT NULL,
  block_id_natural bigint  NOT NULL,
  bucket_id       int      NOT NULL,
  est_members     bigint,
  PRIMARY KEY (run_id, shard_id, block_id_natural)
);

CREATE INDEX IF NOT EXISTS idx_step03_bb_run_shard_bucket
ON rb20_v2_5.step03_block_bucket(run_id, shard_id, bucket_id, block_id_natural);
```

---

### 2.3 生成 bucket 的算法（推荐 agent 用 LPT，SQL-only 作为备选）

#### 推荐（更均衡）：LPT/贪心装箱（agent 实现）

**目标**：让每个 bucket 的 `sum(cnt)` 尽量接近 `target_rows_per_task`，从而任务耗时相近。

* 设定 `target_rows_per_task`：
  推荐：`total_map_rows / (worker_concurrency * 3~6)`
  解释：让任务数是并发的 3~6 倍，便于调度消除长尾。

* 对每个 shard：

  * 计算 `bucket_n = ceil(shard_rows / target_rows_per_task)`（至少 1）
  * 取 `block_sizes`（block_id_natural, cnt）按 cnt DESC
  * 逐个 block 放入“当前总量最小的 bucket”（LPT）

**agent 输出：**

* 写入 `step03_block_bucket`
* 汇总写入 `step03_task_plan`

#### SQL-only 备选（简单、均衡度略差，但可用）：按累计和切分

```sql
-- 你先由 agent 算好一个 target_rows_per_task 常量：{{target_rows}}
WITH block_sizes AS (
  SELECT
    '{{run_id}}'::text AS run_id,
    shard_id,
    block_id_natural,
    COUNT(*) AS cnt
  FROM rb20_v2_5.map_member_block_natural
  WHERE run_id='{{run_id}}'
  GROUP BY shard_id, block_id_natural
),
shard_totals AS (
  SELECT shard_id, SUM(cnt) AS total_cnt
  FROM block_sizes
  GROUP BY shard_id
),
params AS (
  SELECT
    shard_id,
    total_cnt,
    GREATEST(1, CEIL(total_cnt::numeric / {{target_rows}}))::int AS bucket_n
  FROM shard_totals
),
ordered AS (
  SELECT
    b.*,
    p.bucket_n,
    p.total_cnt,
    SUM(cnt) OVER (PARTITION BY b.shard_id ORDER BY cnt DESC, block_id_natural) AS cume_cnt
  FROM block_sizes b
  JOIN params p USING (shard_id)
)
INSERT INTO rb20_v2_5.step03_block_bucket(run_id, shard_id, block_id_natural, bucket_id, est_members)
SELECT
  run_id,
  shard_id,
  block_id_natural,
  1 + FLOOR((cume_cnt - 1) * bucket_n::numeric / total_cnt)::int AS bucket_id,
  cnt
FROM ordered;
```

---

### 2.4 “超级大 block”兜底策略（可选但强烈建议写进方案）

如果出现**单个 block 大到超过 target_rows_per_task 很多倍**，即便 bucket 均衡也没法消除长尾，因为一个 block 不能拆给多个 bucket（否则 `GROUP BY block_id_natural` 结果会被分裂）。

兜底方法是做 **二段聚合（map-reduce 思路）**：

* 第一段：按 `(block_id_natural, sub_part)` 做部分聚合（sub_part 可以按 `ip_long % 16` 或 hash）
* 第二段：对第一段结果再 `GROUP BY block_id_natural` 汇总求和（sum/count 可加和）

这个改动稍大，但能“从理论上”解决单 block 超级大导致的长尾。

---

## 3) 阶段 B：执行层重构（按 bucket 跑，不再按 shard 跑）

### 3.1 输入表的关键索引（必须补齐，否则 bucket 过滤会退化为扫全表）

Bucket 执行时你会先拿 block 列表，再去 map 表拉成员，再 join 宽表。为了让路径变成“按索引点查”，建议增加：

```sql
-- 让 “block -> members(ip_long)” 变成高效索引访问
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_map_run_shard_block
ON rb20_v2_5.map_member_block_natural (run_id, shard_id, block_id_natural)
INCLUDE (ip_long);

-- 维持原 join 方向也建议存在（如已存在可忽略）
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_map_run_shard_ip
ON rb20_v2_5.map_member_block_natural (run_id, shard_id, ip_long);

-- source_members（或 slim 表）的 join 索引
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sm_run_shard_ip
ON rb20_v2_5.source_members (run_id, shard_id, ip_long);
```

> 解释：bucket 过滤天然是按 block 走的；没有 `(run_id, shard_id, block_id_natural)` 这个索引，你很可能又回到“大扫 shard”的老路。

---

### 3.2 强烈建议：先做“瘦身表”减少 I/O（对 SSD 也很有效）

因为 Step03 实际只用十几列，但 `source_members` 是 100+ 列宽表。
推荐为每个 run 构建 slim（可 UNLOGGED）：

```sql
CREATE UNLOGGED TABLE rb20_v2_5.source_members_slim AS
SELECT
  run_id, shard_id, ip_long,
  is_valid,
  "设备数量",
  "上报次数",
  "移动网络设备数量",
  "WiFi设备数量",
  "VPN设备数量",
  "有线网络设备数量",
  "异常网络设备数量",
  "空网络状态设备数量",
  "工作时上报次数",
  "工作日上报次数",
  "周末上报次数",
  "深夜上报次数"
FROM rb20_v2_5.source_members
WHERE run_id='{{run_id}}';

CREATE INDEX ON rb20_v2_5.source_members_slim(run_id, shard_id, ip_long);
ANALYZE rb20_v2_5.source_members_slim;
```

> 这一步对“机器很强但仍 hang”的场景非常关键：并发重查询时，真正能打爆你的是 **读放大 + temp 溢写 + 索引写争用**，不是 CPU。

---

### 3.3 输出写入：从“每任务 DELETE + INSERT”改成“准备阶段一次清理 + 任务只 INSERT”

你原脚本每 shard 都做多张表的 DELETE 清理。
在 bucket 化以后，一个 shard 会有多个 bucket 任务并发，如果每个 bucket 都 DELETE，会直接互相打架/锁冲突。

**改法：**

* 准备阶段（只做一次）：清理 run_id 相关数据（或更推荐：写入 staging 表）
* bucket 任务：只 INSERT，不 DELETE

---

### 3.4 推荐写入模式：staging（UNLOGGED、无索引）→ 收敛落表

这一步能显著降低并发写索引、WAL 压力，是“稳+快”的 ETL 工业做法。

**准备阶段：**

```sql
-- 无索引 staging，专门吃并发写
CREATE UNLOGGED TABLE IF NOT EXISTS rb20_v2_5.profile_pre_stage
(LIKE rb20_v2_5.profile_pre INCLUDING DEFAULTS);

-- 每个 run 复跑时清理一次（只做一次，不要每 bucket 做）
DELETE FROM rb20_v2_5.profile_pre_stage WHERE run_id='{{run_id}}';
```

> 你也可以用“每 run 建一张临时 staging 表然后 rename”的方式，效果更好，但运维复杂度略高。

---

### 3.5 bucket 执行 SQL 模板（核心：用 step03_block_bucket 过滤 block）

把你原来的 CTE `m` 改成从 `step03_block_bucket` 起步：

> 下方只展示关键骨架（字段保持你原逻辑即可），并强调两点：
> 1）**过滤入口从 shard 变成 (shard, bucket)**
> 2）**避免 CTE 物化风险**：尽量把 join 和聚合贴近写（PG11 及更早尤其重要）

```sql
BEGIN;

-- 建议每个任务的会话参数（可按机器调）
SET LOCAL enable_hashagg = on;
SET LOCAL jit = off;

-- 建议从“控制并发”而不是盲目 32 并发开始
-- work_mem 要结合并发与 parallel 一起算
SET LOCAL work_mem = '256MB';
SET LOCAL max_parallel_workers_per_gather = 2;

WITH m AS (
  SELECT
    map.block_id_natural,
    sm.ip_long,
    sm.is_valid,
    sm."设备数量" AS devices,
    sm."上报次数" AS reports,
    sm."移动网络设备数量" AS mobile_devices,
    sm."WiFi设备数量" AS wifi_devices,
    sm."VPN设备数量" AS vpn_devices,
    sm."有线网络设备数量" AS wired_devices,
    sm."异常网络设备数量" AS abnormal_net_devices,
    sm."空网络状态设备数量" AS empty_net_devices,
    sm."工作时上报次数" AS worktime_reports,
    sm."工作日上报次数" AS workday_reports,
    sm."周末上报次数" AS weekend_reports,
    sm."深夜上报次数" AS late_night_reports
  FROM rb20_v2_5.step03_block_bucket bb
  JOIN rb20_v2_5.map_member_block_natural map
    ON map.run_id=bb.run_id
   AND map.shard_id=bb.shard_id
   AND map.block_id_natural=bb.block_id_natural
  JOIN rb20_v2_5.source_members_slim sm  -- 没 slim 就先用 source_members
    ON sm.run_id=map.run_id
   AND sm.shard_id=map.shard_id
   AND sm.ip_long=map.ip_long
  WHERE bb.run_id='{{run_id}}'
    AND bb.shard_id={{shard_id}}::smallint
    AND bb.bucket_id={{bucket_id}}::int
),
agg AS (
  SELECT
    block_id_natural,
    COUNT(*)::bigint AS member_cnt_total,
    COUNT(*) FILTER (WHERE is_valid)::bigint AS valid_cnt,

    SUM(COALESCE(reports,0))::bigint AS reports_sum_total,
    SUM(COALESCE(reports,0)) FILTER (WHERE is_valid)::bigint AS reports_sum_valid,

    SUM(COALESCE(devices,0))::bigint AS devices_sum_total,
    SUM(COALESCE(devices,0)) FILTER (WHERE is_valid)::bigint AS devices_sum_valid,

    -- 其余 SUM 同理...
    SUM(COALESCE(late_night_reports,0))::bigint AS late_night_reports_sum_total,
    SUM(COALESCE(late_night_reports,0)) FILTER (WHERE is_valid)::bigint AS late_night_reports_sum_valid
  FROM m
  GROUP BY 1
),
score AS (
  SELECT
    a.*,
    (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) AS density,
    (a.reports_sum_valid::numeric / NULLIF(a.valid_cnt,0)) AS report_density_valid,
    CASE
      WHEN a.valid_cnt = 0 THEN NULL
      WHEN a.valid_cnt BETWEEN 1 AND 16 THEN 1
      WHEN a.valid_cnt BETWEEN 17 AND 48 THEN 2
      WHEN a.valid_cnt BETWEEN 49 AND 128 THEN 4
      WHEN a.valid_cnt BETWEEN 129 AND 512 THEN 8
      ELSE 16
    END AS wA,
    CASE
      WHEN a.valid_cnt = 0 THEN NULL
      WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 3.5 THEN 1
      WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 6.5 THEN 2
      WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 30 THEN 4
      WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 200 THEN 16
      ELSE 32
    END AS wD
  FROM agg a
),
tier AS (
  SELECT
    s.*,
    CASE WHEN s.valid_cnt = 0 THEN NULL ELSE (s.wA + s.wD) END AS simple_score,
    CASE
      WHEN s.valid_cnt = 0 THEN '无效块'
      WHEN (s.wA + s.wD) >= 40 THEN '超大网络'
      WHEN (s.wA + s.wD) >= 30 THEN '大型网络'
      WHEN (s.wA + s.wD) >= 20 THEN '中型网络'
      WHEN (s.wA + s.wD) >= 10 THEN '小型网络'
      ELSE '微型网络'
    END AS network_tier_pre,
    CASE WHEN s.valid_cnt = 0 THEN false ELSE true END AS keep_flag,
    CASE WHEN s.valid_cnt = 0 THEN 'ALL_ABNORMAL_BLOCK' ELSE NULL END AS drop_reason
  FROM score s
)
INSERT INTO rb20_v2_5.profile_pre_stage( /* 你的 profile_pre 列表 */ )
SELECT /* 与原 INSERT 逻辑一致 */ FROM tier;

COMMIT;
```

> 你原 SQL 中大量 `COALESCE(SUM(x),0)` 建议统一改成 `SUM(COALESCE(x,0))`：更一致、更少 NULL 分支，也便于二段聚合兜底。

---

## 4) 阶段 C：收敛落表（一次性写正式表 + 建索引 + ANALYZE）

当所有 bucket 完成后：

### 4.1 写入正式表（或直接把 stage 当正式表用）

如果必须落到 `profile_pre`：

```sql
-- 只做一次清理（按 run）
DELETE FROM rb20_v2_5.profile_pre WHERE run_id='{{run_id}}';

INSERT INTO rb20_v2_5.profile_pre(/*列*/)
SELECT /*列*/ FROM rb20_v2_5.profile_pre_stage
WHERE run_id='{{run_id}}';
```

### 4.2 索引策略（高吞吐写入建议“先写后建索引”）

如果 `profile_pre_stage` 上需要索引供下游使用，建议在 load 后统一建：

```sql
-- 示例：按你们实际查询模式建
CREATE INDEX ON rb20_v2_5.profile_pre_stage(run_id, shard_id, block_id_natural);
ANALYZE rb20_v2_5.profile_pre_stage;
```

---

## 5) 调度策略（你要的“让切片不倾斜”，还要“跑得快”）

### 5.1 计划验收指标（agent 生成 plan 后必须检查）

```sql
SELECT
  MAX(est_member_rows)::numeric / NULLIF(AVG(est_member_rows),0) AS max_over_avg,
  STDDEV_POP(est_member_rows)::numeric / NULLIF(AVG(est_member_rows),0) AS cv
FROM rb20_v2_5.step03_task_plan
WHERE run_id='{{run_id}}';
```

建议阈值（可自动化）：

* `max_over_avg <= 1.5`（越低越好）
* `cv <= 0.3`

不达标 → 提高 bucket 数/改用 LPT 装箱/启用超级 block 兜底。

### 5.2 worker 并发建议（40 核/256G 的合理起点）

你机器很强，但 Step03 的风险是 **I/O + temp + WAL + 索引争用**。因此我建议：

* **worker 并发先从 12~16** 起（而不是 32）
* 每 worker：`max_parallel_workers_per_gather=2`
* `work_mem` 先从 `256MB` 起（观察 temp spill 再调）

> 如果你把 bucket 切得足够均衡，吞吐通常会更稳定，整体时间也会更可控。

### 5.3 任务顺序

计划里按 `est_member_rows DESC` 排序，先跑重任务，避免最后长尾。

---

## 6) 其它你应该一起上的优化建议（“在不改业务逻辑”的前提下）

结合你文档里的 SQL 结构与瓶颈点，我建议把下面这些作为“同一套交付”的一部分：

1. **必须做 slim 表（或覆盖索引）**：减少宽表读放大
2. **输入索引补齐**：尤其是 `(run_id, shard_id, block_id_natural) INCLUDE (ip_long)`
3. **输出写 staging、避免并发 DELETE**：从根上减锁与膨胀
4. **避免 CTE 物化风险**：PG11 及更早版本尤其重要（你可以直接把 join/agg 更贴近写）
5. **会话级参数**：`work_mem` / `max_parallel_workers_per_gather` / `jit=off`
6. **ANALYZE**：数据装载后一定要 `ANALYZE`，否则优化器选错计划会“看起来像 hang”

---

## 7) 长期根治（可选）：重新定义 shard（如果你确认 shard_id 是范围切分）

如果你最终确认 `shard_id` 是按 IP 数值范围/高位切分导致天然倾斜，那么长期最干净的做法是：

* 给 `map_member_block_natural` 和 `source_members` 都增加一个 `shard_hash`（如 hash/mod），并按 `shard_hash` 驱动全链路
* 这样即使 IP 分布集中，也能把数据均匀打散到 shard

不过在你现在“想快速落地”的诉求下，**自适应 bucket 计划**已经能把倾斜问题压住，而且不必改上游产物结构。

---

“执行清单”

1. 跑 2.1 三个统计 SQL，拿到 shard_rows、block_sizes、max_block
2. 用 LPT 生成 `step03_block_bucket` + `step03_task_plan`（验收 max_over_avg / cv）
3. 建/确认必要索引（3.1）
4. 构建 `source_members_slim`（3.2）
5. 初始化 `profile_pre_stage` 并清理 run_id（3.4）
6. worker pool 并发 12~16，按计划逐个执行 bucket SQL（3.5 模板）
7. 全部完成后，收敛落表/建索引/ANALYZE（4）
