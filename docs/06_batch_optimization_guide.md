# 大批量处理优化指南

> 当数据量从当前 59.7M 增长到 1 亿、5 亿甚至更大规模时，如何优化 Pipeline 避免卡死。

## 1. 瓶颈分析

当前 Pipeline 在 59.7M 规模下已触达多个性能天花板：

```
数据规模增长路径:
  59.7M (当前) → 100M (1.7x) → 500M (8.4x) → 1B (16.7x)
```

| 瓶颈点 | 当前表现 | 100M 预估 | 500M 预估 |
|--------|---------|-----------|-----------|
| Step03 预画像 | 已优化(bucket) | 可控 | 需分布式 |
| Step11 窗口摘要 | 已优化(chunked) | 可控 | 需分布式 |
| Step01 源成员写入 | ~15min | ~25min | ~2h |
| Step02 自然块识别 | ~20min | ~35min | ~3h |
| Step04 切分 | ~10min | ~17min | ~1.5h |
| Step07 E 成员 | ~30min | ~50min | ~4h |
| 全局 H (Step05) | ~5min | ~8min | ~40min |
| QA 断言 | ~3min | ~5min | ~25min |
| **总计** | **~6h** | **~10h** | **~24h+** |

---

## 2. 短期优化（100M 规模，无需改架构）

### 2.1 将 bucket 方案推广到所有 per-shard 阶段

当前只有 Step03 和 Step11 使用了优化编排。其余步骤（01/02/04/04P/06/07/08）仍是"per-shard 直跑"。

**推广方案**:

```python
# 通用 bucket 编排框架
class BucketOrchestrator:
    def __init__(self, step_name, target_rows=200000, concurrency=8):
        self.step_name = step_name
        self.target_rows = target_rows
        self.concurrency = concurrency
    
    def plan(self, shard_id):
        """按 block 大小生成 bucket 计划"""
        # 复用 step03_block_bucket 的逻辑
        pass
    
    def execute(self, shard_id, bucket_id):
        """执行单个 bucket 的 SQL"""
        # 每步替换 SQL 模板
        pass
    
    def commit(self):
        """staging → 正式表"""
        pass
```

### 2.2 并发策略自适应

```
当前: 固定 CONCURRENCY = 8 (Step03/11), 32 (其余)
优化: 根据数据量自动调整

if total_rows < 50M:
    concurrency = 32, work_mem = '128MB'
elif total_rows < 200M:
    concurrency = 16, work_mem = '256MB'  
elif total_rows < 500M:
    concurrency = 8, work_mem = '512MB'
else:
    concurrency = 4, work_mem = '1GB'

# 约束: concurrency × work_mem ≤ 可用内存的 50%
```

### 2.3 索引策略优化

```sql
-- 1. 覆盖索引减少回表
CREATE INDEX CONCURRENTLY idx_map_cover ON rb20_v2_5.map_member_block_natural
  (run_id, shard_id, block_id_natural) INCLUDE (ip_long);

-- 2. 写入前删索引，写入后重建
-- (对 per-shard 步骤，写入完成后再统一建索引)
DROP INDEX IF EXISTS idx_xxx;
-- ... bulk insert ...
CREATE INDEX idx_xxx ON ...;
ANALYZE table;

-- 3. 分区表
-- 按 shard_id 做 PostgreSQL 原生分区
CREATE TABLE rb20_v2_5.source_members_partitioned (
    LIKE rb20_v2_5.source_members INCLUDING ALL
) PARTITION BY RANGE (shard_id);

-- 每个 shard 一个分区
CREATE TABLE rb20_v2_5.source_members_p00 
  PARTITION OF rb20_v2_5.source_members_partitioned 
  FOR VALUES FROM (0) TO (1);
-- ... (对每个 shard_id 重复)
```

### 2.4 PostgreSQL 调优参数

```sql
-- 针对批处理工作负载
ALTER SYSTEM SET shared_buffers = '16GB';          -- 总内存的 25%
ALTER SYSTEM SET effective_cache_size = '48GB';     -- 总内存的 75%  
ALTER SYSTEM SET maintenance_work_mem = '2GB';      -- 建索引/VACUUM
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
ALTER SYSTEM SET max_parallel_workers = 16;
ALTER SYSTEM SET wal_buffers = '256MB';
ALTER SYSTEM SET checkpoint_timeout = '30min';
ALTER SYSTEM SET max_wal_size = '8GB';

-- 批量导入时临时设置
SET LOCAL synchronous_commit = off;  -- 异步写 WAL
SET LOCAL work_mem = '512MB';
SET LOCAL jit = off;                 -- JIT 对批处理无益
```

---

## 3. 中期优化（500M 规模，轻量架构改造）

### 3.1 分区表 + 并行 INSERT

```sql
-- 按 shard_id 原生分区
-- 优势: DELETE 变为 TRUNCATE partition (秒级)
-- 优势: 索引自动按分区维护
-- 优势: 分区裁剪加速查询

-- 清理变为:
ALTER TABLE rb20_v2_5.source_members_p{shard_id} TRUNCATE;
-- 而不是:
DELETE FROM rb20_v2_5.source_members WHERE run_id=... AND shard_id=...;
```

### 3.2 COPY 替代 INSERT

```python
# 当前: INSERT INTO ... SELECT ... (行级写入)
# 优化: 先计算结果到临时文件，再 COPY 加载

import csv, io

buffer = io.StringIO()
writer = csv.writer(buffer)
for row in computed_results:
    writer.writerow(row)
buffer.seek(0)

with conn.cursor() as cur:
    cur.copy_expert(
        f"COPY rb20_v2_5.{table} FROM STDIN WITH CSV",
        buffer
    )
```

### 3.3 多数据库实例并行

```
实例 1 (主库): Phase 0-1 全局初始化
实例 2 (计算库): Shard 0-31 的 per-shard 步骤
实例 3 (计算库): Shard 32-63 的 per-shard 步骤
实例 1: Phase 6 (H) + Phase 8 (QA) 全局合并
```

### 3.4 物化视图 + 增量刷新

```sql
-- 预计算常用查询的物化视图
CREATE MATERIALIZED VIEW rb20_v2_5.mv_shard_stats AS
SELECT shard_id,
    COUNT(*) FILTER (WHERE classification='H') as h_cnt,
    COUNT(*) FILTER (WHERE classification='E') as e_cnt,
    COUNT(*) FILTER (WHERE classification='F') as f_cnt
FROM rb20_v2_5.mv_ip_classification
GROUP BY shard_id;

-- Pipeline 完成后刷新
REFRESH MATERIALIZED VIEW CONCURRENTLY rb20_v2_5.mv_shard_stats;
```

---

## 4. 长期优化（1B+ 规模，架构级改造）

### 4.1 分布式计算引擎

```
选项 A: Citus (PostgreSQL 分布式扩展)
  ├─ 保持 SQL 兼容
  ├─ 按 shard_id 分布式分片
  └─ 并行 JOIN 和聚合

选项 B: Spark + PostgreSQL
  ├─ Spark 负责计算 (Step01~08)
  ├─ PostgreSQL 只存储最终结果
  └─ 适合一次性大规模重算

选项 C: DuckDB (嵌入式 OLAP)
  ├─ 单机亿级数据秒级聚合
  ├─ 无需服务器部署
  └─ 适合分析和验证
```

### 4.2 推荐架构 (Citus)

```
                  ┌─────────────┐
                  │ Coordinator │
                  └─────┬───────┘
            ┌───────────┼───────────┐
       ┌────▼────┐ ┌────▼────┐ ┌────▼────┐
       │Worker 1 │ │Worker 2 │ │Worker 3 │
       │Shard 0- │ │Shard 22-│ │Shard 44-│
       │   21    │ │   43    │ │   64    │
       └─────────┘ └─────────┘ └─────────┘

分布键: (run_id, shard_id)
并置表: source_members, block_natural, profile_pre, ...
         全部按 (run_id, shard_id) 分布

优势:
  - SQL 几乎不用改
  - per-shard JOIN 在本地完成 (co-located)
  - 全局聚合自动并行
```

### 4.3 流式处理（实时增量）

```
新数据到达 (Kafka/CDC)
    │
    ├→ Flink/Spark Streaming: 实时计算 classification
    │     ├─ 查找最近的 block_final → 获取 network_tier
    │     ├─ 判定 H (中型网络块成员)
    │     ├─ 计算 atom27 密度 → 判定 E
    │     └─ 残余 → F
    │
    ├→ 写入 Redis 缓存
    │
    └→ 定期全量重算验证 (Pipeline, 每周/每月)
```

---

## 5. 关键参数调优速查表

| 参数 | 60M | 100M | 500M | 1B |
|------|-----|------|------|-----|
| `SHARD_CNT` | 64 | 64 | 128 | 256 |
| `CONCURRENCY` (Step03) | 8 | 8 | 4 | 4 |
| `CONCURRENCY` (其余) | 32 | 16 | 8 | 8 |
| `TARGET_ROWS_PER_BUCKET` | 200K | 200K | 150K | 100K |
| `BLOCK_CHUNK_SIZE` (Step11) | 500 | 500 | 300 | 200 |
| `work_mem` | 256MB | 256MB | 512MB | 1GB |
| PostgreSQL `shared_buffers` | 8GB | 12GB | 16GB | 32GB |
| 预计总耗时 | 6h | 10h | 24h | 分布式 |

---

## 6. 立即可做的改进清单

| # | 改进 | 复杂度 | 预期收益 |
|---|------|--------|---------|
| 1 | 所有 per-shard 步骤使用 staging 表 | 低 | 减少 40% 写入锁争用 |
| 2 | 统一 slim 表（Step03 和 Step11 共用） | 低 | 避免重复建表 |
| 3 | 增加断点续跑（step_status 表） | 中 | 失败后不需全量重跑 |
| 4 | 将 source_members 改为 PostgreSQL 分区表 | 中 | DELETE 变 TRUNCATE，快 100x |
| 5 | 写入后统一建索引（而非边写边维护） | 低 | 减少 30% 写入耗时 |
| 6 | 增加倾斜度自检（plan 生成后验证 CV） | 低 | 提前发现潜在卡死 |
