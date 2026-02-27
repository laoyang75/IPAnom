# 线上化迁移策略

> 从离线批处理 Pipeline → 在线实时查询服务的架构设计与实施路径。

## 1. 目标

将 RB20 v2.5 的核心产出（IP 分类画像：H/E/F + 网络规模 + 密度指标）转化为**毫秒级实时查询 API**，供下游业务系统实时调用。

```
当前: 离线 Pipeline (T+1) → PostgreSQL → 人工查询
目标: 离线 Pipeline (定期) → Online Cache → 实时 API → 下游业务
```

---

## 2. 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        在线查询层 (Query Engine)                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │
│  │ IP 查询   │    │ 块查询    │    │ 批量查询  │                  │
│  │ API       │    │ API       │    │ API       │                  │
│  └─────┬─────┘    └─────┬─────┘    └─────┬─────┘                │
│        └────────────────┼────────────────┘                      │
│                         │                                       │
│                   ┌─────▼──────┐                                │
│                   │ Redis 缓存层 │                                │
│                   └─────┬──────┘                                │
└─────────────────────────┼───────────────────────────────────────┘
                          │ fallback
┌─────────────────────────▼───────────────────────────────────────┐
│                     数据同步层 (Sync Engine)                     │
│  Pipeline 完成 → trigger sync → Redis + PostgreSQL 同步         │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                    离线计算层 (Data Pipeline)                    │
│  Phase 00 → 01 → 02 → 03 → 11 → 04 → 04P → 05 → 06~08 → 99  │
│  PostgreSQL (rb20_v2_5 schema)                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. 数据库操作基本处理逻辑

### 3.1 数据生命周期

```
阶段 1: 全量重建 (当前)
  - 每次新 run_id 全量计算
  - 保留最近 N 个 run_id 的数据（建议 N=3）
  - 过期 run_id 定时清理

阶段 2: 热数据/冷数据分层
  - 热数据: 最新 run_id → Redis + PostgreSQL 主表
  - 温数据: 前一个 run_id → PostgreSQL（可查询不缓存）
  - 冷数据: 更早 run_id → 压缩归档 / pg_dump → 对象存储

阶段 3: 增量更新（远期）
  - 新数据到达 → 识别受影响 Shard → 局部重算 → 增量同步
```

### 3.2 全量/增量切换逻辑

```python
def decide_update_strategy(new_data_ratio: float) -> str:
    """
    根据新数据占比决定更新策略
    new_data_ratio = 新增/变更 IP 数 / 总 source_members 数
    """
    if new_data_ratio > 0.3:
        return "FULL_REBUILD"     # 变更超过 30%，全量重建
    elif new_data_ratio > 0.05:
        return "SHARD_PARTIAL"   # 变更 5~30%，受影响 Shard 重算
    else:
        return "INCREMENTAL"      # 变更 < 5%，增量更新
```

### 3.3 核心数据库表操作模式

#### 写入（Pipeline 阶段）

```sql
-- 幂等写入模式: 先清理 → 再写入
BEGIN;
DELETE FROM rb20_v2_5.{table} 
WHERE run_id = :run_id AND shard_id = :shard_id;

INSERT INTO rb20_v2_5.{table} 
SELECT ... FROM ... WHERE ...;
COMMIT;

-- 全量完成后
ANALYZE rb20_v2_5.{table};
```

#### 读取（在线查询阶段）

```sql
-- 单 IP 查询 (命中索引，<1ms)
SELECT classification, network_tier_final, density, valid_cnt
FROM rb20_v2_5.v_ip_profile_wide
WHERE run_id = :latest_run_id AND ip_long = :ip_long;

-- 批量 IP 查询 (使用 ANY 数组)
SELECT ip_long, classification, network_tier_final
FROM rb20_v2_5.v_ip_profile_wide
WHERE run_id = :latest_run_id AND ip_long = ANY(:ip_long_array);
```

#### 同步（Pipeline → 在线层）

```sql
-- 创建物化视图供在线查询层使用
CREATE MATERIALIZED VIEW rb20_v2_5.mv_ip_classification AS
SELECT 
    sm.ip_long,
    sm.ip_address,
    sm.shard_id,
    CASE 
        WHEN h.ip_long IS NOT NULL THEN 'H'
        WHEN e.ip_long IS NOT NULL THEN 'E'
        WHEN f.ip_long IS NOT NULL THEN 'F'
        ELSE 'Drop'
    END AS classification,
    COALESCE(pf.network_tier, pp.network_tier_pre, '未分类') AS network_tier,
    COALESCE(pf.density, pp.density) AS density,
    COALESCE(pf.simple_score, pp.simple_score) AS simple_score
FROM rb20_v2_5.source_members sm
LEFT JOIN rb20_v2_5.h_members h USING (run_id, ip_long)
LEFT JOIN rb20_v2_5.e_members e USING (run_id, ip_long)
LEFT JOIN rb20_v2_5.f_members f USING (run_id, ip_long)
LEFT JOIN rb20_v2_5.map_member_block_final mf ON mf.run_id = sm.run_id AND mf.ip_long = sm.ip_long
LEFT JOIN rb20_v2_5.profile_final pf ON pf.run_id = mf.run_id AND pf.block_id_final = mf.block_id_final
LEFT JOIN rb20_v2_5.map_member_block_natural mn ON mn.run_id = sm.run_id AND mn.ip_long = sm.ip_long
LEFT JOIN rb20_v2_5.profile_pre pp ON pp.run_id = mn.run_id AND pp.block_id_natural = mn.block_id_natural
WHERE sm.run_id = :latest_run_id;

-- 建唯一索引支持 CONCURRENTLY 刷新
CREATE UNIQUE INDEX ON rb20_v2_5.mv_ip_classification(ip_long);
```

---

## 4. Redis 缓存层设计

### 4.1 数据模型

```
Key 设计:
  ip:{ip_long} → Hash {
      classification: "H",
      network_tier: "中型网络",
      density: "5.08",
      simple_score: "20",
      shard_id: "0",
      block_id: "N00_18380096_18380156_001",
      ip_address: "1.24.117.116"
  }

  block:{block_id} → Hash {
      network_tier: "微型网络",
      member_cnt: "61",
      valid_cnt: "61",
      density: "5.08",
      ip_start: "18380096",
      ip_end: "18380156"
  }
```

### 4.2 内存估算

```
单条 IP 记录: ~200 bytes (key + hash fields)
总 IP 数: ~59.7M (source_members)
Keep IP 数: ~59.7M (keep_flag=true 的)

估算 Redis 内存:
  59.7M × 200 bytes ≈ 11.9 GB
  + block 记录: 13.3M × 150 bytes ≈ 2.0 GB
  总计: ~14 GB

建议: 64GB Redis 实例，预留 4x 冗余
```

### 4.3 同步策略

```python
async def sync_to_redis(run_id: str):
    """Pipeline 完成后同步到 Redis"""
    
    # 1. 加载分类结果到 Redis Pipeline (批量写入)
    pipe = redis.pipeline()
    
    # 2. 分批读取 PostgreSQL（每批 10000 条）
    for batch in fetch_batches("SELECT * FROM mv_ip_classification", batch_size=10000):
        for row in batch:
            key = f"ip:{row['ip_long']}"
            pipe.hset(key, mapping={
                'classification': row['classification'],
                'network_tier': row['network_tier'],
                'density': str(row['density'] or ''),
                'simple_score': str(row['simple_score'] or ''),
                'ip_address': row['ip_address'],
            })
        pipe.execute()
    
    # 3. 原子切换: 更新版本标记
    redis.set("current_run_id", run_id)
    
    # 4. 清理旧版本 (异步)
    # ...
```

---

## 5. 实时 API 服务设计

### 5.1 端点设计

```
POST /api/v1/ip/classify
  Body: { "ip": "1.24.117.116" }
  Response: {
      "ip": "1.24.117.116",
      "ip_long": 18380148,
      "classification": "E",
      "network_tier": "微型网络",
      "density": 5.08,
      "simple_score": 6,
      "block_id": "N00_18380096_18380156_001",
      "is_malicious": false,
      "latency_ms": 0.8
  }

POST /api/v1/ip/batch-classify
  Body: { "ips": ["1.24.117.116", "1.24.119.164", ...] }
  Response: { "results": [...], "latency_ms": 2.3 }

GET /api/v1/ip/{ip_address}/detail
  Response: { 完整溯源信息 }
```

### 5.2 查询优先级

```
1. Redis Hash 查询 (< 1ms, 预期命中率 > 99%)
     ↓ miss
2. PostgreSQL 物化视图查询 (< 10ms)
     ↓ miss
3. 实时计算 (耗时较长，触发异步缓存回填)
```

### 5.3 性能目标

| 指标 | 目标 |
|------|------|
| 单 IP P50 延迟 | < 1ms |
| 单 IP P99 延迟 | < 5ms |
| 批量 100 IP P50 延迟 | < 10ms |
| QPS | > 10,000 |
| 可用性 | 99.9% |

---

## 6. 数据同步策略

### 6.1 同步触发

```
Pipeline Phase 99 QA ALL PASS
    │
    ├→ 通知同步引擎
    │
    ├→ 刷新物化视图: REFRESH MATERIALIZED VIEW CONCURRENTLY
    │
    ├→ 同步到 Redis (批量导入)
    │
    ├→ 健康检查: 抽样验证 1000 条数据一致性
    │
    └→ 切换版本标记 (current_run_id)
```

### 6.2 回滚能力

Redis 采用"双版本"策略:
- `ip:{run_id_new}:{ip_long}` — 新版本
- `ip:{run_id_old}:{ip_long}` — 旧版本
- `current_run_id` — 指向当前活跃版本

回滚时只需修改 `current_run_id` 指向旧版本，无需重新导入数据。

---

## 7. 监控与告警

### 7.1 业务指标

| 指标 | 告警阈值 | 说明 |
|------|---------|------|
| H/E/F 占比剧变 | 变化 > 5% | 数据质量问题 |
| 新增 unclassified IP | > 1% | 缓存未覆盖 |
| QA 断言失败 | 任意 STOP | 禁止同步新版本 |
| 同步延迟 | > 1h | Pipeline 完成后应在 1h 内完成同步 |

### 7.2 系统指标

| 指标 | 告警阈值 |
|------|---------|
| Redis 内存使用 | > 80% |
| API P99 延迟 | > 50ms |
| PostgreSQL 连接数 | > 80% max_connections |
| 磁盘使用 | > 85% |

---

## 8. 实施路线图

```
Phase 1 (当前): MVP — WebUI + PostgreSQL 直查 [✅ 已完成]
    │
Phase 2 (近期): 物化视图 + API 优化
    ├─ 创建 mv_ip_classification 物化视图
    ├─ 为 WebUI Shard 查询添加物化视图缓存
    └─ API 响应时间优化到 <100ms
    │
Phase 3 (中期): Redis 缓存 + 实时 API
    ├─ Redis 数据同步管道
    ├─ 独立 Query Engine 微服务
    ├─ 批量查询 API
    └─ 性能达标 (<5ms P99)
    │
Phase 4 (远期): 增量更新 + 高可用
    ├─ 分区增量 Pipeline
    ├─ Redis Sentinel / Cluster
    ├─ PostgreSQL 读写分离
    └─ 多数据中心部署
```

### 关键决策点

| 决策 | 选项 | 建议 |
|------|------|------|
| Redis vs Memcached | Redis Hash 更丰富 | Redis |
| 独立 API 服务 vs 扩展 WebUI | 独立服务更可控 | 独立 FastAPI 服务 |
| 同步时机 | 实时 vs 定时 | Pipeline 完成后触发 |
| 缓存粒度 | IP 级 vs Block 级 | 两层缓存 |
