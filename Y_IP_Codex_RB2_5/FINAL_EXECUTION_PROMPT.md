# RB20 v2.0 完整数据库重建 - Agent执行Prompt

你的任务是完成RB20 v2.0数据库的从零完整重建。已经完成了所有bug修复，代码已准备就绪。

## 背景信息

**数据库配置**:
- Host: 192.168.200.217
- Port: 5432
- Database: ip_loc2
- User: postgres
- Password: 123456
- Schema: rb20_v2_5

**Run ID**: `rb20v2_20260202_191900_sg_001`  
**Contract Version**: `contract_v1`  
**Shard Count**: 65

**已修复的关键bug**:
1. ✅ Shard plan SQL - 移除了5个除零断言
2. ✅ Orchestrator配置 - 使用SQL-only版本 + eps参数
3. ✅ source_members_slim验证逻辑 - 检查数据存在性并自动重建

---

## 执行目标

**主要目标**: 完成完整的数据库重建，生成最终的IP地理位置数据库

**关键验收指标**:
- `source_members`: ~59,229,751 行
- `block_natural`: ~13,194,429 行
- `profile_pre`: ~13,194,429 行
- `block_final`: >0 行
- `f_members`: >0 行
- 总执行时间: <2小时
- 无SQL错误

---

## 执行步骤

### Phase 0: 完全清空数据库 (5-10分钟)

**目的**: 清除所有旧数据，确保干净的起点

**执行**:
```bash
cd /Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/04_runbook

# 运行清理脚本
python3 run_cleanup.py
```

**验证**:
```sql
-- 关键表应该都是0
SELECT 
  'source_members' as tbl, count(*) as cnt 
FROM rb20_v2_5.source_members 
WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'block_natural', count(*) FROM rb20_v2_5.block_natural WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'profile_pre', count(*) FROM rb20_v2_5.profile_pre WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'shard_plan', count(*) FROM rb20_v2_5.shard_plan WHERE run_id='rb20v2_20260202_191900_sg_001';
```

**验收标准**: 所有表 cnt = 0

---

### Phase 1: 全流程执行 (30-90分钟)

**目的**: 执行完整的8阶段pipeline

**执行**:
```bash
cd /Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/04_runbook

# 执行完整流程
python3 orchestrate_fresh_start_v2.py 2>&1 | tee /tmp/rb20_full_rebuild_$(date +%Y%m%d_%H%M%S).log
```

**关键监控点**:

1. **Shard Plan (1-2分钟)**
   - 应该生成 **65个shard**
   - 验证: `SELECT count(*) FROM rb20_v2_5.shard_plan WHERE run_id='rb20v2_20260202_191900_sg_001';`
   - Expected: 65

2. **Abnormal Dedup (1-2分钟)**
   - 验证: `SELECT count(*) FROM rb20_v2_5.abnormal_dedup WHERE run_id='rb20v2_20260202_191900_sg_001';`
   - Expected: ~79,000

3. **Source Members + Natural Blocks (20-30分钟，最慢)**
   - 并行执行64个shard
   - 验证: 
     ```sql
     SELECT count(*) FROM rb20_v2_5.source_members WHERE run_id='rb20v2_20260202_191900_sg_001';
     SELECT count(*) FROM rb20_v2_5.block_natural WHERE run_id='rb20v2_20260202_191900_sg_001';
     ```
   - Expected: ~59M, ~13M

4. **⚠️ 关键验证点: source_members_slim**
   在Step 03开始前，脚本会自动检查：
   ```
   Verified: source_members_slim has 59229751 rows
   ```
   如果看到ERROR退出，说明slim表为空，需要手动修复（见下方故障处理）

5. **Step 03 Profile Pre (10-15分钟)**
   - Bucket生成 + 执行
   - 验证: `SELECT count(*) FROM rb20_v2_5.profile_pre WHERE run_id='rb20v2_20260202_191900_sg_001';`
   - Expected: ~13M (和block_natural接近)
   - ❌ 如果=0，见故障处理

6. **Step 11 + 后续阶段 (5-10分钟)**
   - 快速执行

**Phase 5-8: 最终库生成（关键交付物）**

根据《重构2.md》第8.4-8.5节的要求，orchestrator会自动执行以下步骤生成最终的E/H/F三库：

**Phase 5**: Step 04 & 04P（Per-Shard）
- Step 04：SplitEvents + Final Block Entity（切分最终块）
- Step 04P：Final Profile（最终块画像，包含`network_tier_final`）

**Phase 6**: Global H（全局）
- RB20_05：H Blocks + H Members（中型网络块库）
- 准入条件：`network_tier_final='中型网络'`

**Phase 7**: R1 + E + F（Per-Shard + 全局）
- RB20_06：R1 Members（剩余成员 = KeepMembers \ H_cov）
- RB20_07：E库（/27原子 + Runs + E Members）
  - E Atoms：/27原子实体
  - E Runs：连续run块实体
  - E Members：E覆盖成员实体
- RB20_08：F Members（最终剩余 = R1 \ E_cov）

**Phase 8**: QA验证
- RB20_99：终验收断言（STOP级别）
  - H/E/F互斥性检查
  - 成员守恒检查
  - 无幽灵成员检查

**所有阶段成功标志**: 
- 日志最后显示: `=== FULL RERUN SUCCESS ===`
- 无ERROR或FAILED消息

---

## 最终验证（重要！验证E/H/F三库）

执行完成后，运行完整验证查询：

```sql
-- 完整验证查询（包含E/H/F三库）
SELECT 
  'shard_plan' as tbl, count(*) as cnt FROM rb20_v2_5.shard_plan WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'abnormal_dedup', count(*) FROM rb20_v2_5.abnormal_dedup WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'source_members', count(*) FROM rb20_v2_5.source_members WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'source_members_slim', count(*) FROM rb20_v2_5.source_members_slim WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'block_natural', count(*) FROM rb20_v2_5.block_natural WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'profile_pre', count(*) FROM rb20_v2_5.profile_pre WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'block_final', count(*) FROM rb20_v2_5.block_final WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'profile_final', count(*) FROM rb20_v2_5.profile_final WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'h_blocks', count(*) FROM rb20_v2_5.h_blocks WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'h_members', count(*) FROM rb20_v2_5.h_members WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'e_atoms', count(*) FROM rb20_v2_5.e_atoms WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'e_runs', count(*) FROM rb20_v2_5.e_runs WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'e_members', count(*) FROM rb20_v2_5.e_members WHERE run_id='rb20v2_20260202_191900_sg_001'
UNION ALL SELECT 'f_members', count(*) FROM rb20_v2_5.f_members WHERE run_id='rb20v2_20260202_191900_sg_001';
```

**期望结果**:
| 表名 | 预期行数 | 说明 |
|------|----------|------|
| shard_plan | 65 | 分片计划 |
| abnormal_dedup | ~79,000 | 异常IP去重 |
| source_members | ~59,229,751 | 源成员（中国IP） |
| source_members_slim | ~59,229,751 | 源成员轻量表 |
| block_natural | ~13,194,429 | 自然块 |
| profile_pre | ~13,194,429 | 预画像 |
| block_final | >0 | 最终块（切分后） |
| profile_final | >0 | 最终块画像 |
| **h_blocks** | **>0** | **H库：中型网络块** |
| **h_members** | **>0** | **H库：中型网络覆盖成员** |
| **e_atoms** | **>0** | **E库：/27原子** |
| **e_runs** | **>0** | **E库：连续run块** |
| **e_members** | **>0** | **E库：E覆盖成员** |
| **f_members** | **>0** | **F库：最终剩余成员** |

---

## 故障处理

### 问题1: source_members_slim为空导致profile_pre=0

**症状**: 
```
ERROR: source_members_slim is empty for run_id=rb20v2_20260202_191900_sg_001. Cannot proceed.
```

**修复（5分钟）**:
```sql
-- 手动重建slim表
INSERT INTO rb20_v2_5.source_members_slim
SELECT
  run_id, shard_id, ip_long, is_valid,
  "设备数量" as devices, "上报次数" as reports,
  "移动网络设备数量" as mobile_devices, "WiFi设备数量" as wifi_devices,
  "VPN设备数量" as vpn_devices, "有线网络设备数量" as wired_devices,
  "异常网络设备数量" as abnormal_net_devices, "空网络状态设备数量" as empty_net_devices,
  "工作时上报次数" as worktime_reports, "工作日上报次数" as workday_reports,
  "周末上报次数" as weekend_reports, "深夜上报次数" as late_night_reports
FROM rb20_v2_5.source_members
WHERE run_id='rb20v2_20260202_191900_sg_001';

-- 验证
SELECT count(*) FROM rb20_v2_5.source_members_slim WHERE run_id='rb20v2_20260202_191900_sg_001';
-- Expected: 59229751

-- 重新执行Step 03
python3 /Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/04_runbook/orchestrate_step03_bucket_full.py
```

### 问题2: Shard Plan执行失败

**症状**: psql错误，shard_plan表为空

**原因**: SQL中的占位符没有正确替换或除零错误

**检查**: 查看 `/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql` 是否已修复（应该没有 `1/0` 表达式）

### 问题3: 执行中断或超时

**处理**:
1. 检查日志文件查看具体错误
2. 根据错误阶段，从该阶段重新执行（参考 `test_validation_plan.md`）
3. 不要重新执行cleanup，直接从失败的phase继续

---

## 参考文档

所有详细文档位于: `/Users/yangcongan/.gemini/antigravity/brain/9a81e270-8c21-43c3-916d-d4a6fbbf30ae/`

- **[test_validation_plan.md](file:///Users/yangcongan/.gemini/antigravity/brain/9a81e270-8c21-43c3-916d-d4a6fbbf30ae/test_validation_plan.md)**: 详细的阶段测试计划
- **[fix_profile_pre_complete.md](file:///Users/yangcongan/.gemini/antigravity/brain/9a81e270-8c21-43c3-916d-d4a6fbbf30ae/fix_profile_pre_complete.md)**: profile_pre问题的完整诊断和修复

---

## 执行要求

1. **严格按顺序执行**: Phase 0 → Phase 1
2. **每个验证点都要检查**: 不要跳过任何验证
3. **记录所有输出**: 使用 `tee` 保存日志
4. **遇到ERROR立即停止**: 不要继续执行，先诊断问题
5. **报告验证结果**: 最后提供完整的验证查询结果表格

---

## 预期时间线

- Phase 0清理: 5-10分钟
- Phase 1完整执行: 30-90分钟
  - Phase 1 Init: 2-3分钟
  - Phase 2 Source/Blocks: 20-30分钟（最慢）
  - Phase 3 Step 03: 10-15分钟
  - Phase 4-8: 5-10分钟
- 验证: 2-3分钟

**总计**: 约40-100分钟

---

## 成功标准

✅ 所有验证查询通过  
✅ 日志显示 "FULL RERUN SUCCESS"  
✅ 无SQL错误或FAILED状态  
✅ **E/H/F三库都有数据**：
  - h_blocks, h_members > 0（中型网络库）
  - e_atoms, e_runs, e_members > 0（/27原子库）
  - f_members > 0（最终剩余成员库）
✅ QA_Assert（Phase 8）全部通过（H/E/F互斥、成员守恒、无幽灵）  

完成后，向用户报告最终验证结果和执行日志路径。
