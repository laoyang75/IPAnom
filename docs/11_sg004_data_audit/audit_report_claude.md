# sg_004 数据质量审计报告

> 审计时间：2026-03-11  
> 审计对象：`rb20_v2_5` schema, RUN_ID = `rb20v2_20260202_191900_sg_004`  
> 数据库：`ip_loc2` @ `192.168.200.217:5432`

---

## 检查 1: H 库单 IP 块

- **结果: ❌ FAIL**

### 1a. 单 IP 块数量和占比

| single_ip_cnt | total_h_blocks | pct |
|---|---|---|
| 5,966 | 29,014 | **20.6%** |

H 库中有 **5,966 个单 IP 块**，占全部 H 块的 **20.6%**。这意味着五分之一的 H 块实际只包含一个 IP，违背了 H 库"连续 CIDR 段"的核心定义。

### 1b. H blocks 按 size 分布

| size_bucket | cnt |
|---|---|
| 1 (single) | 5,966 |
| 2-3 | 1,564 |
| 4-15 | 1,506 |
| 16-63 | 2,068 |
| 64-255 | 5,561 |
| 256+ | 12,349 |

单 IP 块是最大的异常桶。如果加上 size 2-3 的微型块，则有 **7,530 个极小块（26.0%）**。

### 1c. 单 IP H blocks 的 network_tier 分布

| network_tier_final | count |
|---|---|
| 大型网络 | 5,966 |

所有 5,966 个单 IP 块全部归类为 **"大型网络"** tier。这说明这些 IP 在 profile_final 阶段被错误地赋予了"大型网络"标签，随后无条件进入 H 库。

### 根因分析

查看 `05_h_blocks_and_members.sql`（第 31-33 行），H 库准入条件为：

```sql
FROM rb20_v2_5.profile_final pf
WHERE pf.run_id='{{run_id}}'
  AND pf.network_tier_final IN ('中型网络','大型网络','超大网络');
```

**根因：H 库准入仅基于 `network_tier_final` 字段判断，完全没有 block size（member_cnt_total）的最小阈值检查。** 只要一个 block 被标记为"大型网络"，即使只有 1 个 IP 也会进入 H 库。

### 修复建议

在 `05_h_blocks_and_members.sql` 第 33 行后增加 block size 过滤条件：

```sql
  AND pf.network_tier_final IN ('中型网络','大型网络','超大网络')
  AND pf.member_cnt_total >= 4;   -- 新增：最少 4 个 IP 才能进入 H 库
```

同样需要在 H Members 的 INSERT 中确保一致性（通过 JOIN h_blocks 已自动过滤）。

---

## 检查 2: H/E/F 互斥性

- **结果: ✅ PASS**

### 2a-2c. 交叉检测

| 检测项 | 交叉 IP 数 | 结论 |
|---|---|---|
| H ∩ E | 0 | ✅ 无交叉 |
| H ∩ F | 0 | ✅ 无交叉 |
| E ∩ F | 0 | ✅ 无交叉 |

三个库**严格互斥**，没有任何 IP 重叠。

### 2d. 总量一致性

| 指标 | 数量 |
|---|---|
| source_members (src) | 59,706,088 |
| h_members (h) | 16,267,013 |
| e_members (e) | 41,728,501 |
| f_members (f) | 1,710,574 |
| drop_members (d) | 0 |
| **h + e + f + d** | **59,706,088** |

**h + e + f + d = 59,706,088 = src ✅**，总量完全一致，zero-loss 分发。

> [!NOTE]
> drop_members 为 0，说明本次运行没有丢弃任何 IP。所有 IP 都被分配到了 H/E/F 三个库之一。

---

## 检查 3: E 库摘要画像

- **结果: ❌ FAIL**

### 3a. e_cidr_summary 是否有 sg_004 数据

```
COUNT(*) = 0
```

**sg_004 的 e_cidr_summary 数据不存在。**

### 3b. 现有 run_id 数据

| run_id | count |
|---|---|
| rb20v2_20260202_191900_sg_001 | 129,851 |

e_cidr_summary 表中仅有 sg_001 的数据。

### 修复方案

需要运行 `build_e_cidr_summary.py`，但需先修改其 RUN_ID 配置：

- **文件**：`/Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/04_runbook/build_e_cidr_summary.py`  
- **第 32 行**：`RUN_ID = "rb20v2_20260202_191900_sg_001"` → 改为 `"rb20v2_20260202_191900_sg_004"`

> [!WARNING]
> 该脚本的 `prep_table()` 函数（第 53-61 行）会 **DROP TABLE 整个 e_cidr_summary**，这会删除已有的 sg_001 数据。  
> **建议**：修改脚本，将 `DROP TABLE` 改为 `DELETE FROM ... WHERE run_id = '{RUN_ID}'`，以保留其他 run 的数据。

---

## 检查 4: F 库画像完整性

- **结果: ⚠️ PARTIAL FAIL**

### 4a. F 库表和数据

| 表名 | 记录数 |
|---|---|
| f_members | 1,710,574 |
| f_ip_summary | 0 |

f_members 有数据，但 **f_ip_summary 为空**——F 库缺少 IP 级别的画像摘要。

### 4b. F 库画像标签配置

`f_profile_tags.json` 存在，包含 6 个标签：

| 标签 ID | 名称 | 说明 |
|---|---|---|
| cloud_service | ☁️ 云服务商 | 运营商为云厂商 |
| root_anomaly | 🚨 Root异常 | Root上报≥20% |
| mobile_exit | 📶 移动出口 | 移动占比≥85%，DAA/DNA≥3 |
| enterprise_line | 🏢 企业专线 | 工作日≥85%，深夜<3%，WiFi≥80% |
| scattered_mobile | 📱 零散移动 | 移动占比≥30% |
| residential_broadband | 🏠 家庭宽带 | WiFi>90%，深夜>5% |

### 缺失清单

1. **`f_ip_summary` 数据为空**：F 库需要类似 `build_e_cidr_summary.py` 的脚本来构建单 IP 级别的画像摘要，但目前没有找到对应的构建脚本（搜索 `f_ip_summary` 和 `f_summary` 均无结果）。
2. **缺少 F 库画像构建脚本**：需要创建 `build_f_ip_summary.py` 脚本，从源表聚合 F 库 IP 的画像数据，写入 `f_ip_summary` 表。
3. **画像标签无法应用**：由于 `f_ip_summary` 为空，`f_profile_tags.json` 中定义的 6 个画像标签无法执行（标签依赖 summary 表中的字段如 `top_operator`、`root_report_ratio` 等）。

---

## 检查 5: Step 05 H 库准入逻辑审计

- **结果: ❌ FAIL**

### Q1: H 块准入条件是什么？有没有 block size 的最小阈值？

H 块准入条件（`05_h_blocks_and_members.sql` 第 31-33 行）：
```sql
FROM rb20_v2_5.profile_final pf
WHERE pf.run_id='{{run_id}}'
  AND pf.network_tier_final IN ('中型网络','大型网络','超大网络');
```

**没有 block size 的最小阈值。** 唯一的筛选条件是 `network_tier_final` 属于中型/大型/超大网络。任何被标记为这三种 tier 的 block，无论包含多少 IP，都会无条件进入 H 库。

### Q2: 应该加什么条件？

建议增加 `member_cnt_total >= 4` 条件，过滤掉 IP 数量不足的微型块：

```sql
  AND pf.network_tier_final IN ('中型网络','大型网络','超大网络')
  AND pf.member_cnt_total >= 4;
```

理由：
- 少于 4 个 IP 的"块"不具备统计画像意义
- 单 IP "块"不是真正的连续段，不应归入 H 库
- 这些被排除的 IP 会自然流入 E 库或 F 库进行单点画像

### Q3: 需要修改的具体 SQL 行号

| 文件 | 行号 | 修改说明 |
|---|---|---|
| `05_h_blocks_and_members.sql` | 第 33 行 | 在 `network_tier_final IN (...)` 后追加 `AND pf.member_cnt_total >= 4` |

H Members 的 INSERT（第 36-48 行）通过 JOIN h_blocks 自动过滤，无需额外修改。

---

## 修复方案总结

按优先级排序：

| 优先级 | 问题 | 修复方案 | 影响范围 |
|---|---|---|---|
| **P0** | H 库准入无 size 阈值 (检查 1, 5) | `05_h_blocks_and_members.sql` 第 33 行加 `AND pf.member_cnt_total >= 4` | 会移除 ~7,530 个微型 H 块（size<4），这些 IP 将流入 E/F 库 |
| **P1** | E 库摘要画像缺失 (检查 3) | 修改 `build_e_cidr_summary.py` 的 RUN_ID 为 sg_004 并执行；同时修复 DROP TABLE 为增量清理 | sg_004 的 E 库画像依赖此数据 |
| **P2** | F 库画像摘要缺失 (检查 4) | 创建 `build_f_ip_summary.py` 脚本，构建 F 库单 IP 的画像摘要表 | F 库 170 万 IP 无画像数据 |
| — | H/E/F 互斥性 (检查 2) | 无需修复 | ✅ 通过 |

> [!IMPORTANT]
> P0 修复后需要**重跑 Step 05-07**（H 块、E 库、F 库），因为 H 库准入变化会级联影响 E 库和 F 库的成员分配。
