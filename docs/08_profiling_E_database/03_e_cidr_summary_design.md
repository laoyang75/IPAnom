# E库 CIDR 块级属性摘要表设计 (`e_cidr_summary`)

> **作者**: Agent / 用户协作  
> **日期**: 2026-03-02  
> **状态**: 设计评审中  
> **目标**: 将 E库 ~4200万 IP 级成员聚合到 ~13万 CIDR 块（`e_run`）级，产出一张可直接用于画像分析的属性宽表。

---

## 1. 背景与动机

### 1.1 为什么需要这张表

| 问题 | 现状 | 方案 |
|------|------|------|
| **行数太大** | `e_members` 有 4200万行，任何分析查询都很慢 | 聚合到 ~13万行 e_run 级 |
| **属性缺失** | 已有 `e_runs_summary`只有 10 个字段 | 从原始 IP 源表聚合 ~50 个属性维度 |
| **数据源不够原始** | 之前用 `source_members_slim`（已裁剪字段） | 直接用 `public.ip库构建项目_ip源表_20250811_20250824_v2_1`（63 个原始字段） |

### 1.2 核心思路

```
原始 IP 源表 (public, 63 字段)
    JOIN e_members (通过 ip_long)
    JOIN e_runs (通过 e_run_id，获取 run 结构信息)
    GROUP BY e_run_id
    → e_cidr_summary (~131K 行, ~50+ 字段)
```

### 1.3 与旧表的关系

- **`e_runs_summary`（旧表，10 字段）**：数据已就绪但字段不够，将被新表 **替代**（DROP + 重建）
- **`ip库构建项目_cidr属性表_*_v4`（旧版 cidr 属性表，44 字段）**：参考其聚合方式，但那是按旧系统的 cidr 段聚合的，不是按 e_run 聚合

---

## 2. 数据源清单

| 表名 | Schema | 用途 | 关联键 |
|------|--------|------|--------|
| `ip库构建项目_ip源表_20250811_20250824_v2_1` | public | 原始 IP 属性（63 字段） | `ip_long` |
| `e_members` | rb20_v2_5 | E库成员清单，含 `atom27_id`, `e_run_id` | `ip_long` → 原始表 |
| `e_runs` | rb20_v2_5 | E库连续 run 结构信息 | `e_run_id` → e_members |
| `e_atoms` | rb20_v2_5 | /27 原子密度信息 | `atom27_id` → e_members |
| `ip库构建项目_异常ip表_*_v2` | public | 异常 IP 标记 | `ip_long` |

---

## 3. 表结构设计

### 3.1 表名与存放

- **表名**: `rb20_v2_5.e_cidr_summary`
- **粒度**: 每个 `e_run_id` 一行
- **预期行数**: ~131,496 行

### 3.2 字段分组总览

| 分组 | 字段数 | 说明 |
|------|--------|------|
| A. 主键与结构信息 | 9 | run 本身的标识和结构属性 |
| B. 规模与质量指标 | 6 | IP 计数、异常标记 |
| C. 上报行为（绝对量） | 8 | 各类上报次数的 SUM |
| D. 设备行为（绝对量） | 9 | 各类设备数的 SUM |
| E. ID 标识维度 | 5 | 安卓ID/OAID/谷歌ID/启动ID/型号的 SUM |
| F. WiFi & 网络特征 | 5 | SSID/BSSID/网关/以太网的 SUM |
| G. 风险与异常特征 | 5 | 代理/Root/ADB/充电/制造商风险 |
| H. 衍生画像指标 | 10 | 各种 AVG/比例/密度指标 |
| I. 元数据 | 1 | 创建时间 |
| **合计** | **~58** | |

### 3.3 详细字段定义

#### A. 主键与结构信息（来自 `e_runs` + `e_members`）

| # | 字段名 | 类型 | 来源 | 说明 |
|---|--------|------|------|------|
| 1 | `e_run_id` | VARCHAR | `e_runs.e_run_id` | **主键**，CIDR 块唯一标识 |
| 2 | `run_id` | VARCHAR | `e_runs.run_id` | Pipeline 运行批次 |
| 3 | `shard_id` | SMALLINT | `e_runs.shard_id` | 所属分片 |
| 4 | `atom27_start` | BIGINT | `e_runs.atom27_start` | 起始 /27 原子编号 |
| 5 | `atom27_end` | BIGINT | `e_runs.atom27_end` | 结束 /27 原子编号 |
| 6 | `run_len` | INT | `e_runs.run_len` | 连续原子数量 |
| 7 | `short_run` | BOOLEAN | `e_runs.short_run` | 是否短 run（< 3） |
| 8 | `ip_range_start` | BIGINT | `e_runs.ip_start` | IP 范围起始 |
| 9 | `ip_range_end` | BIGINT | `e_runs.ip_end` | IP 范围结束 |

#### B. 规模与质量指标（聚合 `e_members` + 原始表 + 异常表）

| # | 字段名 | 类型 | 聚合方式 | 说明 |
|---|--------|------|----------|------|
| 10 | `ip_count` | INT | COUNT(ip_long) | 该 run 内总 IP 数 |
| 11 | `ip_density` | NUMERIC | ip_count / (run_len * 32) | IP 填充密度（0~1） |
| 12 | `abnormal_ip_count` | INT | SUM(异常标记) | 异常 IP 数量 |
| 13 | `abnormal_ip_ratio` | NUMERIC | abnormal / ip_count | 异常 IP 占比 |
| 14 | `unstable_ip_count` | INT | SUM(IP稳定性='不稳定网络') | 不稳定 IP 数量 |
| 15 | `unstable_ip_ratio` | NUMERIC | unstable / ip_count | 不稳定 IP 占比 |

#### C. 上报行为 — 绝对量（SUM 聚合自原始表）

| # | 字段名 | 类型 | 原始字段 | 说明 |
|---|--------|------|----------|------|
| 16 | `total_reports` | BIGINT | SUM(上报次数) | 总上报次数 |
| 17 | `total_reports_pre_filter` | BIGINT | SUM(过滤前上报次数) | 过滤前总上报 |
| 18 | `daa_reports` | BIGINT | SUM(DAA业务上报次数) | DAA 业务上报 |
| 19 | `dna_reports` | BIGINT | SUM(DNA业务上报次数) | DNA 业务上报 |
| 20 | `worktime_reports` | BIGINT | SUM(工作时上报次数) | 工作时间段上报 |
| 21 | `workday_reports` | BIGINT | SUM(工作日上报次数) | 工作日上报 |
| 22 | `weekend_reports` | BIGINT | SUM(周末上报次数) | 周末上报 |
| 23 | `late_night_reports` | BIGINT | SUM(深夜上报次数) | 深夜上报 |

#### D. 设备行为 — 绝对量（SUM 聚合）

| # | 字段名 | 类型 | 原始字段 | 说明 |
|---|--------|------|----------|------|
| 24 | `total_devices` | BIGINT | SUM(设备数量) | 去重设备总数 |
| 25 | `total_devices_pre_filter` | BIGINT | SUM(过滤前设备数量) | 过滤前去重设备 |
| 26 | `wifi_devices` | BIGINT | SUM(WiFi设备数量) | WiFi 设备数 |
| 27 | `mobile_devices` | BIGINT | SUM(移动网络设备数量) | 移动网络设备数 |
| 28 | `vpn_devices` | BIGINT | SUM(VPN设备数量) | VPN 设备数 |
| 29 | `wired_devices` | BIGINT | SUM(有线网络设备数量) | 有线设备数 |
| 30 | `abnormal_net_devices` | BIGINT | SUM(异常网络设备数量) | 异常网络设备数 |
| 31 | `empty_net_devices` | BIGINT | SUM(空网络状态设备数量) | 空网络状态设备 |
| 32 | `total_apps` | BIGINT | SUM(应用数量) | 应用总数 |

#### E. ID 标识维度（SUM 聚合）

| # | 字段名 | 类型 | 原始字段 | 说明 |
|---|--------|------|----------|------|
| 33 | `android_id_count` | BIGINT | SUM(安卓ID数量) | 安卓 ID 去重总数 |
| 34 | `oaid_count` | BIGINT | SUM(OAID数量) | OAID 去重总数 |
| 35 | `google_id_count` | BIGINT | SUM(谷歌ID数量) | 谷歌 ID 去重总数 |
| 36 | `boot_id_count` | BIGINT | SUM(启动ID数量) | 启动 ID 去重总数 |
| 37 | `model_count` | BIGINT | SUM(型号数量) | 设备型号总数 |
| 38 | `manufacturer_count` | BIGINT | SUM(制造商数量) | 制造商总数 |

#### F. WiFi & 网络特征（SUM 聚合）

| # | 字段名 | 类型 | 原始字段 | 说明 |
|---|--------|------|----------|------|
| 39 | `ssid_count` | BIGINT | SUM(SSID去重数) | SSID 去重总数 |
| 40 | `bssid_count` | BIGINT | SUM(BSSID去重数) | BSSID 去重总数 |
| 41 | `gateway_reports` | BIGINT | SUM(网关存在上报次数) | 有网关的上报次数 |
| 42 | `ethernet_reports` | BIGINT | SUM(以太网接口上报次数) | 以太网上报次数 |
| 43 | `wifi_comparable_reports` | BIGINT | SUM(WiFi可比上报次数) | WiFi 可比上报 |

#### G. 风险与异常特征（SUM 聚合）

| # | 字段名 | 类型 | 原始字段 | 说明 |
|---|--------|------|----------|------|
| 44 | `proxy_reports` | BIGINT | SUM(代理上报次数) | 代理上报 |
| 45 | `root_reports` | BIGINT | SUM(Root设备上报次数) | Root 设备上报 |
| 46 | `adb_reports` | BIGINT | SUM(ADB调试上报次数) | ADB 调试上报 |
| 47 | `charging_reports` | BIGINT | SUM(充电状态上报次数) | 充电中上报 |
| 48 | `max_single_device_reports` | BIGINT | MAX(单设备最大上报次数) | 单设备最大上报（取 MAX） |

#### H. 衍生画像指标（计算字段）

| # | 字段名 | 类型 | 计算方式 | 说明 |
|---|--------|------|----------|------|
| 49 | `avg_reports_per_ip` | NUMERIC | total_reports / ip_count | 每 IP 平均上报 |
| 50 | `avg_devices_per_ip` | NUMERIC | total_devices / ip_count | 每 IP 平均设备 |
| 51 | `avg_active_days` | NUMERIC | AVG(活跃天数) | 平均活跃天数 |
| 52 | `wifi_device_ratio` | NUMERIC | wifi / total_devices | WiFi 设备占比 |
| 53 | `mobile_device_ratio` | NUMERIC | mobile / total_devices | 移动设备占比 |
| 54 | `vpn_device_ratio` | NUMERIC | vpn / total_devices | VPN 设备占比 |
| 55 | `workday_report_ratio` | NUMERIC | workday / total_reports | 工作日上报占比 |
| 56 | `late_night_report_ratio` | NUMERIC | late_night / total_reports | 深夜上报占比 |
| 57 | `daa_dna_ratio` | NUMERIC | daa / NULLIF(dna, 0) | DAA/DNA 业务比 |
| 58 | `top_operator` | VARCHAR | MODE(IP归属运营商) | 主要运营商 |
| 59 | `distinct_operators` | INT | COUNT(DISTINCT IP归属运营商) | 运营商多样性 |

#### I. 元数据

| # | 字段名 | 类型 | 说明 |
|---|--------|------|------|
| 60 | `created_at` | TIMESTAMPTZ | 记录创建时间 |

---

## 4. 构建策略

### 4.1 JOIN 路径

```sql
e_runs r                                   -- 结构信息 (131K 行)
  JOIN e_members m ON r.e_run_id = m.e_run_id AND r.run_id = m.run_id
                                            -- 展开到 IP 级 (~42M 行)
  JOIN public."ip库构建项目_ip源表_20250811_20250824_v2_1" src 
       ON m.ip_long = src.ip_long           -- 关联原始属性 (63 字段)
  LEFT JOIN public."ip库构建项目_异常ip表_20250811_20250824_v2" abn
       ON m.ip_long = abn.ip_long           -- 异常标记 (可选)
GROUP BY r.e_run_id
```

### 4.2 性能考量

| 挑战 | 方案 |
|------|------|
| `e_members` 有 4200万行 | 按 `shard_id`（64个）分批并发处理 |
| 原始表无 shard_id | 用 `ip_long` 的 BETWEEN 范围过滤（基于 shard 的 IP 范围） |
| JOIN 两个大表 | 先按 shard 过滤 e_members，再 JOIN 原始表 |
| 预估运行时间 | ~15-25 分钟（64 shard 并发） |

### 4.3 构建脚本方案

使用 Python 脚本：
- 基于已有的 `orchestrate_e_runs_summary.py` 框架
- DROP 旧表 `e_runs_summary` → CREATE 新表 `e_cidr_summary`
- 64 个 shard 并发 INSERT（asyncpg + semaphore）
- 完成后创建索引：`(run_id, e_run_id)`, `(shard_id)`, `(ip_count)`, `(total_reports)`

---

## 5. 与旧版 cidr 属性表的对照

下表对比新 `e_cidr_summary` 与旧版 `cidr属性表_v4` 的覆盖情况：

| 旧版 cidr属性表字段 | 新表是否覆盖 | 新表对应字段 |
|-------------------|-------------|-------------|
| 段内ip总数 | ✅ | `ip_count` |
| 总上报次数 | ✅ | `total_reports` |
| 总去重设备数 | ✅ | `total_devices` |
| 总应用数 | ✅ | `total_apps` |
| 去重安卓id总数 | ✅ | `android_id_count` |
| 去重设备制造商总数 | ✅ | `manufacturer_count` |
| 去重ssid总数 | ✅ | `ssid_count` |
| 去重bssid总数 | ✅ | `bssid_count` |
| 工作日上报总数 | ✅ | `workday_reports` |
| 周末上报总数 | ✅ | `weekend_reports` |
| 单ip最大设备数 | ✅ | MAX（可在衍生指标中加入） |
| 平均上报数 | ✅ | `avg_reports_per_ip` |
| 平均设备数 | ✅ | `avg_devices_per_ip` |
| 平均活跃天数 | ✅ | `avg_active_days` |
| wifi设备占比 | ✅ | `wifi_device_ratio` |
| 移动设备占比 | ✅ | `mobile_device_ratio` |
| vpn设备占比 | ✅ | `vpn_device_ratio` |
| 异常ip数量/占比 | ✅ | `abnormal_ip_count/ratio` |
| 不稳定ip数量/占比 | ✅ | `unstable_ip_count/ratio` |
| 夜间活跃设备占比 | ✅ | `late_night_report_ratio` |
| 工作日/周末占比 | ✅ | `workday_report_ratio` |
| ip均匀度 | ✅ | `ip_density` (类似概念) |
| **新增：过滤前上报/设备** | 🆕 | 可对比过滤影响 |
| **新增：run_len/结构信息** | 🆕 | E库特有的连续性画像 |
| **新增：代理/Root/ADB风险** | 🆕 | 安全画像维度 |
| **新增：原子密度** | 🆕 | E库准入核心指标 |

---

## 6. 前端展示规划

### 6.1 宏观 KPI 卡片

在「🧪库画像研究 → 🟣E库」Tab 顶部，基于 `e_cidr_summary` 直接聚合：

| KPI 卡片 | 计算 |
|----------|------|
| CIDR 块总数 | COUNT(*) |
| 总 IP 数 | SUM(ip_count) |
| 平均每块 IP | AVG(ip_count) |
| 总上报次数 | SUM(total_reports) |
| 总去重设备 | SUM(total_devices) |
| 平均 run 长度 | AVG(run_len) |

### 6.2 CIDR 属性明细表

- 可排序、可筛选的表格（分页，每页 50 行）
- 支持按 `ip_count`、`total_reports`、`run_len`、`top_operator` 排序
- 支持按运营商、shard_id 筛选

### 6.3 画像图表

- **散点图**: `ip_count` vs `total_reports`(点大小=`run_len`)
- **直方图**: `run_len` 分布、`ip_density` 分布
- **条形图**: 运营商 Top10

---

## 7. 验证计划

### 7.1 数据完整性验证

```sql
-- 验证总 IP 数一致
SELECT SUM(ip_count) FROM rb20_v2_5.e_cidr_summary;
-- 应等于
SELECT COUNT(*) FROM rb20_v2_5.e_members;

-- 验证 run 数一致
SELECT COUNT(*) FROM rb20_v2_5.e_cidr_summary;
-- 应等于
SELECT COUNT(*) FROM rb20_v2_5.e_runs;
```

### 7.2 抽样验证

选 3 个 e_run_id，手动用 SQL 验算其聚合值是否与 `e_cidr_summary` 一致。

### 7.3 前端验证

启动 WebUI，在「🧪库画像研究 → 🟣E库」验证 KPI 卡片和明细表数据是否正确加载。
