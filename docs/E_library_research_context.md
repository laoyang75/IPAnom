# E 库研究上下文文件

> **用途**：新对话启动时，让 AI 读取此文件恢复完整上下文。  
> **生成时间**：2026-03-06  
> **项目**：IP 画像标注系统 RB20 v2.5

---

## ⚠️ 紧急修复项（新对话第一步）

> [!CAUTION]
> 以下问题需要在新对话中**首先修复**，否则 E 库页面会显示 H 库数据。

### BUG 清单

#### BUG-1: `profiling.py` funnel API 硬编码 `h_block_summary`

**文件**：`/Users/yangcongan/cursor/IP/webui/api/profiling.py`

**问题**：`compute_funnel()` 函数（L121-217）的所有 SQL 都硬编码了 `h_block_summary`：
- L131-135: `FROM {SCHEMA}.h_block_summary WHERE run_id = :run_id`（总量基线）
- L158-166: `FROM {SCHEMA}.h_block_summary WHERE ... AND ({where})`（标签匹配）
- L175-179: `FROM {SCHEMA}.h_block_summary WHERE {remain_clause}`（剩余池）

**现象**：当 E 库点击「画像标签」或「画像标签分布」时，调用的还是 `h_block_summary` 的数据，显示的全是 H 库内容。

**修复方案**：
1. 给 funnel API 增加 `lib` 参数：`GET /funnel?run_id=xxx&lib=h|e`
2. 根据 `lib` 参数选择表名：`h` → `h_block_summary`，`e` → `e_cidr_summary`
3. E 库需要**独立的标签配置文件** `e_profile_tags.json`（标签条件和 H 库完全不同）
4. 前端 `loadFunnel()` 调用时传入 `labLib.value` 参数

#### BUG-2: 画像标签模板 H/E 共用但数据源错误

**文件**：`/Users/yangcongan/cursor/IP/webui/static/index.html`

**问题**：
- L757: `v-if="labLib==='h' || labLib==='e'"` — H/E 共用画像标签按钮
- L774: `v-if="labSub==='profiling'"` — 共用同一个模板，数据全来自 `funnelData`
- `funnelData` 通过 `loadFunnel()` → `/api/research/profiling/funnel` 加载，但 API 只查 `h_block_summary`

**现象**：E 库点画像标签，显示的是 H 库的漏斗数据和标签卡片。

**修复方案**：
1. E 库画像标签应该加 `v-if="labLib==='h' && labSub==='profiling'"` 和 `v-if="labLib==='e' && labSub==='profiling'"` 分别处理
2. 或者让前端传 `lib` 参数给 funnel API（参见 BUG-1 修复）

#### BUG-3: `initLab()` E库默认进入 profiling 但数据是 H库的

**文件**：`/Users/yangcongan/cursor/IP/webui/static/index.html` L2696-2707

**问题**：
```javascript
} else if (labLib.value === 'e') {
    labSub.value = 'profiling';
    await loadFunnel();  // ← 这调用的是 H 库 funnel API
}
```

**修复方案**：E 库未完成标签体系前，默认进入 `summary`（块摘要画像）而非 `profiling`（画像标签）。
```javascript
} else if (labLib.value === 'e') {
    labSub.value = 'summary';
    await loadESummary();
}
```

#### BUG-4: 画像标签分布 sub-tab 对所有库共用 H 库数据

**文件**：`/Users/yangcongan/cursor/IP/webui/static/index.html` L767-768

**问题**：`📊 画像标签分布` 按钮对 H/E/F 全部可见，但 `loadTagDistribution()` → `loadFunnel()` → 查 `h_block_summary`

**修复方案**：
- 暂时只对 H 库显示：`v-if="labLib==='h'"` 加在按钮上
- 或者等 E 库标签体系完成后再开放

#### BUG-5: `e_cidr_summary` 缺少关键派生列

**表**：`rb20_v2_5.e_cidr_summary`（130,210 行）

**已有列**（60列）：基础统计都有（total_devices, total_reports, wifi_device_ratio, daa_dna_ratio 等）

**缺少列**：
- `avg_apps_per_device`（H 库刚加的新指标）
- `avg_apps_per_ip`（H 库的老指标）
- `start_ip_text`（人类可读的起始IP）
- `network_tier_final`（网络等级分类）
- `simple_score`
- `top_operator` ← **已存在** ✅

**修复方案**：
1. 先用 `ALTER TABLE ADD COLUMN` 添加缺少的派生列
2. 再用 UPDATE 填充（参考 H 库的 `fill_derived_columns` 逻辑）
3. 或者构建 `build_e_block_summary.py`（类似 H 库的构建脚本）

---

## 一、项目总览

### 1.1 三库体系

| 库 | 含义 | IP 规模 | 块结构 | 标签状态 |
|---|---|---|---|---|
| **H 库** | 高密度连续 CIDR 块（2C 宽带/移动 NAT） | 1,630 万 IP / 28,569 块 | `block_id_final`（合并后大块，平均 568 IP/块） | ✅ 已完成 12 标签漏斗 |
| **E 库** | 不完全连续，/27 最小 CIDR 块（企业/特殊用途） | 4,497 万 IP / 214 万 atom27 / 130,210 CIDR 段 | `atom27_id`（≤32 IP）→ 连续 run → `e_cidr_summary` | ❌ 待研究 |
| **F 库** | 剩余零散 IP | 173 万 IP | 无块结构 | 暂不处理 |

- **H ∩ E 重叠**：327 万 IP（同一 IP 可同时属于 H 库连续段和 E 库 atom27 块）
- **守恒关系**：`source(5970万) = keep + discard`，H/E/F 是 keep 的子视图，允许重叠
- **数据库 schema**：`rb20_v2_5`
- **当前 run_id**：`rb20v2_20260202_191900_sg_001`

### 1.2 核心代码位置

| 组件 | 路径 |
|---|---|
| WebUI HTML | `/Users/yangcongan/cursor/IP/webui/static/index.html` |
| Profiling API (漏斗引擎) | `/Users/yangcongan/cursor/IP/webui/api/profiling.py` |
| Research API (摘要/统计) | `/Users/yangcongan/cursor/IP/webui/api/research.py` |
| H 库标签配置 | `/Users/yangcongan/cursor/IP/webui/config/profile_tags.json` |
| H 库摘要构建脚本 | `/Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/04_runbook/build_h_block_summary.py` |
| WebUI 启动器 | `/Users/yangcongan/cursor/IP/webui/launcher.py` (端口 8721) |
| 远程服务器 | `root@192.168.200.217` (密码: `111111`, CentOS 7, Python 3.6.8) |

---

## 二、H 库已完成的决策记录

### 2.1 标签体系 (profile_tags.json)

**漏斗顺序（每个标签在前面标签的剩余池上工作）**：

1. 📱 正常移动出口·中国移动 (`mobile_cmcc`)
2. 📡 正常移动出口·中国联通 (`mobile_unicom`)
3. 📶 正常移动出口·中国电信 (`mobile_telecom`)
4. 🔀 混合网络 (`mobile_mixed`)
5. ☁️ 云/IDC (`cloud_idc`)
6. 🚨 安卓ID复用异常 (`android_id_anomaly`)
7. 🎭 应用造假/刷量嫌疑 (`app_fraud`)
8. 📵 刷机造假嫌疑 (`device_fraud`)
9. 🏠 正常固定网络·中国电信 (`wifi_telecom`)
10. 🏢 正常固定网络·中国移动 (`wifi_cmcc`)
11. 🏗️ 正常固定网络·中国联通 (`wifi_unicom`)
12. 🌐 正常固定网络·其他运营商 (`wifi_other`)

**已删除标签**：`mobile_mixed_light`（轻度混合网络，仅5块命中）

### 2.2 关键指标决策

| 决策 | 说明 |
|---|---|
| **apps/device 替代 apps/IP** | WiFi 标签条件从 TIERED `avg_apps_per_ip` 改为简单 `avg_apps_per_device >= 0.10`。apps/IP 对大网段有稀释效应，apps/device 跨规模稳定（P50 = 0.50~0.65） |
| **异常 IP 排除** | `build_h_block_summary.py` 在聚合时排除 `is_abnormal=true` 的 IP，防止数据失真 |
| **动态分档阈值** | 移动出口标签中 `avg_apps_per_ip` 仍用 TIERED 按设备量分档 |

### 2.3 h_block_summary 表关键列

```
block_id_final, shard_id, run_id, ip_count, density,
total_devices, total_reports, total_apps,
wifi_device_ratio, mobile_device_ratio,
avg_apps_per_ip, avg_apps_per_device, avg_devices_per_ip,
daa_dna_ratio, top_operator, network_tier_final,
abnormal_ip_count, abnormal_ip_ratio,
start_ip_text, simple_score
```

---

## 三、E 库数据结构详解

### 3.1 E 库表结构

#### `e_members` — E 库 IP 成员表（4,497 万行）
| 列 | 类型 | 说明 |
|---|---|---|
| run_id | text | 运行批次 |
| ip_long | bigint | IP 整数表示 |
| atom27_id | bigint | /27 最小 CIDR 块 ID |

#### `e_atoms` — E 库原子块表（214 万行）
| 列 | 类型 | 说明 |
|---|---|---|
| atom27_id | bigint | /27 块 ID |
| ip_start / ip_end | bigint | IP 范围 |
| valid_ip_cnt | integer | 块内有效 IP 数 |
| atom_density | numeric | 块密度 (valid / 32) |
| is_e_atom | boolean | 是否为 E 库原子 |

#### `e_cidr_summary` — E 库 CIDR 段汇总表 ⭐ 核心（130,210 行 / 60 列）
关键列：
- 标识：`e_run_id, run_id, shard_id, atom27_start, atom27_end`
- 结构：`run_len, short_run, ip_range_start, ip_range_end, ip_count, ip_density`
- 异常：`abnormal_ip_count, abnormal_ip_ratio, unstable_ip_count, unstable_ip_ratio`
- 量：`total_reports, total_devices, total_apps`
- 设备类型：`wifi_devices, mobile_devices, vpn_devices, wired_devices`
- 比例：`wifi_device_ratio, mobile_device_ratio, vpn_device_ratio`
- 时间：`workday_report_ratio, late_night_report_ratio`
- 活跃：`avg_reports_per_ip, avg_devices_per_ip, avg_active_days, daa_dna_ratio`
- 运营商：`top_operator, distinct_operators`
- ID：`android_id_count, oaid_count, google_id_count, boot_id_count`
- 特殊：`proxy_reports, root_reports, adb_reports, charging_reports`

### 3.2 E 库密度分布

| 密度档（每 atom27 块 IP数） | atom27 块数 | IP 总量 |
|---|---|---|
| 5-8 IP | 84,594 | 634,287 |
| 9-16 IP | 613,410 | 8,146,392 |
| 17-24 IP | 756,035 | 15,077,453 |
| 25-32 IP | 690,143 | 21,107,171 |

### 3.3 H 库 vs E 库的本质区别

| 维度 | H 库 | E 库 |
|---|---|---|
| IP 连续性 | 完全连续的大 CIDR 块 | /27 块级别连续，块间可不连续 |
| 典型用途 | 2C 宽带/移动 NAT 出口 | 企业专线/固定分配/特殊用途 |
| 块大小 | 平均 568 IP/块（最大 2万+） | ≤32 IP/atom27 块，连续段约 130K 段 |
| 设备密度 | 高（宽带共享 NAT） | 低-中（独立 IP，少量设备） |
| 标签方向 | 运营商分类 + 异常检测 | 企业类型识别 + 用途分类（待研究） |

---

## 四、E 库研究计划

### 4.1 修复步骤（按优先级）

1. **修 BUG-3**：`initLab()` 中 E 库默认改为 `summary` + `loadESummary()`
2. **修 BUG-4**：画像标签分布按钮加 `v-if="labLib==='h'"`，E 库暂时不显示
3. **修 BUG-1**：funnel API 添加 `lib` 参数，E 库查 `e_cidr_summary`
4. **创建 E 库标签配置**：`e_profile_tags.json`
5. **补齐 `e_cidr_summary` 派生列**：`avg_apps_per_device`, `start_ip_text` 等

### 4.2 研究方法

沿用 H 库的研究方法：
1. 先看全量数据分布（运营商、时间、设备类型）
2. 找出可辨识的自然聚类
3. 设计标签条件并用漏斗验证覆盖率
4. 迭代优化阈值

---

## 五、当前 WebUI sub-tab 结构

### H 库
📝 块摘要画像 (默认) → 🏷️ 画像标签 → 📋 库总览 → 📊 画像标签分布 → ⚠️ 问题检测

### E 库（有 BUG，需要修复）
🏷️ 画像标签 (默认，**错误地显示H库数据**) → 📝 块摘要画像 → 📋 库总览 → 📊 画像标签分布(**也是H库数据**) → ⚠️ 问题检测

### 已删除 sub-tabs（代码保留但 v-if=false 不渲染）
流转分析 / 深度分析 / 规则沙箱 / IP样本篮

---

## 六、环境信息

- **本地项目**：`/Users/yangcongan/cursor/IP/`
- **数据库**：PostgreSQL on 192.168.200.217, schema `rb20_v2_5`
- **WebUI**：`http://localhost:8721`，启动器 `launcher.py`
- **Python**：本地 3.11，服务器 3.6.8（需 psycopg2-binary==2.8.6）
- **服务器 SSH**：`sshpass -p '111111' ssh -T root@192.168.200.217`

---

## 七、Vue 模板注意事项

> [!WARNING]
> Vue 3 模板编译器**不支持箭头函数 `=>`**。所有需要在模板 `{{ }}` 中使用 `.reduce()/.map()/.filter()` 等的地方，都必须在 `<script>` 中用普通函数封装后导出到 return。
> 
> **错误写法**：`{{ funnelData.funnel?.reduce((s,f) => s + f.matched.blocks, 0) }}`
> **正确写法**：定义 `function tagSum(metric) { ... }` 然后模板用 `{{ tagSum('blocks') }}`
>
> 此问题已导致一次页面冻结事故。
