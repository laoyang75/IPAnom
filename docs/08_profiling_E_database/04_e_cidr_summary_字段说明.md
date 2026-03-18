# e_cidr_summary 字段说明

> 表位置：`rb20_v2_5.e_cidr_summary`
> 粒度：每行 = 1 个 E库 CIDR 连续段（e_run）
> 总行数：130,210
> 数据来源：`public.ip库构建项目_ip源表_20250811_20250824_v2_1`（63字段原始表）

---

## 示例记录（IP数最大的 CIDR 块）

**e_run_id**: `E26_59708096_59711752`

| # | 字段名 | UI表头 | 示例值 | 含义 | 来源 |
|---|--------|--------|--------|------|------|
| 1 | `e_run_id` | e_run_id | E26_59708096_59711752 | E库连续段唯一标识，格式 E{shard}_{atom27起始}_{atom27结束} | e_runs 表 |
| 2 | `shard_id` | — | 26 | 所属分片编号（0-63） | e_runs 表 |
| 3 | `run_len` | Run长 | **3657** | 该连续段包含多少个 /27 原子块。**不是 IP 数**。地址空间 = run_len × 32 = 117,024 | e_runs 表 |
| 4 | `ip_count` | IP数 | **76,058** | 该段内**实际被采集到的去重 IP 数量**（e_members 中属于该 run 且在原始表中有记录的 IP） | COUNT(ip_long) |
| 5 | `ip_density` | 密度 | **0.6499** | = ip_count / (run_len × 32)。实际采集 IP 占理论地址空间的比例。0.65 = 65% 地址有上报 | 计算 |
| 6 | `total_reports` | 总上报 | 1,856,726 | 该段所有 IP 的**过滤后**上报次数之和 | SUM("上报次数") |
| 7 | `total_reports_pre_filter` | 过滤前上报 | 1,884,831 | 过滤前上报次数之和 | SUM("过滤前上报次数") |
| 8 | `daa_reports` | DAA | 1,703,515 | DAA 业务上报次数之和 | SUM("DAA业务上报次数") |
| 9 | `dna_reports` | DNA | 153,211 | DNA 业务上报次数之和 | SUM("DNA业务上报次数") |
| 10 | `worktime_reports` | 工作时上报 | 668,799 | 工作时间段上报总数 | SUM("工作时上报次数") |
| 11 | `workday_reports` | 工作日上报 | 1,328,269 | 工作日上报总数 | SUM("工作日上报次数") |
| 12 | `weekend_reports` | 周末上报 | 528,457 | 周末上报总数 | SUM("周末上报次数") |
| 13 | `late_night_reports` | 深夜上报 | 212,492 | 深夜时段上报总数 | SUM("深夜上报次数") |
| 14 | `total_devices` | 总设备 | 169,846 | 所有 IP 的**过滤后**设备数之和 | SUM("设备数量") |
| 15 | `total_devices_pre_filter` | 过滤前设备 | 171,860 | 过滤前设备数之和 | SUM("过滤前设备数量") |
| 16 | `wifi_devices` | WiFi设备 | 168,819 | WiFi 网络设备数之和 | SUM("WiFi设备数量") |
| 17 | `mobile_devices` | 移动设备 | 164 | 移动网络设备数之和 | SUM("移动网络设备数量") |
| 18 | `vpn_devices` | VPN设备 | 517 | VPN 设备数之和 | SUM("VPN设备数量") |
| 19 | `wired_devices` | 有线设备 | 1 | 有线网络设备数之和 | SUM("有线网络设备数量") |
| 20 | `abnormal_net_devices` | 异常网络设备 | 333 | 异常网络状态设备数之和 | SUM("异常网络设备数量") |
| 21 | `empty_net_devices` | 空网络设备 | 12 | 网络状态为空的设备数之和 | SUM("空网络状态设备数量") |
| 22 | `total_apps` | 应用数 | 153,551 | 应用数量之和 | SUM("应用数量") |
| 23 | `android_id_count` | 安卓ID数 | 192,243 | 安卓ID 数量之和 | SUM("安卓ID数量") |
| 24 | `oaid_count` | OAID数 | 146,673 | OAID 数量之和 | SUM("OAID数量") |
| 25 | `google_id_count` | 谷歌ID数 | 37,422 | 谷歌广告ID 数量之和 | SUM("谷歌ID数量") |
| 26 | `boot_id_count` | 启动ID数 | 204,924 | 启动ID 数量之和 | SUM("启动ID数量") |
| 27 | `model_count` | 型号数 | 161,163 | 型号数量之和 | SUM("型号数量") |
| 28 | `manufacturer_count` | 制造商数 | 122,474 | 制造商数量之和 | SUM("制造商数量") |
| 29 | `ssid_count` | SSID数 | 37,558 | SSID 去重数之和 | SUM("SSID去重数") |
| 30 | `bssid_count` | BSSID数 | 46,135 | BSSID 去重数之和 | SUM("BSSID去重数") |
| 31 | `gateway_reports` | 网关上报 | 1,853,123 | 网关存在时的上报次数之和 | SUM("网关存在上报次数") |
| 32 | `ethernet_reports` | 以太网上报 | 5 | 以太网接口上报次数之和 | SUM("以太网接口上报次数") |
| 33 | `wifi_comparable_reports` | WiFi可比上报 | 358,971 | WiFi 可比上报次数之和 | SUM("WiFi可比上报次数") |
| 34 | `proxy_reports` | 代理上报 | 956 | 代理上报次数之和 | SUM("代理上报次数") |
| 35 | `root_reports` | Root上报 | 60 | Root 设备上报次数之和 | SUM("Root设备上报次数") |
| 36 | `adb_reports` | ADB上报 | 34,929 | ADB 调试上报次数之和 | SUM("ADB调试上报次数") |
| 37 | `charging_reports` | 充电上报 | 582,468 | 充电状态上报次数之和 | SUM("充电状态上报次数") |
| 38 | `max_single_device_reports` | 单设备最大上报 | 4,537 | 该段内单个设备上报次数最大值 | MAX("单设备最大上报次数") |
| 39 | `avg_reports_per_ip` | 均上报/IP | **24.41** | = total_reports / ip_count，每 IP 平均上报次数 | 计算 |
| 40 | `avg_devices_per_ip` | 均设备/IP | **2.23** | = total_devices / ip_count，每 IP 平均设备数 | 计算 |
| 41 | `avg_active_days` | 均活跃天 | **3.58** | 段内所有 IP 的平均活跃天数 | AVG("活跃天数") |
| 42 | `wifi_device_ratio` | WiFi比 | **0.9940** | = wifi_devices / total_devices | 计算 |
| 43 | `mobile_device_ratio` | 移动比 | **0.0010** | = mobile_devices / total_devices | 计算 |
| 44 | `vpn_device_ratio` | VPN比 | **0.0030** | = vpn_devices / total_devices | 计算 |
| 45 | `workday_report_ratio` | 工作日比 | **0.7154** | = workday_reports / total_reports | 计算 |
| 46 | `late_night_report_ratio` | 深夜比 | **0.1144** | = late_night_reports / total_reports | 计算 |
| 47 | `daa_dna_ratio` | DAA/DNA比 | **11.12** | = daa_reports / dna_reports | 计算 |
| 48 | `top_operator` | 运营商 | 中国联通 | 段内出现次数最多的运营商 | MODE("IP归属运营商") |
| 49 | `distinct_operators` | 运营商种类 | 1 | 去重运营商数量 | COUNT(DISTINCT) |
| 50 | `unstable_ip_count` | 不稳定IP | **67,408** | IP稳定性="不稳定网络"的 IP 数 | COUNT(WHERE "IP稳定性"='不稳定网络') |
| 51 | `unstable_ip_ratio` | 不稳定比 | 0.8863 | = unstable_ip_count / ip_count | 计算 |

---

## 关键概念区分

| 概念 | 示例值 | 说明 |
|------|--------|------|
| **run_len** | 3657 | /27 原子块个数，理论地址空间 = 3657 × 32 = **117,024** 个 IP 地址 |
| **ip_count** | 76,058 | 理论空间中**实际被SDK采集到**的去重 IP 数 |
| **ip_density** | 0.6499 | 76,058 / 117,024 ≈ 65%，地址空间的利用率 |

> 简单说：run_len 是"房间数"，ip_count 是"实际住人的房间数"，density 是"入住率"。
