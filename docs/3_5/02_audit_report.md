# RB20 v2.5 深度审计报告（2026-03-05）

> 审计时间：2026-03-05 11:00-11:30
> 审计范围：SQL管道、运行脚本、WebUI、文档、数据完整性

## 概要统计

| 类别 | 检查项 | ✅ 通过 | ⚠️ 警告 | ❌ 问题 |
|------|--------|--------|---------|--------|
| A. SQL 管道 | 3 文件 | 3 | 1 | 0 |
| B. 运行脚本 | 1 文件 | 1 | 2 | 0 |
| C. WebUI | 5 文件 | 4 | 1 | 0 |
| D. 文档 | 3 文件 | 0 | 0 | 3 |
| E. 数据完整性 | 5 表 | 5 | 1 | 0 |

**总计**：13 ✅ / 5 ⚠️ / 3 ❌

---

## 问题清单

### P0 — 阻塞性问题
无

### P1 — 高优先级

#### A-001: build DDL 中 avg_devices_per_ip 重复定义
- **文件**: `04_runbook/build_h_block_summary.py`
- **行号**: L130 (H.衍生画像) + L146 (I.派生画像)
- **问题**: `avg_devices_per_ip` 在 DDL 中定义了两次 — 一次在 H 节（聚合时计算），一次在 I 节（fill_derived 覆盖）。DDL 会报错因为同名列不能定义两次。但因为当前 DB 是通过 ALTER TABLE 添加的而非 DDL 重建，所以暂时没影响。**下次 DROP+CREATE 重建会报错**。
- **影响**: 下次执行 build_h_block_summary.py 会失败
- **建议修复**: 从 I 节删除 `avg_devices_per_ip`，保留 H 节的定义

#### A-002: fill_derived_columns 中 abnormal_ip_count 关联子查询性能风险
- **文件**: `04_runbook/build_h_block_summary.py`
- **行号**: L375-378
- **问题**: 对 29034 行执行关联子查询（JOIN source_members + h_members），每行一次子查询。source_members 有 5970 万行，性能可能极差。
- **影响**: 重建时 fill_derived_columns 可能极慢（数小时级别）
- **建议修复**: 改用窗口函数或先预聚合再 JOIN UPDATE

### P2 — 中优先级

#### A-003: 03 SQL 注释过时
- **文件**: `03_sql/RB20_03/03_pre_profile_shard.sql`
- **行号**: L184
- **问题**: 注释说 "valid_cnt>0"，但该条件已被移除。代码仅检查 `keep_flag` 和跨 bucket64 边界。
- **建议修复**: 更新注释为 "Keep 块且跨 bucket64 边界的自然块（含全异常块）"

#### A-004: 05 SQL 注释过时
- **文件**: `03_sql/RB20_05/05_h_blocks_and_members.sql`
- **行号**: L35
- **问题**: 注释说 "valid 口径产物"，但现在 H 库也包含 valid_cnt=0 的块
- **建议修复**: 更新注释

#### A-005: build DDL H 节 avg_devices_per_ip 与 I 节 fill 重复覆盖
- **文件**: `04_runbook/build_h_block_summary.py`
- **行号**: L130 vs L365
- **问题**: 聚合 SQL 在 H 节 L130 已计算 `avg_devices_per_ip`（为 agg 计算值），而 fill_derived_columns L365 又用 total_devices/ip_count 覆盖。两者计算口径可能不一致。
- **建议修复**: 确认应使用哪个口径，删除多余的

#### A-006: profile_tags.json 电信 daa 阈值 3 可能过低
- **文件**: `webui/config/profile_tags.json`
- **行号**: L47
- **问题**: 电信移动出口的 daa 从 5 降到 3，与 `device_fraud`(daa<3) 阈值紧邻。可能导致刷机嫌疑块被标记为正常出口。
- **影响**: 标签优先级决定结果 — device_fraud 在电信移动之后执行，反序可能有不同结果
- **建议修复**: 考虑将 device_fraud 调整到移动标签之前执行，或保持当前顺序但添加注释

#### A-007: research.py summary-blocks 使用 SELECT *
- **文件**: `webui/api/research.py`
- **行号**: L1283
- **问题**: `SELECT * FROM h_block_summary` 返回全部 77 列到前端，大部分列前端不使用。浪费带宽。
- **建议修复**: 显式列出需要的列

### P3 — 低优先级

#### A-008: CHANGELOG.md 未更新
- **文件**: `CHANGELOG.md`
- **行号**: 末尾
- **问题**: 最近的 H库扩展、ALL_ABNORMAL_BLOCK 修复、标签调整未记录
- **建议修复**: 添加 v2.5.3 变更记录

#### A-009: docs/ 管道文档过时
- **文件**: `docs/01_pipeline_logic_overview.md`
- **问题**: 管道文档未反映 H 库准入扩展（中→中/大/超大）和 ALL_ABNORMAL_BLOCK 保留策略
- **建议修复**: 更新管道概览

#### A-010: webui/README.md 未更新
- **文件**: `webui/README.md`
- **问题**: 未记录画像标签功能、h_block_summary 派生列
- **建议修复**: 补充 WebUI 功能文档

#### A-011: 04_runbook 废弃脚本过多
- **文件**: `04_runbook/` 目录
- **问题**: 有 4 个 `fix_shard_plan_v*.py` 和多个历史 orchestrate 脚本，已不再使用
- **建议修复**: 归档到 `04_runbook/archive/`

---

## 三方一致性矩阵（关键字段）

| 字段名 | build DDL | DB实际 | 前端引用 | API引用 | 标签引用 | 状态 |
|--------|:---------:|:------:|:--------:|:-------:|:--------:|:----:|
| start_ip_text | ✅ I节 | ✅ | ✅ | ✅ | — | ✅ |
| avg_apps_per_ip | ✅ I节 | ✅ | ✅ | — | ✅ | ✅ |
| avg_devices_per_ip | ⚠️ H+I重复 | ✅ | ✅ | — | ✅ | ⚠️ |
| android_device_ratio | ✅ I节 | ✅ | ✅ | — | ✅ | ✅ |
| android_oaid_ratio | ✅ I节 | ✅ | ✅ | — | ✅ | ✅ |
| report_oaid_ratio | ✅ I节 | ✅ | ✅ | — | ✅ | ✅ |
| avg_manufacturer_per_ip | ✅ I节 | ✅ | ✅ | — | — | ✅ |
| avg_model_per_ip | ✅ I节 | ✅ | ✅ | — | — | ✅ |
| oaid_device_ratio | ✅ I节 | ✅ | ✅ | — | — | ✅ |
| abnormal_ip_count | ✅ I节 | ✅ | ✅ | ✅ | — | ✅ |
| abnormal_ip_ratio | ✅ I节 | ✅ | ✅ | — | — | ✅ |
| abnormal_rule_hits_total | ✅ I节 | ✅ | — | — | — | ✅ |
| mobile_device_ratio | ✅ H节 | ✅ | ✅ | — | ✅ | ✅ |
| wifi_device_ratio | ✅ H节 | ✅ | ✅ | — | ✅ | ✅ |
| daa_dna_ratio | ✅ H节 | ✅ | ✅ | — | ✅ | ✅ |
| top_operator | ✅ H节 | ✅ | ✅ | — | ✅ | ✅ |

## 数据完整性

| 表 | 行数 | 对齐 | 状态 |
|----|------|------|:----:|
| h_blocks | 29,034 | = h_block_summary | ✅ |
| h_block_summary | 29,034 | = h_blocks | ✅ |
| h_members | 16,302,902 | — | ✅ |
| profile_final | 13,281,844 | = block_final | ✅ |
| block_final | 13,281,844 | = profile_final | ✅ |
| block_final 重复 | 0 | — | ✅ |
| NULL start_ip_text | 0 | — | ✅ |
| NULL daa_dna_ratio | 5 | ip_count=0 的空块 | ⚠️ |
| NULL top_operator | 36 | 小块无上报 | ⚠️ |
