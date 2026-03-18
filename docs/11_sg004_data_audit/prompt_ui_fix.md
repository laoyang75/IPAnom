# 任务：H/E/F 库 WebUI 展示修复

> **前置条件**：本任务需要在 DB 审计（`prompt_db_audit.md`）完成后执行。
> 请先确认 `docs/11_sg004_data_audit/audit_report.md` 已存在。

## 服务器

- 本地 WebUI 目录：`/Users/yangcongan/cursor/IP/webui/`
- PG Host: `192.168.200.217:5432`，DB: `ip_loc2`，Schema: `rb20_v2_5`
- RUN_ID: `rb20v2_20260202_191900_sg_004`

## 问题描述

用户在 WebUI 中打开 H 库页面时看不到数据。需要排查 WebUI 的数据链路，确保 H/E/F 三个库都能在 UI 中正确展示。

## 检查步骤

### 步骤 1：理解 WebUI 架构

读取以下文件，理解 WebUI 的数据流：

- `webui/launcher.py` — 入口
- `webui/api/profiling.py` — 画像 API（核心）
- `webui/api/dashboard.py` — 仪表盘 API
- `webui/config/e_profile_tags.json` — E 库标签定义
- `webui/static/index.html` — 前端

产出：画数据流图（从前端请求 → API endpoint → SQL 查询 → DB 表）。

### 步骤 2：启动 WebUI 并检查

1. 启动 WebUI（如果未运行）：`cd /Users/yangcongan/cursor/IP && python3 webui/launcher.py`
2. 打开浏览器访问 WebUI
3. 分别点击 H/E/F 三个库的页面
4. 检查浏览器 Network 面板，记录：
   - 请求了哪些 API endpoint
   - 哪些返回空数据
   - 错误信息（如有）

### 步骤 3：定位数据链路断点

对每个返回空数据的 API：
1. 找到对应的 Python handler
2. 提取其 SQL 查询
3. 直接在 PG 中执行这些 SQL，看是否有数据
4. 确认断在哪一环（前端 → API → SQL → 表数据）

### 步骤 4：修复并验证

根据断点修复代码，修复后重启 WebUI，用浏览器截图确认 H/E/F 三个库都能正常展示。

## 输出要求

将结果写入 `/Users/yangcongan/cursor/IP/docs/11_sg004_data_audit/ui_fix_report.md`
