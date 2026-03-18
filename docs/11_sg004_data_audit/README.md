# sg_004 数据质量审计 + UI 修复

本目录包含两个独立的 agent 任务 prompt，**请按顺序执行**。

## 文件说明

| 文件 | 用途 | 优先级 |
|---|---|---|
| `prompt_db_audit.md` | DB 数据质量审计（H 库单 IP、互斥性、摘要画像缺失） | 🔴 先做 |
| `prompt_ui_fix.md` | WebUI H/E/F 库展示修复 | 🟡 后做 |

## 使用方式

1. 新建对话，粘贴 `prompt_db_audit.md` 全文内容
2. 等 agent 输出审计报告到 `audit_report.md`
3. 新建对话，粘贴 `prompt_ui_fix.md` 全文内容
4. 等 agent 输出 UI 修复报告到 `ui_fix_report.md`

## 背景参考

更完整的背景说明见 `background.md`（可选阅读，不需要粘贴给 agent）。
