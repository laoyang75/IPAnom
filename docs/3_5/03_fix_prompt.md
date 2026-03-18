# RB20 v2.5 修复 Prompt（2026-03-05）

## 修复背景

本 Prompt 基于 `02_audit_report.md` 中识别的问题清单，进行逐项修复。
修复完成后输出到 `04_fix_report.md`。

## 修复原则

1. **按优先级顺序修复**：P0 → P1 → P2 → P3
2. **每个修复必须包含验证步骤**
3. **修改文件前先确认当前内容**（避免覆盖其他修复）
4. **遵循最小变更原则**（只改需要改的）
5. **SQL 模板修改需同步 03 和 04P**
6. **build 脚本修改后需实际运行验证**
7. **前端修改后需浏览器截图验证**

## 修复流程

对每个问题：

```
1. 读取问题描述
2. 定位文件和行号
3. 确认当前代码状态
4. 执行修复
5. 验证修复效果
6. 记录到 04_fix_report.md
```

## 验证清单

### V1. SQL 管道一致性验证
```sql
-- 确认 03 和 04P 的 wA/wD 逻辑完全一致
-- 确认 05 的准入条件包含 中/大/超大
-- 确认 keep_flag 逻辑
```

### V2. 数据库列一致性验证
```sql
-- 对比 build DDL 和实际 DB 列
SELECT column_name FROM information_schema.columns
WHERE table_schema='rb20_v2_5' AND table_name='h_block_summary'
ORDER BY ordinal_position;
```

### V3. 前端字段引用验证
```bash
# 提取前端引用的所有 h_block_summary 字段
grep -oP 'b\.\w+' webui/static/index.html | sort -u
# 与数据库列交叉比对
```

### V4. API 字段引用验证
```bash
# 提取 API 中对 h_block_summary 的 SQL 查询
grep -n 'h_block_summary\|bs\.' webui/api/*.py
```

### V5. 标签配置验证
```bash
# 提取 profile_tags.json 中引用的字段
jq '.tags[].conditions[].field' webui/config/profile_tags.json | sort -u
# 确认每个字段在 h_block_summary 中存在
```

### V6. 端到端功能验证（浏览器）
- [ ] H 库页面正常加载，所有列有值
- [ ] 画像标签页显示漏斗数据
- [ ] IP 邻域搜索返回结果
- [ ] IP 溯源搜索通过
- [ ] 排除标签后排序功能正常

### V7. 文档同步验证
- [ ] CHANGELOG.md 记录最近修改
- [ ] README.md 反映当前架构
- [ ] docs/ 目录管道文档与当前逻辑一致

## 修复产出

输出到 `docs/3_5/04_fix_report.md`，格式：

```markdown
# 修复报告

## 修复统计
| 优先级 | 问题数 | 已修复 | 跳过 | 说明 |

## 修复详情
### FIX-001: 问题标题
- **原问题ID**: A-001
- **修改文件**: 文件路径
- **修改内容**: diff 或描述
- **验证结果**: 通过/失败 + 证据

## 验证清单执行结果
### V1~V7 逐项结果
```
