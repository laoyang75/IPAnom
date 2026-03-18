# RB20 v2.5 深度审计 Prompt（2026-03-05）

## 审计背景

最近（2026-03-02 ~ 2026-03-05）对 IP 画像管道做了一系列重大修改：
1. **H库准入扩展**：从仅收"中型网络"扩展到"中型+大型+超大网络"
2. **ALL_ABNORMAL_BLOCK 修复**：不再丢弃 valid_cnt=0 的块，改为用 total 计数 fallback
3. **热修复注入**：3,059 块直接注入 block_final → profile_final → h_blocks → h_members
4. **h_block_summary 重建**：多次 DROP+CREATE 导致派生列反复丢失
5. **profile_tags.json 多次调整**：新增标签（轻度混合、混合网络）、调整阈值
6. **运营商数据修复**：中国移动设备从 9000万 → 4.41亿

这些修改分散在多个文件、多次会话中完成，存在以下风险：
- 代码一致性问题（SQL 模板 vs 热修复数据 vs build 脚本）
- 文档陈旧（README/CHANGELOG 未同步更新）
- 临时修复被永久化（ALTER TABLE 列 vs DDL 列）
- 前端 UI 引用字段与后端不匹配

## 审计范围

### A. SQL 管道模板（核心逻辑层）
| 文件 | 路径 | 审计重点 |
|------|------|----------|
| 03_pre_profile_shard.sql | `03_sql/RB20_03/` | wA/wD COALESCE、keep_flag、preh_blocks 过滤、注释一致性 |
| 04P_final_profile_shard.sql | `03_sql/RB20_04P/` | wA/wD COALESCE、tier 分类、与 03 同步性 |
| 05_h_blocks_and_members.sql | `03_sql/RB20_05/` | 准入条件 IN(中/大/超大)、与 profile_final 一致性 |
| 其余 SQL 阶段 | `03_sql/RB20_*` | 是否受上游修改影响 |

### B. 运行脚本（执行层）
| 文件 | 路径 | 审计重点 |
|------|------|----------|
| build_h_block_summary.py | `04_runbook/` | DDL 完整性、fill_derived_columns 正确性、性能 |
| orchestrate_*.py | `04_runbook/` | 是否引用旧逻辑 |
| run_shard_64*.py | 根目录 | 与 SQL 模板的参数传递 |

### C. WebUI（展示层）
| 文件 | 路径 | 审计重点 |
|------|------|----------|
| research.py | `webui/api/` | SQL 查询引用的列是否存在于 h_block_summary |
| profiling.py | `webui/api/` | 漏斗逻辑、字段引用完整性 |
| explorer.py | `webui/api/` | IP 溯源链 |
| dashboard.py | `webui/api/` | 统计口径 |
| profile_tags.json | `webui/config/` | 标签条件完整性、字段名与 DB 一致性 |
| index.html | `webui/static/` | 前端字段引用 vs h_block_summary 列清单 |

### D. 文档与配置
| 文件 | 路径 | 审计重点 |
|------|------|----------|
| README.md | 根 & webui/ | 是否反映当前架构 |
| CHANGELOG.md | 根 | 是否记录最近修改 |
| docs/01-10 | docs/ | 是否与修改后逻辑一致 |

### E. 数据完整性（数据库层）
| 对象 | 审计重点 |
|------|----------|
| h_block_summary 列清单 | DDL vs 实际列 vs 前端引用 三方一致 |
| profile_final | 热修复注入数据的 tier/score 与模板逻辑一致 |
| h_blocks | 行数与 h_block_summary 一致 |
| h_members | 行数合理（~16M） |
| block_final | 无重复 block_id_final |

## 审计产出

输出到 `docs/3_5/02_audit_report.md`，格式要求：

```markdown
# 审计报告

## 概要统计
| 类别 | 检查项 | 通过 | 警告 | 问题 |

## 问题清单
### P0 — 阻塞性问题（必须立即修复）
### P1 — 高优先级（影响数据质量或用户体验）
### P2 — 中优先级（代码质量/可维护性）
### P3 — 低优先级（文档/命名/风格）

## 每个问题格式
- **ID**: A-001
- **文件**: 具体文件路径
- **行号**: 具体行号
- **问题**: 问题描述
- **影响**: 影响范围
- **建议修复**: 具体修复方案

## 三方一致性矩阵
| 字段名 | h_block_summary DDL | h_block_summary 实际 | build 脚本 | 前端引用 | API引用 | 状态 |
```

## 审计规则

1. **不要修改任何文件**，只读取和报告
2. **每个问题必须有具体行号**
3. **区分"热修复前遗留"和"热修复引入"**的问题
4. **SQL 模板检查**：确认 COALESCE 逻辑在 03/04P 两个文件中完全同步
5. **列一致性检查**：以 h_block_summary 的 DDL 为真相源，交叉比对所有引用方
6. **标签逻辑检查**：确认 profile_tags.json 中每个字段在 h_block_summary 中存在
7. **废弃文件识别**：04_runbook 中是否有不再使用的脚本
