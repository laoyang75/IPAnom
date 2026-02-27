# Agent Review Prompt — Y_IP_Codex_RB2_5 完整性核查（必须严格）

你是代码审计/文档审计 agent。你的唯一目标：确认 `Y_IP_Codex_RB2_5/` 作为一次“全量重建版本”是**完整可复跑**的，不允许出现“写了保持不变/省略/略过导致信息丢失”的情况。

## 背景

- 这是 RB20 v2.0 的一次重构迭代版本，输出 schema 统一为：`rb20_v2_5`
- 旧版参考目录：`Y_IP_Codex_RB2/`
- 新版目标目录：`Y_IP_Codex_RB2_5/`

## 你必须完成的检查（逐条给出结论 + 证据）

### 1) 目录完整性（文件是否齐）

要求：`Y_IP_Codex_RB2_5/` 下必须存在并且内容非空（不是“省略”）：
- `README.md`
- `CHANGELOG.md`
- `重构2.md`
- `02_contracts/schema_contract_v1.md`
- `02_contracts/metric_contract_v1.md`
- `02_contracts/report_contract_v1.md`
- `03_sql/00_contracts/01_ddl_rb20_v2_full.sql`
- `03_sql/00_contracts/02_indexes_rb20_v2.sql`
- `03_sql/00_contracts/03_views_rb20_v2.sql`
- `03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`
- `03_sql/RB20_01..RB20_99` 的关键步骤 SQL（至少 01A/01/02/03/11/04/04P/05/06/07/08/99）
- `04_runbook/00_full_rebuild_strategy.md`
- `04_runbook/03_exec_agent_runbook_all_in_one.md`
- `04_runbook/04_perf_eval_and_fix_tasks_v1.md`
- `04_runbook/diagnose_run_status.sql`
- `04_runbook/orchestrate_rb20_v2.py`

输出：列一个 checklist，逐项标记 PASS/FAIL，并在 FAIL 时给出缺失路径。

### 2) 交叉引用完整性（最容易丢信息的点）

要求：所有 Markdown 里的反引号路径引用都必须是**真实存在的文件**。

建议执行（你可以用任何你习惯的方式，但必须给出结果）：
- 扫描 `Y_IP_Codex_RB2_5` 下所有 Markdown 文件中的反引号引用（例如 ...*.sql / ...*.py / ...*.md）
- 对不存在的引用必须列出“来源文件 → 引用路径”

输出：若为 0 个缺失引用则 PASS；否则 FAIL 并列清单。

### 3) schema 一致性（最硬规则）

要求：`Y_IP_Codex_RB2_5` 中所有 SQL/Runbook 必须一致指向 `rb20_v2_5`，不得混入 `rb20_v2`。

建议检查：
```bash
rg --pcre2 "\\brb20_v2\\b(?!_5)" Y_IP_Codex_RB2_5
```

输出：必须为 0 个命中；否则列出命中行并判 FAIL。

### 4) 执行入口一致性（避免“路径写错导致漏跑”）

要求：
- Runbook/Orchestrator 里写的 DDL/索引/视图/ShardPlan/QA 文件路径必须都存在
- `04_runbook/orchestrate_rb20_v2.py` 中的 `BASE_DIR` 必须指向 `Y_IP_Codex_RB2_5/03_sql`

输出：给出你核验到的关键路径列表（至少 10 个）并说明都存在。

### 5) “省略/保持不变”风险排查

要求：禁止出现会导致未来迭代丢信息的表述（例如“保持不变/略/省略/同上/不展开”等）出现在“规范/合同/Runbook”关键位置。

建议检查关键词（你可补充）：
- `保持不变|省略|略过|同上|不展开|TODO|TBD|FIXME`

输出：命中列表（文件+行号）以及你的判断：是否会导致信息丢失；若会则 FAIL 并给修改建议。

## 最终输出格式（必须）

1) 结论：PASS/FAIL（只允许一个）
2) 关键风险 Top-5（按严重程度排序，含证据）
3) 需要我（主 agent）修改的最小补丁列表（文件路径 + 1 句话说明）
