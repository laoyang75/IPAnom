# DP-014：严重倾斜时 shard_cnt 如何确定（是否允许 >64）

决策：**选B**（允许提高 shard_cnt；必须在开跑前写死）。

背景：
- 主版本默认以 `shard_cnt=64` 分片，并要求外部 orchestrator 并发 ≤32。
- 真实数据可能存在“倾斜严重/单 shard 过重”，导致 per-shard 步骤（尤其 Step64）出现超时或无法在可接受窗口内完成。
- 风险：若执行中途临时“加 shard / 改 shard_id 约束”继续跑，会导致口径漂移、漏跑 shard、以及“汇总数字看似正确但画像链路不完整”的不可验收状态。

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（默认：固定 64；不允许 >64）
- `shard_cnt=64` 固定不变，`shard_id=0..63`
- 若出现某 shard 过重：只能通过 SQL/索引优化、拆批调度（外部队列）解决；允许不均衡，不追求每 shard 同耗时
- 若仍无法完成：视为本轮不可跑通 ⇒ STOP，进入排障（不允许“加 shard”补丁）

### B（推荐：允许提高 shard_cnt；必须在开跑前写死）
- `shard_cnt` 允许从 64 提高到 128/192/256（上限 255；与 DDL shard_id 物理约束一致）
- 必须在 `RB20_00D`（ShardPlan）之前写死到合同（Schema/RunMeta/Runbook），并在整个 run 中保持不变
- 仍要求：
  - ShardPlan 输出 shard_id 连续：`0..(shard_cnt-1)`
  - per-shard 关键产物对 ShardPlan **全覆盖**（缺 1 个 shard 即不可验收）
- 适用：明显倾斜、64 shard 在既定资源/窗口内无法完成的场景

### C（高级：逻辑 64 + 执行子分片）
- 对外仍保持“逻辑 shard_cnt=64”（用于报告/习惯），但允许对个别重 shard 生成 `exec_sub_shard` 做执行层拆分
- 需要新增字段与映射（例如 `exec_shard_id`、`logical_shard_id`），并要求所有 per-shard SQL 支持该映射
- 优点：兼容既有“64 shard”心智；缺点：改造面更大，需额外合同与实现成本

## 强制规则（不随选项变化）
- 禁止执行中途修改 shard_cnt / 修改 shard_id 约束“继续跑”；若需要变更，只能新 run_id 重新生成 ShardPlan 并重跑下游
- 终验收必须包含：
  - `shard_plan_matches_shard_cnt`（数量/连续性）
  - `per_shard_outputs_complete`（关键实体全覆盖）
  - 全套 STOP 断言（H/E/F 不重不漏、无幽灵、Drop 映射不蒸发、F 反连接审计、cnt0 审计等）

## 固化位置（被选中后）
- `Y_IP_Codex_RB2_5/02_contracts/schema_contract_draft_v1.md`（`shard_cnt` 参数与 shard_id 范围约束）
- `Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`（按 `{{shard_cnt}}` 生成）
- `Y_IP_Codex_RB2_5/04_runbook/03_exec_agent_runbook_all_in_one.md`（执行时 shard 列表来自 ShardPlan）
- `Y_IP_Codex_RB2_5/03_sql/RB20_99/99_qa_assert.sql`（验收断言包含 shard_cnt 与 per-shard 覆盖）
- `Y_IP_Codex_RB2_5/CHANGELOG.md`（原因/影响/对应 DP-014）
