# DP-007：F 反连接实现（禁止 BETWEEN/NOT BETWEEN）

决策：按你的“其他都按推荐”已确认 **选A**（`atom27_id` 等值 anti-join）。

背景（主版本红线）：F 必须用 `atom27_id` 等值 anti-join（或“range+GiST”作为备选），禁止 `BETWEEN/NOT BETWEEN`。

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（推荐）
`atom27_id` 等值 anti-join（最直接）：
- `F = R1 LEFT JOIN E_cov_atoms USING (atom27_id) WHERE E_cov_atoms.atom27_id IS NULL`

### B
`atom27_id` 等值 anti-join + 额外审计列（例如命中原因/覆盖来源），便于解释与排障（不改变集合口径）。

### C
range+GiST 备选方案（仅当 A 性能不可接受时）：
- 以“原子/覆盖 run”形成范围实体（需要 GiST 索引），再做范围 anti-join
- 仍然禁止 BETWEEN/NOT BETWEEN，且不得区间展开造行

## 固化位置（被选中后）

- `02_contracts/schema_contract.*`（相关实体表与索引）
- `02_contracts/metric_contract.*`（口径与禁止项）
- 并在 `CHANGELOG.md` 记录原因与影响范围
