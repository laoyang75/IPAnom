# DP-012：ShardPlan 生成策略（范围等分+调整 vs 分位数重切）

决策：按你的授权“我可以根据结果自己判断、不要试太多次（最多 3 次）”，本期在真实数据分布（中国过滤后稀疏且成簇）下，固化为：
- 采用 **选C：按分位数重切（NTILE）**，直接生成 `shard_cnt` 个近似均衡的 shard
- 参数：默认 `shard_cnt=64`；若严重倾斜需提高分片数，走 DP-014 在合同里写死

背景（主版本 5.4 固化要求）：
- 初始按 `ip_long` 范围 64 等分
- 基于统计结果做 **2 次调整**：
  - 数量少的 shard 放大范围
  - 数量大的 shard 缩小范围
  - 调整尺度按 int 范围的 **5%**
- 两次后停止；允许不均衡
- 失败策略：空 shard/重叠 shard ⇒ 只能重生成 ShardPlan 并重跑下游

缺口：主版本未写死“如何判定数量大/小”和“如何移动边界且不重叠/不断档”。必须先选定实现口径写入合同。

你只需回复：`选A` / `选B` / `选C`

## 选项

### A（推荐：相对均值阈值 + 相邻边界推拉）
判定：
- `avg_cnt = 总行数/64`
- `big`：`cnt > avg_cnt * (1 + eps)`
- `small`：`cnt < avg_cnt * (1 - eps)`
- `eps` 默认建议 0.10（需要合同写死）

移动：
- 每轮对每个 shard 计算当前区间长度 `len = ip_end - ip_start`
- `big` shard：向两侧分别“缩小”边界 `shrink = floor(len*0.05/2)`（同时把缩出的范围分配给相邻 shard）
- `small` shard：向两侧分别“扩张”边界 `expand = floor(len*0.05/2)`（从相邻 shard 取出范围）

约束：
- 保持 shard 连续无重叠（边界只在相邻 shard 之间移动）
- 若某次移动会导致相邻 shard 变空或反向（start>=end），则该轮判定为失败 ⇒ STOP 并重生成

### B（推荐：按分位数选 Top/Bottom N）
判定：
- 每轮按 `cnt` 排序，选 Top N 为 big、Bottom N 为 small（N 例如 8，需要合同写死）

移动：
- 同 A（相邻边界推拉 5%）

### C（推荐：放弃“范围边界调整”，改为按分位数重切）
判定/实现：
- 先按 `ip_long` 排序生成累积分位点，直接切成 64 个“按行数近似均衡”的 shard（两轮迭代）

备注：
- 更均衡，但已偏离“按范围等分 + 5% 调整”的字面实现，属于策略变更；若选 C 必须在 CHANGELOG 写清原因与影响。

## 固化位置（被选中后）

- `02_contracts/metric_contract.*` 或 `02_contracts/schema_contract.*`（ShardPlan 生成规则参数：eps 或 N）
- `03_sql/00_contracts/10_shard_plan.sql`（实现 SQL）
- `CHANGELOG.md`（原因/影响/对应 DP-012）
