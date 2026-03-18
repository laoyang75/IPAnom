# sg_004 数据质量审计报告

审计对象：`rb20_v2_5` schema，`RUN_ID = rb20v2_20260202_191900_sg_004`

## 检查 1: H 库单 IP 块
- 结果: FAIL
- 数据:
  - H blocks 单 IP块：`5966 / 29014 = 20.6%`
  - H blocks 按 size 分布：

    | size_bucket | cnt |
    | --- | ---: |
    | 1 (single) | 5966 |
    | 2-3 | 1564 |
    | 4-15 | 1506 |
    | 16-63 | 2068 |
    | 64-255 | 5561 |
    | 256+ | 12349 |

  - 单 IP H blocks 的 `network_tier_final` 分布：`大型网络 = 5966`
  - 单 IP H blocks 的 `member_cnt_total`：`avg=1.00, min=1, max=1`
  - H blocks 中 `member_cnt_total < 4` 的块数：`7530`（其中 `1=>5966`, `2=>1072`, `3=>492`）
- 根因:
  - `Y_IP_Codex_RB2_5/03_sql/RB20_05/05_h_blocks_and_members.sql` 的 H 准入仅依赖 `network_tier_final IN ('中型网络','大型网络','超大网络')`，没有最小成员数阈值。
  - 结果是 `member_cnt_total = 1` 的单 IP 块，只要上游被打成“大型网络”，就会直接进入 H 库。
- 修复建议:
  - 在 H blocks 插入条件中增加 `AND pf.member_cnt_total >= 4`。
  - 该改动可一次性剔除全部 `5966` 个单 IP H blocks，同时也会剔除 `member_cnt_total` 为 2/3 的 H blocks（共 `1564` 个 size=2-3 块中的绝大多数）。

## 检查 2: H/E/F 互斥性
- 结果: PASS
- 数据:
  - `H ∩ E = 0`
  - `H ∩ F = 0`
  - `E ∩ F = 0`
  - 总量核对：

    | 指标 | 数量 |
    | --- | ---: |
    | source_members | 59706088 |
    | h_members | 16267013 |
    | e_members | 41728501 |
    | f_members | 1710574 |
    | drop_members | 0 |
    | h+e+f+d | 59706088 |

- 根因: 无。当前 H/E/F 三库成员严格互斥，总量守恒成立。
- 修复建议: 无。

## 检查 3: E 库摘要画像
- 结果: FAIL（初始为空，已修复）
- 数据:
  - 初始检查：`e_cidr_summary WHERE run_id='rb20v2_20260202_191900_sg_004' = 0`
  - 原表中仅有：`rb20v2_20260202_191900_sg_001 = 129851`
  - 执行补建后：

    | 指标 | 数量 |
    | --- | ---: |
    | e_cidr_summary(sg_004) | 129809 |
    | e_runs(sg_004) | 129809 |
    | missing_summary_runs | 0 |

- 根因:
  - `Y_IP_Codex_RB2_5/04_runbook/build_e_cidr_summary.py` 原始配置硬编码 `RUN_ID = sg_001`，不会为 `sg_004` 生成画像。
  - 脚本原始实现还假定分片固定为 `0-64`，但 `sg_004` 实际分片覆盖到 `0-241`；若不修正，补建会严重缺数。
  - 脚本原始 `prep_table()` 直接重建整张 `e_cidr_summary`，会误删其他 run 的已有结果，存在较大操作风险。
- 执行动作:
  - 已将 `Y_IP_Codex_RB2_5/04_runbook/build_e_cidr_summary.py` 改为：
    - 默认目标 `RUN_ID = sg_004`，且支持环境变量覆盖；
    - 只清理目标 `run_id` 的旧数据，不再整表 DROP；
    - 从 `e_members` 动态读取实际 `shard_id` 列表，按真实分片补建；
  - 已执行脚本，最终日志为：`242 success, 0 failed`，`DONE: 129809 CIDR blocks in e_cidr_summary`。
- 修复建议:
  - 保留当前“按 run_id 清理 + 动态 shard 枚举”的脚本逻辑，避免后续 run 再次出现漏跑或误删。

## 检查 4: F 库画像完整性
- 结果: FAIL
- 数据:

  | 表 / 配置 | sg_004 结果 |
  | --- | ---: |
  | f_members | 1710574 |
  | f_ip_summary | 0 |
  | webui/config/f_profile_tags.json | 已存在 |

- 根因:
  - F 库成员已存在，但 `f_ip_summary` 对 `sg_004` 完全为空，说明 F 库单 IP 画像摘要未构建。
  - `webui/config/f_profile_tags.json` 虽然存在，但 `webui/api/profiling.py` 当前只支持 `h/e` 两类库，`TABLE_MAP` 不含 `f`，`_load_tags()` / `_save_tags()` 也不会读取 `f_profile_tags.json`，因此 F 标签配置目前是“死配置”。
  - 当前仓库内虽然已有 `Y_IP_Codex_RB2_5/04_runbook/rebuild_f_and_summary.py` 可手工构建 `f_ip_summary`，但 `sg_004` 并未执行到这一步。
- 缺失清单:
  1. `rb20_v2_5.f_ip_summary` 的 `sg_004` 数据。
  2. `webui/api/profiling.py` 对 `lib='f'` 的查询路由支持。
  3. `f_profile_tags.json` 的加载 / 保存接线。
  4. F 库画像漏斗在 UI / API 侧的正式打通。
- 修复建议:
  - 先为 `sg_004` 补建 `f_ip_summary`。
  - 再将 profiling API 从 `h/e` 扩展到 `h/e/f`，使 F 库标签配置真正生效。

## 检查 5: Step 05 准入逻辑
- 结果: FAIL
- 数据:
  - H 块准入 SQL 位于 `Y_IP_Codex_RB2_5/03_sql/RB20_05/05_h_blocks_and_members.sql`。
  - 当前准入条件为：
    - `pf.run_id='{{run_id}}'`
    - `pf.network_tier_final IN ('中型网络','大型网络','超大网络')`
  - 不存在 block size / member count 的最小阈值。
- 逻辑结论:
  1. H 块准入条件目前仅看 `network_tier_final`，没有 `block size` 或 `member_cnt_total` 的最小阈值。
  2. 应增加条件：`pf.member_cnt_total >= 4`。
  3. 需要修改的具体 SQL 行号：
     - `Y_IP_Codex_RB2_5/03_sql/RB20_05/05_h_blocks_and_members.sql:33` —— 在现有 `network_tier_final` 过滤后追加 `AND pf.member_cnt_total >= 4`
     - `Y_IP_Codex_RB2_5/03_sql/RB20_05/05_h_blocks_and_members.sql:35` —— 注释应同步改为“由 `network_tier_final + member_cnt_total>=4` 决定”
- 根因:
  - Step 05 只消费上游画像等级，不校验“是否至少形成一个有意义的小连续段”，因此单点或 2~3 点的小碎块也能进入 H。
- 修复建议:
  - 先落地 `member_cnt_total >= 4`。
  - 如后续仍担心“非连续伪块”进入 H，可在上游 block/profile 侧增加更强的块尺寸一致性校验。

## 修复方案总结
1. **最高优先级**：修改 `05_h_blocks_and_members.sql`，给 H 准入增加 `member_cnt_total >= 4`。
2. **已完成**：修复并执行 `build_e_cidr_summary.py`，已为 `sg_004` 补齐 `e_cidr_summary`，且与 `e_runs` 数量完全对齐。
3. **高优先级**：为 `sg_004` 补建 `f_ip_summary`，补齐 F 库单 IP 画像摘要。
4. **高优先级**：打通 `webui/api/profiling.py` 对 `lib='f'` 的支持，让 `f_profile_tags.json` 真正生效。
5. **中优先级**：将 E/F 摘要构建纳入常规 runbook，避免依赖人工补跑。
