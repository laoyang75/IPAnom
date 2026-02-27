# RB20 v2.0 — Decision Points 状态表

## 已确认

- DP-001：选A（中国谓词：`IN ('中国')`）`Y_IP_Codex_RB2_5/01_decisions/DP-001_China_predicate.md`
- DP-002：选A（异常去重：DISTINCT）`Y_IP_Codex_RB2_5/01_decisions/DP-002_Abnormal_dedup_strategy.md`
- DP-003：选A（`设备数量` NULL：`COALESCE(...,0)`）`Y_IP_Codex_RB2_5/01_decisions/DP-003_Devices_null_handling.md`
- DP-004：选C（PreH=Keep 且跨 `bucket64` 边界）`Y_IP_Codex_RB2_5/01_decisions/DP-004_PreH_selection.md`
- DP-005：选A（除 `valid_cnt=0` 外全部 Keep）`Y_IP_Codex_RB2_5/01_decisions/DP-005_Keep_drop_rules.md`
- DP-006：选A（输出 schema=`rb20_v2_5`）`Y_IP_Codex_RB2_5/01_decisions/DP-006_Output_schema_strategy.md`
- DP-007：选A（F=`atom27_id` 等值 anti-join）`Y_IP_Codex_RB2_5/01_decisions/DP-007_F_antijoin_implementation.md`
- DP-008：选C（H/E/F Members 全量镜像投影）`Y_IP_Codex_RB2_5/01_decisions/DP-008_Member_attr_projection_width.md`
- DP-009：选A（假日=周末）`Y_IP_Codex_RB2_5/01_decisions/DP-009_Holiday_ratio_definition.md`
- DP-010：选A（密度分母=IP数量；移动设备占比/上报占比按标题选择分母）`Y_IP_Codex_RB2_5/01_decisions/DP-010_Block_ratio_aggregation_method.md`
- DP-011：选A（sum 标准化为 0；ratio 分母 0→NULL）`Y_IP_Codex_RB2_5/01_decisions/DP-011_Empty_sum_and_null_handling.md`
- DP-012：选C（ShardPlan 按分位数重切 NTILE，避免空 shard）`Y_IP_Codex_RB2_5/01_decisions/DP-012_ShardPlan_adjustment_heuristic.md`
- DP-013：选A（Step64 指标定义固化到合同）`Y_IP_Codex_RB2_5/01_decisions/DP-013_Step64_metric_definitions.md`
- DP-014：选B（允许提高 shard_cnt；必须在开跑前写死）`Y_IP_Codex_RB2_5/01_decisions/DP-014_Shard_count_under_skew.md`

## 待确认（进入全量/长跑前必须确认并写入正式合同）

（无）
