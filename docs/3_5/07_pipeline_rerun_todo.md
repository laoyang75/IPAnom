# 待办：管道重跑任务

> **创建时间**: 2026-03-05 12:40
> **优先级**: P1
> **计划执行**: 今晚（避免白天影响使用）

## 原因

profile_pre 表中有 **3,059 个 ALL_ABNORMAL_BLOCK** 的 `keep_flag = false`（涉及 12,014 个 IP），
这是因为 SQL 修改（`keep_flag = true`）后管道没有重跑，数据库中仍是旧数据。

## 影响

- 这些块在 block_final、profile_final、h_blocks 中都不存在
- 守恒公式略有偏差（12,014 IP 未进入最终画像）
- 但这些都是 **单 IP 异常块（valid_cnt=0）**，评分一般 < 20，不会进 H 库
- 对当前 H 库分析影响很小，但数据完整性需要修复

## 重跑范围

从 RB20_03 开始重跑（03 → 11 → 04 → 04P → 05 → summary）

```bash
# 方案 A：快速修正（仅修改 profile_pre 的 keep_flag）
UPDATE rb20_v2_5.profile_pre
SET keep_flag = true
WHERE run_id = 'rb20v2_20260202_191900_sg_001'
  AND keep_flag = false
  AND drop_reason = 'ALL_ABNORMAL_BLOCK';
# 然后从 RB20_04 开始重跑

# 方案 B：完整重跑
# 从 RB20_03 的 64 个 shard 开始全量重跑
```

## 注意事项

- 重跑后需重新执行 `build_h_block_summary.py`
- 重跑前确认 `03_pre_profile_shard.sql` 中 `keep_flag` 已改为 `true`（已确认 ✅）
- 重跑后验证守恒：`source_members = keep_members + drop_members`
