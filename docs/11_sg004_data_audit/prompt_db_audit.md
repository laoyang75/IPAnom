# 任务：sg_004 数据质量审计

你的任务是对 PostgreSQL 数据库 `ip_loc2` 中 `rb20_v2_5` schema 下 RUN_ID = `rb20v2_20260202_191900_sg_004` 的数据进行深度质量审计。

## 服务器

- PG Host: `192.168.200.217:5432`，DB: `ip_loc2`，User: `postgres`，Pwd: `123456`
- SSH: `root@192.168.200.217`，Pwd: `111111`
- Schema: `rb20_v2_5`
- RUN_ID: `rb20v2_20260202_191900_sg_004`

## 核心概念

- **H 库 (Hub)**：由连续 IP 组成的 CIDR 段，是核心画像单元。一个 H block 应该是有意义的连续 IP 段（不应该是单个 IP）
- **E 库 (Extended)**：不在 H 库中、但满足特定行为模式的 IP，以 CIDR 聚合为画像单元
- **F 库 (Filtrate)**：既不在 H 也不在 E 中的剩余 IP

## 审计检查项

请**逐项执行**以下检查，每项用 SQL 查证，输出结论和数据。

### 检查 1：H 库单 IP 块

H 库应该是连续的 CIDR 段，不应包含单个 IP（size=1）的块。

```sql
-- 1a. H blocks 中单 IP 块数量和占比
SELECT 
    COUNT(*) FILTER (WHERE bf.ip_end = bf.ip_start) AS single_ip_cnt,
    COUNT(*) AS total_h_blocks,
    ROUND(100.0 * COUNT(*) FILTER (WHERE bf.ip_end = bf.ip_start) / COUNT(*), 1) AS pct
FROM rb20_v2_5.h_blocks hb
JOIN rb20_v2_5.block_final bf ON bf.run_id=hb.run_id AND bf.block_id_final=hb.block_id_final
WHERE hb.run_id='rb20v2_20260202_191900_sg_004';

-- 1b. H blocks 按 size 分布
SELECT 
    CASE 
        WHEN bf.ip_end - bf.ip_start + 1 = 1 THEN '1 (single)'
        WHEN bf.ip_end - bf.ip_start + 1 BETWEEN 2 AND 3 THEN '2-3'
        WHEN bf.ip_end - bf.ip_start + 1 BETWEEN 4 AND 15 THEN '4-15'
        WHEN bf.ip_end - bf.ip_start + 1 BETWEEN 16 AND 63 THEN '16-63'
        WHEN bf.ip_end - bf.ip_start + 1 BETWEEN 64 AND 255 THEN '64-255'
        ELSE '256+'
    END AS size_bucket, COUNT(*) AS cnt
FROM rb20_v2_5.block_final bf
JOIN rb20_v2_5.h_blocks hb ON hb.run_id=bf.run_id AND hb.block_id_final=bf.block_id_final
WHERE bf.run_id='rb20v2_20260202_191900_sg_004'
GROUP BY 1 ORDER BY MIN(bf.ip_end - bf.ip_start + 1);

-- 1c. 单 IP H blocks 的 network_tier 分布（看它们的原始分类）
SELECT hb.network_tier_final, COUNT(*)
FROM rb20_v2_5.h_blocks hb
JOIN rb20_v2_5.block_final bf ON bf.run_id=hb.run_id AND bf.block_id_final=hb.block_id_final
WHERE hb.run_id='rb20v2_20260202_191900_sg_004' AND bf.ip_end = bf.ip_start
GROUP BY 1 ORDER BY 2 DESC;
```

**然后分析根因**：查看 Step 05 (`05_h_blocks_and_members.sql`) 的 H 库准入逻辑，确认是否缺少 block size 的最小阈值。该文件在 `/Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/03_sql/RB20_05/`。

### 检查 2：H/E/F 数据互斥性

三个库应该严格互斥（no overlap）。

```sql
-- 2a. H∩E
SELECT COUNT(*) FROM rb20_v2_5.h_members hm
WHERE hm.run_id='rb20v2_20260202_191900_sg_004'
AND EXISTS (SELECT 1 FROM rb20_v2_5.e_members em WHERE em.ip_long=hm.ip_long AND em.run_id=hm.run_id);

-- 2b. H∩F
SELECT COUNT(*) FROM rb20_v2_5.h_members hm
WHERE hm.run_id='rb20v2_20260202_191900_sg_004'
AND EXISTS (SELECT 1 FROM rb20_v2_5.f_members fm WHERE fm.ip_long=hm.ip_long AND fm.run_id=hm.run_id);

-- 2c. E∩F
SELECT COUNT(*) FROM rb20_v2_5.e_members em
WHERE em.run_id='rb20v2_20260202_191900_sg_004'
AND EXISTS (SELECT 1 FROM rb20_v2_5.f_members fm WHERE fm.ip_long=em.ip_long AND fm.run_id=em.run_id);

-- 2d. 总量一致性：H+E+F+Drop = Source?
SELECT 
    (SELECT COUNT(*) FROM rb20_v2_5.source_members WHERE run_id='rb20v2_20260202_191900_sg_004') as src,
    (SELECT COUNT(*) FROM rb20_v2_5.h_members WHERE run_id='rb20v2_20260202_191900_sg_004') as h,
    (SELECT COUNT(*) FROM rb20_v2_5.e_members WHERE run_id='rb20v2_20260202_191900_sg_004') as e,
    (SELECT COUNT(*) FROM rb20_v2_5.f_members WHERE run_id='rb20v2_20260202_191900_sg_004') as f,
    (SELECT COUNT(*) FROM rb20_v2_5.drop_members WHERE run_id='rb20v2_20260202_191900_sg_004') as d;
-- 然后计算 h+e+f+d 是否 = src
```

### 检查 3：E 库摘要画像

```sql
-- 3a. e_cidr_summary 是否有 sg_004 数据
SELECT COUNT(*) FROM rb20_v2_5.e_cidr_summary WHERE run_id='rb20v2_20260202_191900_sg_004';

-- 3b. 如果为空，检查有哪些 run_id 的数据
SELECT run_id, COUNT(*) FROM rb20_v2_5.e_cidr_summary GROUP BY 1;
```

如果为空，需要运行 `/Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/04_runbook/build_e_cidr_summary.py`。先读取该文件确认它的 run_id 配置，然后修改为 sg_004 并执行。

### 检查 4：F 库画像完整性

```sql
-- 4a. F 库有哪些表和数据
SELECT 'f_members' as tbl, COUNT(*) FROM rb20_v2_5.f_members WHERE run_id='rb20v2_20260202_191900_sg_004'
UNION ALL SELECT 'f_ip_summary', COUNT(*) FROM rb20_v2_5.f_ip_summary WHERE run_id='rb20v2_20260202_191900_sg_004';

-- 4b. 检查 F 库是否有画像标签配置
-- 查看 /Users/yangcongan/cursor/IP/webui/config/ 下是否有 f_profile_tags.json
```

**结论**：F 库应该像 E 库一样有画像标签和摘要画像。检查差什么，列出缺失清单。

### 检查 5：Step 05 H 库准入逻辑审计

读取 SQL 文件 `/Users/yangcongan/cursor/IP/Y_IP_Codex_RB2_5/03_sql/RB20_05/05_h_blocks_and_members.sql`，全文理解其逻辑后回答：

1. H 块准入条件是什么？有没有 block size 的最小阈值？
2. 如果没有，应该加什么条件？建议 `member_cnt_total >= 4`
3. 列出需要修改的具体 SQL 行号

## 输出要求

请将检查结果写入 `/Users/yangcongan/cursor/IP/docs/11_sg004_data_audit/audit_report.md`，格式：

```markdown
# sg_004 数据质量审计报告

## 检查 1: H 库单 IP 块
- 结果: [PASS/FAIL]
- 数据: [SQL 查询结果]
- 根因: [...]
- 修复建议: [...]

## 检查 2: H/E/F 互斥性
...（同上格式）

## 检查 5: Step 05 准入逻辑
...

## 修复方案总结
[按优先级排序的修复清单]
```
