#!/bin/bash
# RB20 v2.5 补数脚本 - 在服务器上运行
# 使用方法: nohup ./bushu_server.sh > bushu.log 2>&1 &

set -e

RUN_ID="rb20v2_20260202_191900_sg_001"
CONTRACT="contract_v1"
DB_NAME="ip_loc2"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

run_sql() {
    psql -d "$DB_NAME" -c "$1"
}

log "=========================================="
log "RB20 v2.5 补数开始"
log "Run ID: $RUN_ID"
log "待补分片: 62, 63 (完整 06→07→08)"
log "待补分片: 55, 61 (07→08)"
log "=========================================="

# ===== Shard 62: 完整 RB20_06 → 07 → 08 =====
log ""
log ">>> [1/4] Shard 62: RB20_06 R1 Members..."
run_sql "
DELETE FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=62;
INSERT INTO rb20_v2_5.r1_members(run_id, contract_version, shard_id, ip_long, atom27_id, block_id_natural, block_id_final)
SELECT '$RUN_ID','$CONTRACT', 62, km.ip_long, sm.atom27_id, km.block_id_natural, mf.block_id_final
FROM rb20_v2_5.keep_members km
JOIN rb20_v2_5.source_members sm ON sm.run_id=km.run_id AND sm.shard_id=km.shard_id AND sm.ip_long=km.ip_long
LEFT JOIN rb20_v2_5.map_member_block_final mf ON mf.run_id=km.run_id AND mf.shard_id=km.shard_id AND mf.ip_long=km.ip_long
WHERE km.run_id='$RUN_ID' AND km.shard_id=62
  AND NOT EXISTS (SELECT 1 FROM rb20_v2_5.h_members hm WHERE hm.run_id=km.run_id AND hm.ip_long=km.ip_long);
DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_06' AND shard_id=62;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '$RUN_ID','$CONTRACT','RB20_06', 62, 'r1_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=62;
"

log ">>> [1/4] Shard 62: RB20_07 E Atoms/Runs/Members..."
run_sql "
DELETE FROM rb20_v2_5.e_members WHERE run_id='$RUN_ID' AND shard_id=62;
DELETE FROM rb20_v2_5.e_runs WHERE run_id='$RUN_ID' AND shard_id=62;
DELETE FROM rb20_v2_5.e_atoms WHERE run_id='$RUN_ID' AND shard_id=62;

WITH r1 AS (SELECT ip_long, atom27_id FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=62),
r1v AS (SELECT r1.atom27_id, sm.is_valid FROM r1 JOIN rb20_v2_5.source_members sm ON sm.run_id='$RUN_ID' AND sm.shard_id=62 AND sm.ip_long=r1.ip_long),
agg AS (SELECT atom27_id, COUNT(*) FILTER (WHERE is_valid)::integer AS valid_ip_cnt FROM r1v GROUP BY 1)
INSERT INTO rb20_v2_5.e_atoms(run_id, contract_version, shard_id, atom27_id, ip_start, ip_end, valid_ip_cnt, atom_density, is_e_atom)
SELECT '$RUN_ID','$CONTRACT', 62, atom27_id, (atom27_id * 32)::bigint, (atom27_id * 32 + 31)::bigint, valid_ip_cnt, (valid_ip_cnt::numeric / 32.0), (valid_ip_cnt >= 7) FROM agg;

WITH ea AS (SELECT atom27_id FROM rb20_v2_5.e_atoms WHERE run_id='$RUN_ID' AND shard_id=62 AND is_e_atom),
seq AS (SELECT atom27_id, (atom27_id - ROW_NUMBER() OVER (ORDER BY atom27_id)) AS grp FROM ea),
runs AS (SELECT MIN(atom27_id)::bigint AS atom27_start, MAX(atom27_id)::bigint AS atom27_end, COUNT(*)::integer AS run_len FROM seq GROUP BY grp)
INSERT INTO rb20_v2_5.e_runs(run_id, contract_version, shard_id, e_run_id, atom27_start, atom27_end, run_len, short_run, ip_start, ip_end)
SELECT '$RUN_ID','$CONTRACT', 62, ('E62_' || atom27_start::text || '_' || atom27_end::text), atom27_start, atom27_end, run_len, (run_len < 3), (atom27_start * 32)::bigint, (atom27_end * 32 + 31)::bigint FROM runs;

WITH r1 AS (SELECT ip_long, atom27_id FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=62),
atom_to_run AS (SELECT ea.atom27_id, er.e_run_id FROM rb20_v2_5.e_atoms ea JOIN rb20_v2_5.e_runs er ON er.run_id=ea.run_id AND er.shard_id=ea.shard_id AND ea.atom27_id BETWEEN er.atom27_start AND er.atom27_end WHERE ea.run_id='$RUN_ID' AND ea.shard_id=62 AND ea.is_e_atom)
INSERT INTO rb20_v2_5.e_members(run_id, contract_version, shard_id, ip_long, atom27_id, e_run_id)
SELECT '$RUN_ID','$CONTRACT', 62, r1.ip_long, r1.atom27_id, atr.e_run_id FROM r1 JOIN atom_to_run atr ON atr.atom27_id=r1.atom27_id;

DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_07' AND shard_id=62;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric) SELECT '$RUN_ID','$CONTRACT','RB20_07', 62, 'e_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.e_members WHERE run_id='$RUN_ID' AND shard_id=62;
"

log ">>> [1/4] Shard 62: RB20_08 F Members..."
run_sql "
DELETE FROM rb20_v2_5.f_members WHERE run_id='$RUN_ID' AND shard_id=62;
INSERT INTO rb20_v2_5.f_members(run_id, contract_version, shard_id, ip_long, atom27_id)
SELECT '$RUN_ID','$CONTRACT', 62, r1.ip_long, r1.atom27_id
FROM rb20_v2_5.r1_members r1
LEFT JOIN rb20_v2_5.e_atoms ea ON ea.run_id=r1.run_id AND ea.shard_id=r1.shard_id AND ea.atom27_id=r1.atom27_id AND ea.is_e_atom
WHERE r1.run_id='$RUN_ID' AND r1.shard_id=62 AND ea.atom27_id IS NULL;
DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_08' AND shard_id=62;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric) SELECT '$RUN_ID','$CONTRACT','RB20_08', 62, 'f_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.f_members WHERE run_id='$RUN_ID' AND shard_id=62;
"
log ">>> [1/4] Shard 62 完成!"

# ===== Shard 63: 完整 RB20_06 → 07 → 08 =====
log ""
log ">>> [2/4] Shard 63: RB20_06 R1 Members..."
run_sql "
DELETE FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=63;
INSERT INTO rb20_v2_5.r1_members(run_id, contract_version, shard_id, ip_long, atom27_id, block_id_natural, block_id_final)
SELECT '$RUN_ID','$CONTRACT', 63, km.ip_long, sm.atom27_id, km.block_id_natural, mf.block_id_final
FROM rb20_v2_5.keep_members km
JOIN rb20_v2_5.source_members sm ON sm.run_id=km.run_id AND sm.shard_id=km.shard_id AND sm.ip_long=km.ip_long
LEFT JOIN rb20_v2_5.map_member_block_final mf ON mf.run_id=km.run_id AND mf.shard_id=km.shard_id AND mf.ip_long=km.ip_long
WHERE km.run_id='$RUN_ID' AND km.shard_id=63
  AND NOT EXISTS (SELECT 1 FROM rb20_v2_5.h_members hm WHERE hm.run_id=km.run_id AND hm.ip_long=km.ip_long);
DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_06' AND shard_id=63;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
SELECT '$RUN_ID','$CONTRACT','RB20_06', 63, 'r1_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=63;
"

log ">>> [2/4] Shard 63: RB20_07 E Atoms/Runs/Members..."
run_sql "
DELETE FROM rb20_v2_5.e_members WHERE run_id='$RUN_ID' AND shard_id=63;
DELETE FROM rb20_v2_5.e_runs WHERE run_id='$RUN_ID' AND shard_id=63;
DELETE FROM rb20_v2_5.e_atoms WHERE run_id='$RUN_ID' AND shard_id=63;

WITH r1 AS (SELECT ip_long, atom27_id FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=63),
r1v AS (SELECT r1.atom27_id, sm.is_valid FROM r1 JOIN rb20_v2_5.source_members sm ON sm.run_id='$RUN_ID' AND sm.shard_id=63 AND sm.ip_long=r1.ip_long),
agg AS (SELECT atom27_id, COUNT(*) FILTER (WHERE is_valid)::integer AS valid_ip_cnt FROM r1v GROUP BY 1)
INSERT INTO rb20_v2_5.e_atoms(run_id, contract_version, shard_id, atom27_id, ip_start, ip_end, valid_ip_cnt, atom_density, is_e_atom)
SELECT '$RUN_ID','$CONTRACT', 63, atom27_id, (atom27_id * 32)::bigint, (atom27_id * 32 + 31)::bigint, valid_ip_cnt, (valid_ip_cnt::numeric / 32.0), (valid_ip_cnt >= 7) FROM agg;

WITH ea AS (SELECT atom27_id FROM rb20_v2_5.e_atoms WHERE run_id='$RUN_ID' AND shard_id=63 AND is_e_atom),
seq AS (SELECT atom27_id, (atom27_id - ROW_NUMBER() OVER (ORDER BY atom27_id)) AS grp FROM ea),
runs AS (SELECT MIN(atom27_id)::bigint AS atom27_start, MAX(atom27_id)::bigint AS atom27_end, COUNT(*)::integer AS run_len FROM seq GROUP BY grp)
INSERT INTO rb20_v2_5.e_runs(run_id, contract_version, shard_id, e_run_id, atom27_start, atom27_end, run_len, short_run, ip_start, ip_end)
SELECT '$RUN_ID','$CONTRACT', 63, ('E63_' || atom27_start::text || '_' || atom27_end::text), atom27_start, atom27_end, run_len, (run_len < 3), (atom27_start * 32)::bigint, (atom27_end * 32 + 31)::bigint FROM runs;

WITH r1 AS (SELECT ip_long, atom27_id FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=63),
atom_to_run AS (SELECT ea.atom27_id, er.e_run_id FROM rb20_v2_5.e_atoms ea JOIN rb20_v2_5.e_runs er ON er.run_id=ea.run_id AND er.shard_id=ea.shard_id AND ea.atom27_id BETWEEN er.atom27_start AND er.atom27_end WHERE ea.run_id='$RUN_ID' AND ea.shard_id=63 AND ea.is_e_atom)
INSERT INTO rb20_v2_5.e_members(run_id, contract_version, shard_id, ip_long, atom27_id, e_run_id)
SELECT '$RUN_ID','$CONTRACT', 63, r1.ip_long, r1.atom27_id, atr.e_run_id FROM r1 JOIN atom_to_run atr ON atr.atom27_id=r1.atom27_id;

DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_07' AND shard_id=63;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric) SELECT '$RUN_ID','$CONTRACT','RB20_07', 63, 'e_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.e_members WHERE run_id='$RUN_ID' AND shard_id=63;
"

log ">>> [2/4] Shard 63: RB20_08 F Members..."
run_sql "
DELETE FROM rb20_v2_5.f_members WHERE run_id='$RUN_ID' AND shard_id=63;
INSERT INTO rb20_v2_5.f_members(run_id, contract_version, shard_id, ip_long, atom27_id)
SELECT '$RUN_ID','$CONTRACT', 63, r1.ip_long, r1.atom27_id
FROM rb20_v2_5.r1_members r1
LEFT JOIN rb20_v2_5.e_atoms ea ON ea.run_id=r1.run_id AND ea.shard_id=r1.shard_id AND ea.atom27_id=r1.atom27_id AND ea.is_e_atom
WHERE r1.run_id='$RUN_ID' AND r1.shard_id=63 AND ea.atom27_id IS NULL;
DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_08' AND shard_id=63;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric) SELECT '$RUN_ID','$CONTRACT','RB20_08', 63, 'f_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.f_members WHERE run_id='$RUN_ID' AND shard_id=63;
"
log ">>> [2/4] Shard 63 完成!"

# ===== Shard 55: 仅 RB20_07 → 08 =====
log ""
log ">>> [3/4] Shard 55: RB20_07 E Atoms/Runs/Members..."
run_sql "
DELETE FROM rb20_v2_5.e_members WHERE run_id='$RUN_ID' AND shard_id=55;
DELETE FROM rb20_v2_5.e_runs WHERE run_id='$RUN_ID' AND shard_id=55;
DELETE FROM rb20_v2_5.e_atoms WHERE run_id='$RUN_ID' AND shard_id=55;

WITH r1 AS (SELECT ip_long, atom27_id FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=55),
r1v AS (SELECT r1.atom27_id, sm.is_valid FROM r1 JOIN rb20_v2_5.source_members sm ON sm.run_id='$RUN_ID' AND sm.shard_id=55 AND sm.ip_long=r1.ip_long),
agg AS (SELECT atom27_id, COUNT(*) FILTER (WHERE is_valid)::integer AS valid_ip_cnt FROM r1v GROUP BY 1)
INSERT INTO rb20_v2_5.e_atoms(run_id, contract_version, shard_id, atom27_id, ip_start, ip_end, valid_ip_cnt, atom_density, is_e_atom)
SELECT '$RUN_ID','$CONTRACT', 55, atom27_id, (atom27_id * 32)::bigint, (atom27_id * 32 + 31)::bigint, valid_ip_cnt, (valid_ip_cnt::numeric / 32.0), (valid_ip_cnt >= 7) FROM agg;

WITH ea AS (SELECT atom27_id FROM rb20_v2_5.e_atoms WHERE run_id='$RUN_ID' AND shard_id=55 AND is_e_atom),
seq AS (SELECT atom27_id, (atom27_id - ROW_NUMBER() OVER (ORDER BY atom27_id)) AS grp FROM ea),
runs AS (SELECT MIN(atom27_id)::bigint AS atom27_start, MAX(atom27_id)::bigint AS atom27_end, COUNT(*)::integer AS run_len FROM seq GROUP BY grp)
INSERT INTO rb20_v2_5.e_runs(run_id, contract_version, shard_id, e_run_id, atom27_start, atom27_end, run_len, short_run, ip_start, ip_end)
SELECT '$RUN_ID','$CONTRACT', 55, ('E55_' || atom27_start::text || '_' || atom27_end::text), atom27_start, atom27_end, run_len, (run_len < 3), (atom27_start * 32)::bigint, (atom27_end * 32 + 31)::bigint FROM runs;

WITH r1 AS (SELECT ip_long, atom27_id FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=55),
atom_to_run AS (SELECT ea.atom27_id, er.e_run_id FROM rb20_v2_5.e_atoms ea JOIN rb20_v2_5.e_runs er ON er.run_id=ea.run_id AND er.shard_id=ea.shard_id AND ea.atom27_id BETWEEN er.atom27_start AND er.atom27_end WHERE ea.run_id='$RUN_ID' AND ea.shard_id=55 AND ea.is_e_atom)
INSERT INTO rb20_v2_5.e_members(run_id, contract_version, shard_id, ip_long, atom27_id, e_run_id)
SELECT '$RUN_ID','$CONTRACT', 55, r1.ip_long, r1.atom27_id, atr.e_run_id FROM r1 JOIN atom_to_run atr ON atr.atom27_id=r1.atom27_id;

DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_07' AND shard_id=55;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric) SELECT '$RUN_ID','$CONTRACT','RB20_07', 55, 'e_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.e_members WHERE run_id='$RUN_ID' AND shard_id=55;
"

log ">>> [3/4] Shard 55: RB20_08 F Members..."
run_sql "
DELETE FROM rb20_v2_5.f_members WHERE run_id='$RUN_ID' AND shard_id=55;
INSERT INTO rb20_v2_5.f_members(run_id, contract_version, shard_id, ip_long, atom27_id)
SELECT '$RUN_ID','$CONTRACT', 55, r1.ip_long, r1.atom27_id
FROM rb20_v2_5.r1_members r1
LEFT JOIN rb20_v2_5.e_atoms ea ON ea.run_id=r1.run_id AND ea.shard_id=r1.shard_id AND ea.atom27_id=r1.atom27_id AND ea.is_e_atom
WHERE r1.run_id='$RUN_ID' AND r1.shard_id=55 AND ea.atom27_id IS NULL;
DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_08' AND shard_id=55;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric) SELECT '$RUN_ID','$CONTRACT','RB20_08', 55, 'f_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.f_members WHERE run_id='$RUN_ID' AND shard_id=55;
"
log ">>> [3/4] Shard 55 完成!"

# ===== Shard 61: 仅 RB20_07 → 08 =====
log ""
log ">>> [4/4] Shard 61: RB20_07 E Atoms/Runs/Members..."
run_sql "
DELETE FROM rb20_v2_5.e_members WHERE run_id='$RUN_ID' AND shard_id=61;
DELETE FROM rb20_v2_5.e_runs WHERE run_id='$RUN_ID' AND shard_id=61;
DELETE FROM rb20_v2_5.e_atoms WHERE run_id='$RUN_ID' AND shard_id=61;

WITH r1 AS (SELECT ip_long, atom27_id FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=61),
r1v AS (SELECT r1.atom27_id, sm.is_valid FROM r1 JOIN rb20_v2_5.source_members sm ON sm.run_id='$RUN_ID' AND sm.shard_id=61 AND sm.ip_long=r1.ip_long),
agg AS (SELECT atom27_id, COUNT(*) FILTER (WHERE is_valid)::integer AS valid_ip_cnt FROM r1v GROUP BY 1)
INSERT INTO rb20_v2_5.e_atoms(run_id, contract_version, shard_id, atom27_id, ip_start, ip_end, valid_ip_cnt, atom_density, is_e_atom)
SELECT '$RUN_ID','$CONTRACT', 61, atom27_id, (atom27_id * 32)::bigint, (atom27_id * 32 + 31)::bigint, valid_ip_cnt, (valid_ip_cnt::numeric / 32.0), (valid_ip_cnt >= 7) FROM agg;

WITH ea AS (SELECT atom27_id FROM rb20_v2_5.e_atoms WHERE run_id='$RUN_ID' AND shard_id=61 AND is_e_atom),
seq AS (SELECT atom27_id, (atom27_id - ROW_NUMBER() OVER (ORDER BY atom27_id)) AS grp FROM ea),
runs AS (SELECT MIN(atom27_id)::bigint AS atom27_start, MAX(atom27_id)::bigint AS atom27_end, COUNT(*)::integer AS run_len FROM seq GROUP BY grp)
INSERT INTO rb20_v2_5.e_runs(run_id, contract_version, shard_id, e_run_id, atom27_start, atom27_end, run_len, short_run, ip_start, ip_end)
SELECT '$RUN_ID','$CONTRACT', 61, ('E61_' || atom27_start::text || '_' || atom27_end::text), atom27_start, atom27_end, run_len, (run_len < 3), (atom27_start * 32)::bigint, (atom27_end * 32 + 31)::bigint FROM runs;

WITH r1 AS (SELECT ip_long, atom27_id FROM rb20_v2_5.r1_members WHERE run_id='$RUN_ID' AND shard_id=61),
atom_to_run AS (SELECT ea.atom27_id, er.e_run_id FROM rb20_v2_5.e_atoms ea JOIN rb20_v2_5.e_runs er ON er.run_id=ea.run_id AND er.shard_id=ea.shard_id AND ea.atom27_id BETWEEN er.atom27_start AND er.atom27_end WHERE ea.run_id='$RUN_ID' AND ea.shard_id=61 AND ea.is_e_atom)
INSERT INTO rb20_v2_5.e_members(run_id, contract_version, shard_id, ip_long, atom27_id, e_run_id)
SELECT '$RUN_ID','$CONTRACT', 61, r1.ip_long, r1.atom27_id, atr.e_run_id FROM r1 JOIN atom_to_run atr ON atr.atom27_id=r1.atom27_id;

DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_07' AND shard_id=61;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric) SELECT '$RUN_ID','$CONTRACT','RB20_07', 61, 'e_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.e_members WHERE run_id='$RUN_ID' AND shard_id=61;
"

log ">>> [4/4] Shard 61: RB20_08 F Members..."
run_sql "
DELETE FROM rb20_v2_5.f_members WHERE run_id='$RUN_ID' AND shard_id=61;
INSERT INTO rb20_v2_5.f_members(run_id, contract_version, shard_id, ip_long, atom27_id)
SELECT '$RUN_ID','$CONTRACT', 61, r1.ip_long, r1.atom27_id
FROM rb20_v2_5.r1_members r1
LEFT JOIN rb20_v2_5.e_atoms ea ON ea.run_id=r1.run_id AND ea.shard_id=r1.shard_id AND ea.atom27_id=r1.atom27_id AND ea.is_e_atom
WHERE r1.run_id='$RUN_ID' AND r1.shard_id=61 AND ea.atom27_id IS NULL;
DELETE FROM rb20_v2_5.step_stats WHERE run_id='$RUN_ID' AND step_id='RB20_08' AND shard_id=61;
INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric) SELECT '$RUN_ID','$CONTRACT','RB20_08', 61, 'f_member_cnt', COUNT(*)::numeric FROM rb20_v2_5.f_members WHERE run_id='$RUN_ID' AND shard_id=61;
"
log ">>> [4/4] Shard 61 完成!"

log ""
log "=========================================="
log "补数全部完成!"
log "=========================================="

# 验证
log ""
log "验证结果..."
run_sql "SELECT step_id, COUNT(DISTINCT shard_id) as completed FROM rb20_v2_5.step_stats WHERE shard_id >= 0 GROUP BY step_id ORDER BY step_id;"
