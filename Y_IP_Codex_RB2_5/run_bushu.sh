#!/bin/bash
# RB20 v2.5 补数脚本 - 后台运行
# 补跑 4 个未完成分片: 55, 61, 62, 63

set -e

RUN_ID="rb20v2_20260202_191900_sg_001"
CONTRACT="contract_v1"
DB_HOST="192.168.200.217"
DB_PORT="5432"
DB_NAME="ip_loc2"
DB_USER="postgres"
export PGPASSWORD="123456"

LOG_DIR="$(dirname "$0")/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/bushu_$(date +%Y%m%d_%H%M%S).log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

run_sql() {
    local sql="$1"
    psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" -c "$sql" 2>&1 | tee -a "$LOG_FILE"
}

run_sql_file() {
    local file="$1"
    local shard="$2"
    # 替换占位符
    sed -e "s/{{run_id}}/$RUN_ID/g" \
        -e "s/{{contract_version}}/$CONTRACT/g" \
        -e "s/{{shard_id}}/$shard/g" "$file" | \
    psql -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -U "$DB_USER" 2>&1 | tee -a "$LOG_FILE"
}

SQL_DIR="$(dirname "$0")/03_sql"

log "=========================================="
log "RB20 v2.5 补数开始"
log "Run ID: $RUN_ID"
log "待补分片: 62, 63 (完整 06→07→08)"
log "待补分片: 55, 61 (07→08)"
log "=========================================="

# Shard 62: 完整 RB20_06 → 07 → 08
log ""
log ">>> [1/4] Shard 62: RB20_06 R1 Members..."
run_sql_file "$SQL_DIR/RB20_06/06_r1_members_shard.sql" 62
log ">>> [1/4] Shard 62: RB20_07 E Atoms/Runs/Members..."
run_sql_file "$SQL_DIR/RB20_07/07_e_atoms_runs_members_shard.sql" 62
log ">>> [1/4] Shard 62: RB20_08 F Members..."
run_sql_file "$SQL_DIR/RB20_08/08_f_members_shard.sql" 62
log ">>> [1/4] Shard 62 完成!"

# Shard 63: 完整 RB20_06 → 07 → 08
log ""
log ">>> [2/4] Shard 63: RB20_06 R1 Members..."
run_sql_file "$SQL_DIR/RB20_06/06_r1_members_shard.sql" 63
log ">>> [2/4] Shard 63: RB20_07 E Atoms/Runs/Members..."
run_sql_file "$SQL_DIR/RB20_07/07_e_atoms_runs_members_shard.sql" 63
log ">>> [2/4] Shard 63: RB20_08 F Members..."
run_sql_file "$SQL_DIR/RB20_08/08_f_members_shard.sql" 63
log ">>> [2/4] Shard 63 完成!"

# Shard 55: 仅 RB20_07 → 08 (已有 r1_members)
log ""
log ">>> [3/4] Shard 55: RB20_07 E Atoms/Runs/Members..."
run_sql_file "$SQL_DIR/RB20_07/07_e_atoms_runs_members_shard.sql" 55
log ">>> [3/4] Shard 55: RB20_08 F Members..."
run_sql_file "$SQL_DIR/RB20_08/08_f_members_shard.sql" 55
log ">>> [3/4] Shard 55 完成!"

# Shard 61: 仅 RB20_07 → 08 (已有 r1_members)
log ""
log ">>> [4/4] Shard 61: RB20_07 E Atoms/Runs/Members..."
run_sql_file "$SQL_DIR/RB20_07/07_e_atoms_runs_members_shard.sql" 61
log ">>> [4/4] Shard 61: RB20_08 F Members..."
run_sql_file "$SQL_DIR/RB20_08/08_f_members_shard.sql" 61
log ">>> [4/4] Shard 61 完成!"

log ""
log "=========================================="
log "补数全部完成!"
log "=========================================="

# 验证结果
log ""
log "验证结果..."
run_sql "SELECT step_id, COUNT(DISTINCT shard_id) as completed FROM rb20_v2_5.step_stats WHERE shard_id >= 0 GROUP BY step_id ORDER BY step_id;"

log ""
log "日志文件: $LOG_FILE"
