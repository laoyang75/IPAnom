import os
import subprocess
import time
from datetime import datetime

# ==========================================
# Configuration & Constants
# ==========================================

RUN_ID = os.getenv("RUN_ID", "rb20v2_20260107_194500_sg_001")
CONTRACT_VERSION = os.getenv("CONTRACT_VERSION", "contract_v1")
SHARD_CNT = int(os.getenv("SHARD_CNT", "64"))
SQL_FILE = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/00_contracts/10_shard_plan_generate_sql_only.sql"

# Database Connection Info
os.environ["PGHOST"] = "192.168.200.217"
os.environ["PGPORT"] = "5432"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "123456"
os.environ["PGDATABASE"] = "ip_loc2"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_cmd(cmd, shell=True):
    try:
        subprocess.check_output(cmd, shell=shell, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        log(f"ERROR executing command: {cmd}")
        log(f"Output: {e.output.decode('utf-8')}")
        raise e

def exec_sql_file_fix():
    if not os.path.exists(SQL_FILE):
        raise FileNotFoundError(f"SQL file not found: {SQL_FILE}")
    
    # Use binary hex literal for '中国' to avoid encoding issues
    SAFE_CHINA = "convert_from('\\xe4b8ade59bbd'::bytea, 'UTF8')"
    
    with open(SQL_FILE, 'r', encoding='utf-8') as f:
        content = f.read()
        
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_cnt}}": str(SHARD_CNT),
        "{{eps}}": "0.10",
        "'中国'": SAFE_CHINA
    }
        
    for k, v in replacements.items():
        content = content.replace(k, str(v))
        
    temp_filename = f"/tmp/rb20_fix_{int(time.time()*1000)}.sql"
    
    with open(temp_filename, 'w', encoding='utf-8') as f:
        f.write(content)
        
    try:
        log(f"Executing RB20_00D fix with RUN_ID={RUN_ID} (Hex Safe)...")
        run_cmd(f"psql -v ON_ERROR_STOP=1 -f {temp_filename}")
        log("Execution successful.")
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def verify():
    log("Verifying results...")
    cmd_cnt = f"psql -t -c \"SELECT COUNT(*) FROM rb20_v2_5.shard_plan WHERE run_id='{RUN_ID}';\""
    cnt = subprocess.check_output(cmd_cnt, shell=True).decode('utf-8').strip()
    
    cmd_empty = f"psql -t -c \"SELECT COUNT(*) FROM rb20_v2_5.shard_plan WHERE run_id='{RUN_ID}' AND COALESCE(est_rows,0)=0;\""
    empty = subprocess.check_output(cmd_empty, shell=True).decode('utf-8').strip()
    
    log(f"Shard Count: {cnt} (Expected: 64)")
    log(f"Empty Shards: {empty} (Expected: 0)")
    
    if cnt and int(cnt) == 64 and empty and int(empty) == 0:
        log("VALIDATION PASS.")
    else:
        log("VALIDATION FAIL.")
        exit(1)

if __name__ == "__main__":
    exec_sql_file_fix()
    verify()
