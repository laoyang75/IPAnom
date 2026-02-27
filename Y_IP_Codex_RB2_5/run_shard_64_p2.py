import os
import subprocess
import sys
import time
from datetime import datetime

RUN_ID = "rb20v2_20260202_191900_sg_001"
CONTRACT_VERSION = "contract_v1"
SHARD_ID = 64
SHARD_CNT = 65

os.environ["PGHOST"] = "192.168.200.217"
os.environ["PGPORT"] = "5432"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "123456"
os.environ["PGDATABASE"] = "ip_loc2"

BASE_DIR = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql"

SQL_FILES = {
    "h_global": f"{BASE_DIR}/RB20_05/05_h_blocks_and_members.sql",
    "p2_06": f"{BASE_DIR}/RB20_06/06_r1_members_shard.sql",
    "p2_07": f"{BASE_DIR}/RB20_07/07_e_atoms_runs_members_shard.sql",
    "p2_08": f"{BASE_DIR}/RB20_08/08_f_members_shard.sql",
    "qa_assert": f"{BASE_DIR}/RB20_99/99_qa_assert.sql",
}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_cmd(cmd):
    try:
        subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        log(f"ERROR executing: {cmd}")
        log(f"Output: {e.output.decode('utf-8')}")
        raise e
        
def exec_sql_file(file_path, replacements, description):
    with open(file_path, 'r') as f:
        content = f.read()
    for k, v in replacements.items():
        content = content.replace(k, str(v))
    
    # Disable nestloop for safety
    content = "SET work_mem = '2GB';\nSET enable_nestloop = off;\n" + content
    
    temp = f"/tmp/rb20_exec_{int(time.time()*1000)}_{os.getpid()}.sql"
    with open(temp, 'w') as f:
        f.write(content)
    try:
        log(f"Executing {description}...")
        run_cmd(f"psql -v ON_ERROR_STOP=1 -f {temp} > /dev/null")
    except Exception as e:
        log(f"FAILED: {description}")
        raise e
    finally:
        if os.path.exists(temp):
            os.remove(temp)

def main():
    log(f"Starting Shard {SHARD_ID} P2 & QA. RUN_ID: {RUN_ID}")
    
    # 1. Global H
    exec_sql_file(SQL_FILES["h_global"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION
    }, "05_h_global")
    
    # 2. Pipeline 2 for Shard 64
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_id}}": str(SHARD_ID)
    }
    exec_sql_file(SQL_FILES["p2_06"], replacements, "06_r1_members")
    exec_sql_file(SQL_FILES["p2_07"], replacements, "07_e_atoms_runs")
    exec_sql_file(SQL_FILES["p2_08"], replacements, "08_f_members")
    
    # 3. Final QA
    log("=== Phase 7: Final QA ===")
    exec_sql_file(SQL_FILES["qa_assert"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_cnt}}": str(SHARD_CNT), # Now it's 65 to account for 0-64
    }, "QA Assert")
    
    cmd = f'psql -t -c "SELECT assert_name, severity, pass_flag, details FROM rb20_v2_5.qa_assert WHERE run_id=\'{RUN_ID}\' AND NOT pass_flag ORDER BY assert_name"'
    failed_qa = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
    
    if failed_qa:
        log("STOP: QA Assertions FAILED.")
        print(failed_qa)
        sys.exit(1)
    else:
        log("QA Assertions PASSED.")

    log(f"=== SUCCESS ===")

if __name__ == "__main__":
    main()
