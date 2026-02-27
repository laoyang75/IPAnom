import os
import subprocess
import sys
import multiprocessing
import time
from datetime import datetime

RUN_ID = "rb20v2_20260202_191900_sg_001"
CONTRACT_VERSION = "contract_v1"
SHARD_ID = 64

os.environ["PGHOST"] = "192.168.200.217"
os.environ["PGPORT"] = "5432"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "123456"
os.environ["PGDATABASE"] = "ip_loc2"

BASE_DIR = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql"

SQL_FILES = {
    "p1_01": f"{BASE_DIR}/RB20_01/01_source_members_shard.sql",
    "p1_02": f"{BASE_DIR}/RB20_02/02_natural_blocks_shard.sql",
    "p1_03": f"{BASE_DIR}/RB20_03/03_pre_profile_shard.sql",
    "p1_11": f"{BASE_DIR}/RB20_11/11_window_headtail_shard.sql",
    "p1_04": f"{BASE_DIR}/RB20_04/04_split_and_final_blocks_shard.sql",
    "p1_04P": f"{BASE_DIR}/RB20_04P/04P_final_profile_shard.sql",
    "p2_06": f"{BASE_DIR}/RB20_06/06_r1_members_shard.sql",
    "p2_07": f"{BASE_DIR}/RB20_07/07_e_atoms_runs_members_shard.sql",
    "p2_08": f"{BASE_DIR}/RB20_08/08_f_members_shard.sql",
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
    
    # Disable nestloop to avoid query planner deadlock for massive blocks
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
    log(f"Starting Shard {SHARD_ID} full pipeline. RUN_ID: {RUN_ID}")
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_id}}": str(SHARD_ID)
    }
    try:
        # Pipeline 1
        exec_sql_file(SQL_FILES["p1_01"], replacements, "01_source_members")
        exec_sql_file(SQL_FILES["p1_02"], replacements, "02_natural_blocks")
        exec_sql_file(SQL_FILES["p1_03"], replacements, "03_pre_profile")
        exec_sql_file(SQL_FILES["p1_11"], replacements, "11_window_headtail")
        exec_sql_file(SQL_FILES["p1_04"], replacements, "04_split_finally")
        exec_sql_file(SQL_FILES["p1_04P"], replacements, "04P_final_profile")
        
        # Add a quick mapping update for global tables missing this shard (H members)
        # Note: actually h_members runs per block, but we need to insert it for shard 64 if there's any H inside
        # Actually H is global phase 5. Let's run a partial H for this shard just in case.
        # But Phase 5 is global... we assume it's global and we will re-run it fully (it's fast).
        
        # Pipeline 2
        exec_sql_file(SQL_FILES["p2_06"], replacements, "06_r1_members")
        exec_sql_file(SQL_FILES["p2_07"], replacements, "07_e_atoms_runs")
        exec_sql_file(SQL_FILES["p2_08"], replacements, "08_f_members")
        
        log(f"=== Shard {SHARD_ID} Pipeline completed ===")
    except Exception as e:
        log(f"STOP: Pipeline failed for Shard {SHARD_ID}")
        sys.exit(1)

if __name__ == "__main__":
    main()
