
import os
import subprocess
import sys
import multiprocessing
import time
from datetime import datetime
from typing import List
import psycopg2

# ==========================================
# Configuration & Constants
# ==========================================

RUN_ID = "rb20v2_20260202_191900_sg_001"
CONTRACT_VERSION = "contract_v1"
SHARD_CNT = 64
CONCURRENCY = 32  # For per-shard SQL steps

# Database Connection Info
os.environ["PGHOST"] = "192.168.200.217"
os.environ["PGPORT"] = "5432"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "123456"
os.environ["PGDATABASE"] = "ip_loc2"

DB_CONFIG = {
    "host": os.environ["PGHOST"],
    "port": os.environ["PGPORT"],
    "database": os.environ["PGDATABASE"],
    "user": os.environ["PGUSER"],
    "password": os.environ["PGPASSWORD"]
}

# Base Paths
BASE_DIR = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql"
RUNBOOK_DIR = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/04_runbook"

# Optimization Scripts (Global)
SCRIPT_STEP03_OPT = f"{RUNBOOK_DIR}/orchestrate_step03_bucket_full.py"
SCRIPT_STEP11_OPT = f"{RUNBOOK_DIR}/orchestrate_step11_chunked.py"
SQL_STEP03_POST = f"{BASE_DIR}/RB20_03/03_post_process.sql"

# SQL Files Map
SQL_FILES = {
    # Phase 0: DDL (Optional, usually we assume struct exists, but user asked for "Complete Rerun". 
    # Warning: Re-running Full DDL might drop tables. We'll stick to TRUNCATE/DELETE by default 
    # unless we really want to rebuild schema. Let's rely on Run Init to clear data.)
    
    # Phase 1: Global Init
    "p1_00_run_init": f"{BASE_DIR}/00_contracts/00_run_init.sql",
    "p1_00_shard_plan": f"{BASE_DIR}/00_contracts/10_shard_plan_generate_sql_only.sql",
    "p1_01A_dedup": f"{BASE_DIR}/RB20_01/01A_abnormal_dedup.sql",

    # Phase 2: Per-Shard Pre-Opt
    "p2_01": f"{BASE_DIR}/RB20_01/01_source_members_shard.sql",
    "p2_02": f"{BASE_DIR}/RB20_02/02_natural_blocks_shard.sql",
    
    # Phase 5: Per-Shard Post-Opt
    "p5_04": f"{BASE_DIR}/RB20_04/04_split_and_final_blocks_shard.sql",
    "p5_04P": f"{BASE_DIR}/RB20_04P/04P_final_profile_shard.sql",

    # Phase 6: Global H
    "p6_h_global": f"{BASE_DIR}/RB20_05/05_h_blocks_and_members.sql",
    
    # Phase 7: Per-Shard Pipeline 2
    "p7_06": f"{BASE_DIR}/RB20_06/06_r1_members_shard.sql",
    "p7_07": f"{BASE_DIR}/RB20_07/07_e_atoms_runs_members_shard.sql",
    "p7_08": f"{BASE_DIR}/RB20_08/08_f_members_shard.sql",
    
    # Phase 8: QA
    "p8_qa": f"{BASE_DIR}/RB20_99/99_qa_assert.sql",
}

# ==========================================
# Helpers
# ==========================================

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

def run_cmd(cmd, shell=True):
    try:
        subprocess.check_output(cmd, shell=shell, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        log(f"ERROR executing command: {cmd}")
        log(f"Output: {e.output.decode('utf-8')}")
        raise e

def exec_sql_file(file_path, replacements, description=""):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    
    with open(file_path, 'r') as f:
        content = f.read()
        
    for k, v in replacements.items():
        content = content.replace(k, str(v))
        
    temp_filename = f"/tmp/rb20_exec_{int(time.time()*1000)}_{os.getpid()}.sql"
    with open(temp_filename, 'w') as f:
        f.write(content)
        
    try:
        # log(f"Executing {description}...")
        run_cmd(f"psql -v ON_ERROR_STOP=1 -f {temp_filename}")
    except Exception as e:
        log(f"FAILED: {description}")
        raise e
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def run_python_script(script_path):
    log(f"Running Python Script: {os.path.basename(script_path)}...")
    start_t = time.time()
    try:
        subprocess.check_call([sys.executable, script_path])
        log(f"Script finished in {time.time() - start_t:.2f}s.")
    except subprocess.CalledProcessError as e:
        log(f"Script FAILED with code {e.returncode}")
        sys.exit(1)

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def check_row_count(table, where_clause="1=1"):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM {table} WHERE {where_clause}")
    cnt = cur.fetchone()[0]
    conn.close()
    return cnt

# ==========================================
# Workers
# ==========================================

def worker_phase2(shard_id):
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_id}}": str(shard_id)
    }
    try:
        exec_sql_file(SQL_FILES["p2_01"], replacements, f"Shard {shard_id}: 01 Source Members")
        exec_sql_file(SQL_FILES["p2_02"], replacements, f"Shard {shard_id}: 02 Natural Blocks")
        return (True, shard_id, None)
    except Exception as e:
        return (False, shard_id, str(e))

def worker_phase5(shard_id):
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_id}}": str(shard_id)
    }
    try:
        exec_sql_file(SQL_FILES["p5_04"], replacements, f"Shard {shard_id}: 04 Split/Final")
        exec_sql_file(SQL_FILES["p5_04P"], replacements, f"Shard {shard_id}: 04P Final Profile")
        return (True, shard_id, None)
    except Exception as e:
        return (False, shard_id, str(e))

def worker_phase7(shard_id):
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_id}}": str(shard_id)
    }
    try:
        exec_sql_file(SQL_FILES["p7_06"], replacements, f"Shard {shard_id}: 06 R1 Members")
        exec_sql_file(SQL_FILES["p7_07"], replacements, f"Shard {shard_id}: 07 E Atoms")
        exec_sql_file(SQL_FILES["p7_08"], replacements, f"Shard {shard_id}: 08 F Members")
        return (True, shard_id, None)
    except Exception as e:
        return (False, shard_id, str(e))

def run_post_process_all_shards():
    log("Running 03_post_process.sql for all 64 shards...")
    conn = get_db_conn()
    cur = conn.cursor()
    with open(SQL_STEP03_POST, 'r') as f:
        template = f.read()
    
    for sid in range(SHARD_CNT):
        sql = template.replace("{{run_id}}", RUN_ID)\
                      .replace("{{contract_version}}", CONTRACT_VERSION)\
                      .replace("{{shard_id}}", str(sid))
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            log(f"Shard {sid} Post-Process Failed: {e}")
            conn.rollback()
            sys.exit(1)
    conn.close()
    log("Post-Processing Complete.")

# ==========================================
# Main
# ==========================================

def main():
    log(f"=== Starting COMPLETE FRESH START (Run: {RUN_ID}) ===")
    
    # ---------------------------
    # Phase 0: Explicit Cleanup
    # ---------------------------
    log("=== Phase 0: Explicit Cleanup ===")
    try:
        cleanup_script = f"{RUNBOOK_DIR}/run_cleanup.py"
        run_python_script(cleanup_script)
    except Exception as e:
        log(f"Cleanup warning: {e}")

    # ---------------------------
    # Phase 1: Global Init
    # ---------------------------
    log("=== Phase 1: Global Init (Run Init, Shard Plan, Dedup) ===")
    
    # Run Init (Clears data for run_id usually, resets stats)
    exec_sql_file(SQL_FILES["p1_00_run_init"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION
    }, "00 Run Init")
    
    # Shard Plan (Populates shard_plan table)
    exec_sql_file(SQL_FILES["p1_00_shard_plan"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_cnt}}": str(SHARD_CNT),
        "{{eps}}": "0.10"
    }, "00 Shard Plan")
    
    # Abnormal Dedup (Populates source_members)
    # Note: verify if it takes shard_cnt or just runs globally? usually global.
    # Looking at file list, '01A_abnormal_dedup.sql'.
    exec_sql_file(SQL_FILES["p1_01A_dedup"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION
    }, "01A Abnormal Dedup")
    
    count_sm = check_row_count("rb20_v2_5.source_members", f"run_id='{RUN_ID}'")
    log(f"Source Members Generated: {count_sm}")
    
    # ---------------------------
    # Phase 2: Per-Shard Pre-Opt (01, 02)
    # ---------------------------
    log("=== Phase 2: Per-Shard Pre-Optimization (Step 01, Step 02) ===")
    shards = list(range(SHARD_CNT))
    with multiprocessing.Pool(CONCURRENCY) as p:
        results = p.map(worker_phase2, shards)
    
    failures = [r for r in results if not r[0]]
    if failures:
        log(f"STOP: {len(failures)} shards failed in Phase 2.")
        sys.exit(1)
        
    count_blocks = check_row_count("rb20_v2_5.block_natural", f"run_id='{RUN_ID}'")
    log(f"Natural Blocks Generated: {count_blocks}")

    # ---------------------------
    # Phase 3: Optimized Step 03
    # ---------------------------
    log("=== Phase 3: Optimized Step 03 (Global) ===")
    run_python_script(SCRIPT_STEP03_OPT)
    
    log("--- Phase 3.5: Step 03 Post-Process ---")
    run_post_process_all_shards()
    
    count_pre = check_row_count("rb20_v2_5.profile_pre", f"run_id='{RUN_ID}'")
    log(f"Profile Pre Rows: {count_pre}")

    # ---------------------------
    # Phase 4: Optimized Step 11
    # ---------------------------
    log("=== Phase 4: Optimized Step 11 (Global Chunked) ===")
    run_python_script(SCRIPT_STEP11_OPT)
    
    count_win = check_row_count("rb20_v2_5.window_headtail_64", f"run_id='{RUN_ID}'")
    log(f"Window HeadTail Rows: {count_win}")

    # ---------------------------
    # Phase 5: Per-Shard Post-Opt (04, 04P)
    # ---------------------------
    log("=== Phase 5: Per-Shard Post-Optimization (Step 04, Step 04P) ===")
    with multiprocessing.Pool(CONCURRENCY) as p:
        results = p.map(worker_phase5, shards)
        
    if any(not r[0] for r in results):
        log("STOP: Failures in Phase 5.")
        sys.exit(1)

    # ---------------------------
    # Phase 6: Global H
    # ---------------------------
    log("=== Phase 6: Global H ===")
    exec_sql_file(SQL_FILES["p6_h_global"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_cnt}}": str(SHARD_CNT)
    }, "Global H")

    # ---------------------------
    # Phase 7: Per-Shard Pipeline 2 (06, 07, 08)
    # ---------------------------
    log("=== Phase 7: Per-Shard Pipeline 2 ===")
    with multiprocessing.Pool(CONCURRENCY) as p:
        results = p.map(worker_phase7, shards)
        
    if any(not r[0] for r in results):
        log("STOP: Failures in Phase 7.")
        sys.exit(1)

    # ---------------------------
    # Phase 8: QA
    # ---------------------------
    log("=== Phase 8: Final QA ===")
    exec_sql_file(SQL_FILES["p8_qa"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_cnt}}": str(SHARD_CNT)
    }, "QA Assert")
    
    log("=== FULL RERUN SUCCESS ===")

if __name__ == "__main__":
    main()
