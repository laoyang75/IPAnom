import os
import subprocess
import sys
import multiprocessing
import time
from datetime import datetime
from typing import List
from pathlib import Path

# ==========================================
# Configuration & Constants
# ==========================================

# Generated Run ID
RUN_ID = os.getenv("RUN_ID", "rb20v2_20260107_194500_sg_001")
CONTRACT_VERSION = os.getenv("CONTRACT_VERSION", "contract_v1")
SHARD_CNT = int(os.getenv("SHARD_CNT", "64"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "32"))

# Database Connection Info
os.environ["PGHOST"] = "192.168.200.217"
os.environ["PGPORT"] = "5432"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "123456"
os.environ["PGDATABASE"] = "ip_loc2"

# Base Paths
BASE_DIR = str(Path(__file__).resolve().parent.parent / "03_sql")

# SQL Files Map
SQL_FILES = {
    # Global Contracts (Section 2.2/2.3)
    "ddl": f"{BASE_DIR}/00_contracts/01_ddl_rb20_v2_full.sql",
    "indexes": f"{BASE_DIR}/00_contracts/02_indexes_rb20_v2.sql",
    "views": f"{BASE_DIR}/00_contracts/03_views_rb20_v2.sql",
    
    # Global Init (Section 3)
    "run_init": f"{BASE_DIR}/00_contracts/00_run_init.sql",
    "shard_plan": f"{BASE_DIR}/00_contracts/10_shard_plan_generate_sql_only.sql",
    "abnormal_dedup": f"{BASE_DIR}/RB20_01/01A_abnormal_dedup.sql",
    
    # Per-Shard Pipeline 1 (Section 4)
    "p1_01": f"{BASE_DIR}/RB20_01/01_source_members_shard.sql",
    "p1_02": f"{BASE_DIR}/RB20_02/02_natural_blocks_shard.sql",
    "p1_03": f"{BASE_DIR}/RB20_03/03_pre_profile_shard.sql",
    "p1_11": f"{BASE_DIR}/RB20_11/11_window_headtail_shard.sql",
    "p1_04": f"{BASE_DIR}/RB20_04/04_split_and_final_blocks_shard.sql",
    "p1_04P": f"{BASE_DIR}/RB20_04P/04P_final_profile_shard.sql",

    # Global H (Section 5)
    "h_global": f"{BASE_DIR}/RB20_05/05_h_blocks_and_members.sql",
    
    # Per-Shard Pipeline 2 (Section 6)
    "p2_06": f"{BASE_DIR}/RB20_06/06_r1_members_shard.sql",
    "p2_07": f"{BASE_DIR}/RB20_07/07_e_atoms_runs_members_shard.sql",
    "p2_08": f"{BASE_DIR}/RB20_08/08_f_members_shard.sql",
    
    # Final QA (Section 7)
    "qa_assert": f"{BASE_DIR}/RB20_99/99_qa_assert.sql",
}

# ==========================================
# Helper Functions
# ==========================================

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_cmd(cmd, shell=True):
    try:
        subprocess.check_output(cmd, shell=shell, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        log(f"ERROR executing command: {cmd}")
        log(f"Output: {e.output.decode('utf-8')}")
        raise e

def query_lines(sql: str) -> List[str]:
    cmd = f'psql -t -A -c "{sql}"'
    out = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
    if not out:
        return []
    return [line.strip() for line in out.splitlines() if line.strip()]

def query_ints(sql: str) -> List[int]:
    return [int(x) for x in query_lines(sql)]

def exec_sql_file(file_path, replacements, description=""):
    """
    Reads SQL file, performs replacements, writes to temp file, executes, and cleans up.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    for k, v in replacements.items():
        content = content.replace(k, str(v))
        
    # Create temp execution file
    temp_filename = f"/tmp/rb20_exec_{int(time.time()*1000)}_{os.getpid()}.sql"
    with open(temp_filename, 'w', encoding='utf-8') as f:
        f.write(content)
        
    try:
        log(f"Executing {description}...")
        # using -v ON_ERROR_STOP=1 to ensure we catch errors
        run_cmd(f"psql -v ON_ERROR_STOP=1 -f {temp_filename}")
    except Exception as e:
        log(f"FAILED: {description}")
        raise e
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def check_query_has_rows(sql, error_msg):
    """
    Returns True if query returns > 0 rows.
    """
    cmd = f'psql -t -c "{sql}"'
    try:
        output = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if not output: 
            return False
        return bool(output)
    except Exception as e:
        log(f"Query check failed: {e}")
        raise e

def check_query_value(sql):
    cmd = f'psql -t -c "{sql}"'
    return subprocess.check_output(cmd, shell=True).decode('utf-8').strip()

# ==========================================
# Worker Functions
# ==========================================

def worker_pipeline_1(shard_id):
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_id}}": str(shard_id)
    }
    
    try:
        exec_sql_file(SQL_FILES["p1_01"], replacements, f"Shard {shard_id}: 01 Source Members")
        exec_sql_file(SQL_FILES["p1_02"], replacements, f"Shard {shard_id}: 02 Natural Blocks")
        exec_sql_file(SQL_FILES["p1_03"], replacements, f"Shard {shard_id}: 03 Pre Profile")
        exec_sql_file(SQL_FILES["p1_11"], replacements, f"Shard {shard_id}: 11 Window")
        exec_sql_file(SQL_FILES["p1_04"], replacements, f"Shard {shard_id}: 04 Split/Final")
        exec_sql_file(SQL_FILES["p1_04P"], replacements, f"Shard {shard_id}: 04P Final Profile")
        return (True, shard_id, None)
    except Exception as e:
        return (False, shard_id, str(e))

def worker_pipeline_2(shard_id):
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_id}}": str(shard_id)
    }
    
    try:
        exec_sql_file(SQL_FILES["p2_06"], replacements, f"Shard {shard_id}: 06 R1 Members")
        exec_sql_file(SQL_FILES["p2_07"], replacements, f"Shard {shard_id}: 07 E Atoms/Runs")
        exec_sql_file(SQL_FILES["p2_08"], replacements, f"Shard {shard_id}: 08 F Members")
        return (True, shard_id, None)
    except Exception as e:
        return (False, shard_id, str(e))

# ==========================================
# Main Orchestration logic
# ==========================================

def main():
    log(f"Starting Runbook Execution. RUN_ID: {RUN_ID}")
    
    start_time = time.time()
    
    # ------------------------------------
    # 2) Pre-checks & Contracts
    # ------------------------------------
    log("=== Phase 2: Pre-checks & Contracts ===")
    
    # 2.1 Schema Check
    exists = check_query_has_rows(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name='rb20_v2_5';", 
        "Schema rb20_v2_5 not found"
    )
    if not exists:
        log("STOP: Schema rb20_v2_5 missing.")
        sys.exit(1)
        
    # 2.2 Re-apply DDL/Indexes (Idempotent)
    exec_sql_file(SQL_FILES["ddl"], {}, "2.2 DDL")
    exec_sql_file(SQL_FILES["indexes"], {}, "2.2 Indexes")
    
    # 2.3 Views
    exec_sql_file(SQL_FILES["views"], {}, "2.3 Views")
    
    # ------------------------------------
    # 3) Global Steps
    # ------------------------------------
    log("=== Phase 3: Global Steps initialization ===")
    
    # 3.1 Run Init
    exec_sql_file(SQL_FILES["run_init"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION
    }, "3.1 Run Init")
    
    # Verify 3.1
    if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.run_meta WHERE run_id='{RUN_ID}'", "Run Init Failed"):
        log("STOP: Run Init verification failed.")
        sys.exit(1)
        
    # 3.2 Shard Plan
    exec_sql_file(SQL_FILES["shard_plan"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_cnt}}": str(SHARD_CNT),
        "{{eps}}": "0.10"
    }, "3.2 Shard Plan")
    
    # Verify 3.2
    shard_cnt = check_query_value(f"SELECT COUNT(*) FROM rb20_v2_5.shard_plan WHERE run_id='{RUN_ID}'")
    if int(shard_cnt) != SHARD_CNT:
        log(f"STOP: Shard count is {shard_cnt}, expected {SHARD_CNT}.")
        sys.exit(1)
        
    # 3.3 Abnormal Dedup
    exec_sql_file(SQL_FILES["abnormal_dedup"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION
    }, "3.3 Abnormal Dedup")
    
    # Verify 3.3
    if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND step_id='RB20_01A'", "Abnormal Dedup Failed"):
         log("STOP: Abnormal Dedup verification failed.")
         sys.exit(1)

    # ------------------------------------
    # 4) Per-shard Pipeline 1 (01 -> 04P)
    # ------------------------------------
    log("=== Phase 4: Per-shard Pipeline 1 (Concurrency: 32) ===")
    
    shards = query_ints(f"SELECT shard_id::int FROM rb20_v2_5.shard_plan WHERE run_id='{RUN_ID}' ORDER BY shard_id")
    if len(shards) != SHARD_CNT:
        log(f"STOP: shard_plan returned {len(shards)} shard_ids, expected {SHARD_CNT}.")
        sys.exit(1)
    
    with multiprocessing.Pool(processes=CONCURRENCY) as pool:
        results = pool.map(worker_pipeline_1, shards)
        
    # Check results
    failures = [r for r in results if not r[0]]
    if failures:
        log(f"STOP: {len(failures)} shards failed in Pipeline 1.")
        for f in failures:
            log(f"Shard {f[1]} Error: {f[2]}")
        sys.exit(1)
        
    # Verify completion (4.1)
    missing_source = check_query_has_rows(
        f"SELECT sp.shard_id FROM rb20_v2_5.shard_plan sp WHERE sp.run_id='{RUN_ID}' AND NOT EXISTS (SELECT 1 FROM rb20_v2_5.source_members sm WHERE sm.run_id=sp.run_id AND sm.shard_id=sp.shard_id)", 
        ""
    )
    if missing_source:
        log("STOP: Missing Source Members shards.")
        sys.exit(1)
        
    missing_final = check_query_has_rows(
        f"SELECT sp.shard_id FROM rb20_v2_5.shard_plan sp WHERE sp.run_id='{RUN_ID}' AND NOT EXISTS (SELECT 1 FROM rb20_v2_5.profile_final pf WHERE pf.run_id=sp.run_id AND pf.shard_id=sp.shard_id)", 
        ""
    )
    if missing_final:
        log("STOP: Missing Final Profile shards.")
        sys.exit(1)

    # ------------------------------------
    # 5) Global H
    # ------------------------------------
    log("=== Phase 5: Global H ===")
    
    exec_sql_file(SQL_FILES["h_global"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION
    }, "5.1 H Blocks & Members")
    
    # Verify 5.1
    if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.core_numbers WHERE run_id='{RUN_ID}' AND metric_name='h_block_cnt'", "H Global Failed"):
        log("STOP: H Global verification failed.")
        sys.exit(1)

    # ------------------------------------
    # 6) Per-shard Pipeline 2 (06 -> 08)
    # ------------------------------------
    log("=== Phase 6: Per-shard Pipeline 2 (Concurrency: 32) ===")
    
    with multiprocessing.Pool(processes=CONCURRENCY) as pool:
        results = pool.map(worker_pipeline_2, shards)
        
    failures = [r for r in results if not r[0]]
    if failures:
        log(f"STOP: {len(failures)} shards failed in Pipeline 2.")
        for f in failures:
            log(f"Shard {f[1]} Error: {f[2]}")
        sys.exit(1)
        
    # Verify completion (6.1)
    missing_e = check_query_has_rows(
        f"SELECT sp.shard_id FROM rb20_v2_5.shard_plan sp WHERE sp.run_id='{RUN_ID}' AND NOT EXISTS (SELECT 1 FROM rb20_v2_5.e_members em WHERE em.run_id=sp.run_id AND em.shard_id=sp.shard_id)", 
        ""
    )
    if missing_e:
        log("STOP: Missing E Members shards.")
        sys.exit(1)
        
    missing_f = check_query_has_rows(
        f"SELECT sp.shard_id FROM rb20_v2_5.shard_plan sp WHERE sp.run_id='{RUN_ID}' AND NOT EXISTS (SELECT 1 FROM rb20_v2_5.f_members fm WHERE fm.run_id=sp.run_id AND fm.shard_id=sp.shard_id)", 
        ""
    )
    if missing_f:
        log("STOP: Missing F Members shards.")
        sys.exit(1)

    # ------------------------------------
    # 7) Final QA
    # ------------------------------------
    log("=== Phase 7: Final QA ===")
    
    exec_sql_file(SQL_FILES["qa_assert"], {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_cnt}}": str(SHARD_CNT),
    }, "7.1 QA Assert")
    
    # Verify 7.1
    failed_qa = check_query_has_rows(f"SELECT * FROM rb20_v2_5.qa_assert WHERE run_id='{RUN_ID}' AND NOT pass_flag", "")
    if failed_qa:
        log("STOP: QA Assertions FAILED.")
        run_cmd(f'psql -c "SELECT assert_name, severity, pass_flag, details FROM rb20_v2_5.qa_assert WHERE run_id=\'{RUN_ID}\' AND NOT pass_flag ORDER BY assert_name"')
        sys.exit(1)
    else:
        log("QA Assertions PASSED.")

    # ------------------------------------
    # Done
    # ------------------------------------
    end_time = time.time()
    log(f"=== SUCCESS ===")
    log(f"Run ID: {RUN_ID}")
    log(f"Total Time: {int(end_time - start_time)} seconds")

if __name__ == "__main__":
    main()
