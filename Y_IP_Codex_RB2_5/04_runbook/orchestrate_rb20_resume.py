import os
import subprocess
import sys
import multiprocessing
import time
from datetime import datetime

# ==========================================
# Configuration & Constants
# ==========================================

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
os.environ["PGCLIENTENCODING"] = "UTF8"

# Base Paths
BASE_DIR = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql"

# SQL Files Map
SQL_FILES = {
    "run_init": f"{BASE_DIR}/00_contracts/00_run_init.sql",
    "shard_plan": f"{BASE_DIR}/00_contracts/10_shard_plan_generate_sql_only.sql",
    "abnormal_dedup": f"{BASE_DIR}/RB20_01/01A_abnormal_dedup.sql",
    "p1_01": f"{BASE_DIR}/RB20_01/01_source_members_shard.sql",
    "p1_02": f"{BASE_DIR}/RB20_02/02_natural_blocks_shard.sql",
    "p1_03": f"{BASE_DIR}/RB20_03/03_pre_profile_shard.sql",
    "p1_11": f"{BASE_DIR}/RB20_11/11_window_headtail_shard.sql",
    "p1_04": f"{BASE_DIR}/RB20_04/04_split_and_final_blocks_shard.sql",
    "p1_04P": f"{BASE_DIR}/RB20_04P/04P_final_profile_shard.sql",
    "h_global": f"{BASE_DIR}/RB20_05/05_h_blocks_and_members.sql",
    "p2_06": f"{BASE_DIR}/RB20_06/06_r1_members_shard.sql",
    "p2_07": f"{BASE_DIR}/RB20_07/07_e_atoms_runs_members_shard.sql",
    "p2_08": f"{BASE_DIR}/RB20_08/08_f_members_shard.sql",
    "qa_assert": f"{BASE_DIR}/RB20_99/99_qa_assert.sql",
}

# ==========================================
# Helper Functions
# ==========================================

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

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
    
    SAFE_CHINA = "convert_from('\\xe4b8ade59bbd'::bytea, 'UTF8')"
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    replacements["'中国'"] = SAFE_CHINA
    for k, v in replacements.items():
        content = content.replace(k, str(v))
    
    # Prepend performance optimizations
    optimized_content = """
SET work_mem = '1GB';
SET enable_nestloop = off;
SET enable_hashagg = on;
SET enable_hashjoin = on;
SET enable_mergejoin = on;
""" + content
        
    temp_filename = f"/tmp/rb20_exec_{int(time.time()*1000)}_{os.getpid()}.sql"
    with open(temp_filename, 'w', encoding='utf-8') as f:
        f.write(optimized_content)
        
    try:
        run_cmd(f"psql -X -v ON_ERROR_STOP=1 -f {temp_filename}")
    except Exception as e:
        log(f"FAILED: {description}")
        raise e
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def get_all_shards():
    sql = f"SELECT shard_id FROM rb20_v2_5.shard_plan WHERE run_id='{RUN_ID}'"
    cmd = f'psql -X -t -c "{sql}"'
    try:
        output = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if not output: return list(range(64))
        return sorted([int(x.strip()) for x in output.split('\n') if x.strip()])
    except:
        return list(range(64))

def check_query_has_rows(sql):
    cmd = f'psql -X -t -c "{sql}"'
    try:
        output = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        return bool(output)
    except:
        return False

def get_finished_shards(step_id):
    sql = f"SELECT DISTINCT shard_id FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND step_id='{step_id}'"
    cmd = f'psql -X -t -c "{sql}"'
    try:
        output = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if not output: return []
        return [int(x.strip()) for x in output.split('\n') if x.strip()]
    except:
        return []

# ==========================================
# Worker Functions
# ==========================================

def worker_pipeline_1(shard_id):
    replacements = {"{{run_id}}": RUN_ID, "{{contract_version}}": CONTRACT_VERSION, "{{shard_id}}": str(shard_id)}
    log(f"Starting Shard {shard_id} Pipeline 1...")
    try:
        # Granular Step Resume
        if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND shard_id={shard_id} AND step_id='RB20_01'"):
            exec_sql_file(SQL_FILES["p1_01"], replacements, f"S{shard_id}: 01 Source")
        
        if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND shard_id={shard_id} AND step_id='RB20_02'"):
            exec_sql_file(SQL_FILES["p1_02"], replacements, f"S{shard_id}: 02 Natural")
            
        if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND shard_id={shard_id} AND step_id='RB20_03'"):
            exec_sql_file(SQL_FILES["p1_03"], replacements, f"S{shard_id}: 03 Pre")
            
        if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND shard_id={shard_id} AND step_id='RB20_11'"):
            exec_sql_file(SQL_FILES["p1_11"], replacements, f"S{shard_id}: 11 Window")
            
        if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND shard_id={shard_id} AND step_id='RB20_04'"):
            exec_sql_file(SQL_FILES["p1_04"], replacements, f"S{shard_id}: 04 Split")
            
        if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND shard_id={shard_id} AND step_id='RB20_04P'"):
            exec_sql_file(SQL_FILES["p1_04P"], replacements, f"S{shard_id}: 04P Profile")
            
        log(f"DONE Shard {shard_id} Pipeline 1.")
        return (True, shard_id, None)
    except Exception as e:
        log(f"ERROR Shard {shard_id}: {str(e)}")
        return (False, shard_id, str(e))

def worker_pipeline_2(shard_id):
    replacements = {"{{run_id}}": RUN_ID, "{{contract_version}}": CONTRACT_VERSION, "{{shard_id}}": str(shard_id)}
    try:
        if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND shard_id={shard_id} AND step_id='RB20_06'"):
            exec_sql_file(SQL_FILES["p2_06"], replacements, f"S{shard_id}: 06 R1")
        if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND shard_id={shard_id} AND step_id='RB20_07'"):
            exec_sql_file(SQL_FILES["p2_07"], replacements, f"S{shard_id}: 07 E")
        if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND shard_id={shard_id} AND step_id='RB20_08'"):
            exec_sql_file(SQL_FILES["p2_08"], replacements, f"S{shard_id}: 08 F")
        return (True, shard_id, None)
    except Exception as e:
        return (False, shard_id, str(e))

def main():
    log(f"Orchestrator Resuming. RUN_ID: {RUN_ID}")
    
    # 3.3 Abnormal Dedup
    log("Checking 3.3 Abnormal Dedup...")
    if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND step_id='RB20_01A'"):
        log("Running 3.3 Abnormal Dedup...")
        exec_sql_file(SQL_FILES["abnormal_dedup"], {"{{run_id}}": RUN_ID, "{{contract_version}}": CONTRACT_VERSION}, "3.3")

    # 4) Pipeline 1
    all_shards = get_all_shards()
    finished = get_finished_shards("RB20_04P")
    pending = [s for s in all_shards if s not in finished]
    log(f"Phase 4: Pipeline 1. Total: {len(all_shards)}, Finished: {len(finished)}, Pending: {len(pending)}")
    
    if pending:
        # Balanced 4-concurrency for healthy HDD sequential throughput
        with multiprocessing.Pool(processes=4) as pool:
            results = pool.map(worker_pipeline_1, pending)
        failures = [r for r in results if not r[0]]
        if failures:
            log(f"Phase 4 FAILED for {len(failures)} shards.")
            sys.exit(1)

    # 5) Global H
    log("Checking Phase 5: Global H...")
    if not check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.core_numbers WHERE run_id='{RUN_ID}' AND metric_name='h_block_cnt'"):
        log("Running Phase 5: Global H...")
        exec_sql_file(SQL_FILES["h_global"], {"{{run_id}}": RUN_ID, "{{contract_version}}": CONTRACT_VERSION}, "5.1")

    # 6) Pipeline 2
    all_shards = get_all_shards() # Refresh list in case Phase 5 logic added something (unlikely but safe)
    finished_p2 = get_finished_shards("RB20_08")
    pending_p2 = [s for s in all_shards if s not in finished_p2]
    log(f"Phase 6: Pipeline 2. Total: {len(all_shards)}, Finished: {len(finished_p2)}, Pending: {len(pending_p2)}")
    
    if pending_p2:
        with multiprocessing.Pool(processes=10) as pool:
            results = pool.map(worker_pipeline_2, pending_p2)
        failures = [r for r in results if not r[0]]
        if failures:
            log(f"Phase 6 FAILED for {len(failures)} shards.")
            sys.exit(1)

    # 7) QA
    log("Running Phase 7: Final QA...")
    exec_sql_file(SQL_FILES["qa_assert"], {"{{run_id}}": RUN_ID, "{{contract_version}}": CONTRACT_VERSION}, "7.1")
    if check_query_has_rows(f"SELECT 1 FROM rb20_v2_5.qa_assert WHERE run_id='{RUN_ID}' AND NOT pass_flag"):
        log("QA Assertions FAILED.")
        sys.exit(1)
    
    log("=== ALL STEPS COMPLETED SUCCESSFULLY ===")

if __name__ == "__main__":
    main()
