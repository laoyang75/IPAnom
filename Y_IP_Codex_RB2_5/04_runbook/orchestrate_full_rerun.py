
import os
import time
import psycopg2
import logging
import sys
import subprocess

# Configuration
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

RUN_ID = "rb20v2_20260202_191900_sg_001"
CONTRACT_VERSION = "contract_v1"

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S', stream=sys.stdout)

def log(msg):
    logging.info(msg)
    sys.stdout.flush()

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def run_sql(sql):
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        log(f"SQL Error: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()

def run_script(script_path):
    log(f"Running script: {script_path}...")
    start_t = time.time()
    try:
        # Use subprocess to run the python scripts in the same env
        subprocess.check_call([sys.executable, script_path])
        log(f"Script {os.path.basename(script_path)} finished in {time.time() - start_t:.2f}s.")
    except subprocess.CalledProcessError as e:
        log(f"Script {os.path.basename(script_path)} FAILED with code {e.returncode}")
        sys.exit(1)

def clean_data():
    log("=== Phase 0: Cleaning Data ===")
    sqls = [
        f"DELETE FROM rb20_v2_5.step03_task_plan WHERE run_id='{RUN_ID}'",
        f"DELETE FROM rb20_v2_5.step03_block_bucket WHERE run_id='{RUN_ID}'",
        f"DELETE FROM rb20_v2_5.profile_pre WHERE run_id='{RUN_ID}'",
        f"DELETE FROM rb20_v2_5.profile_pre_stage WHERE run_id='{RUN_ID}'",
        f"DELETE FROM rb20_v2_5.preh_blocks WHERE run_id='{RUN_ID}'",
        f"DELETE FROM rb20_v2_5.keep_members WHERE run_id='{RUN_ID}'",
        f"DELETE FROM rb20_v2_5.drop_members WHERE run_id='{RUN_ID}'",
        f"DELETE FROM rb20_v2_5.window_headtail_64 WHERE run_id='{RUN_ID}'",
        f"DELETE FROM rb20_v2_5.step_stats WHERE run_id='{RUN_ID}' AND step_id IN ('RB20_03', 'RB20_11')"
    ]
    for sql in sqls:
        run_sql(sql)
    log("Data clean complete.")

def main():
    log(f"=== Starting Full Optimization Rerun (Run: {RUN_ID}) ===")
    
    # 0. Clean
    clean_data()
    
    # 1. Step 03 Optimization (Bucket Full Rollout)
    # This script handles Plan Gen -> Execution -> Commit to profile_pre
    step03_script = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/04_runbook/orchestrate_step03_bucket_full.py"
    run_script(step03_script)
    
    # 2. Step 03 Post-Processing (Dependency for Step 11)
    # We reuse the finalize logic or just run the post-process SQL
    # Actually, we should incorporate post-processing into the orchestrate script or run finalize here.
    # The 'finalize_step03.py' script does the Commit AND Post-Processing.
    # But 'orchestrate_step03_bucket_full.py' DOES include 'commit_to_final()'.
    # It DOES NOT include 'post_process.sql'. We need to add that.
    # To be safe, let's run finalize_step03.py AGAIN? No, that would duplicate inserts or be empty.
    # Better: Run a dedicated post-process runner.
    
    log("=== Phase 1.5: Step 03 Post-Processing (PreH Generation) ===")
    finalize_script = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/04_runbook/finalize_step03.py"
    # Note: finalize_step03.py creates 03_post_process.sql and runs it.
    # It creates profile_pre from stage. orchestrate_step03_bucket_full also does it.
    # We should ensure orchestrate_step03_bucket_full does NOT commit, or we accept the redundancy (idempotent delete/insert).
    # Since orchestrate_step03 DOES commit, finalize_step03 will just re-do it (safe).
    run_script(finalize_script)

    # 3. Step 11 Optimization (Chunked Execution)
    step11_script = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/04_runbook/orchestrate_step11_chunked.py"
    run_script(step11_script)
    
    log("=== Full Optimization Rerun Complete ===")

if __name__ == "__main__":
    main()
