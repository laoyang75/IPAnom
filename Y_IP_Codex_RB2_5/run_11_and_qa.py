import os
import subprocess
import sys
import multiprocessing
import time
from datetime import datetime

RUN_ID = "rb20v2_20260202_191900_sg_001"
CONTRACT_VERSION = "contract_v1"
SHARD_CNT = 64
CONCURRENCY = 4

os.environ["PGHOST"] = "192.168.200.217"
os.environ["PGPORT"] = "5432"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "123456"
os.environ["PGDATABASE"] = "ip_loc2"

BASE_DIR = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_cmd(cmd):
    try:
        subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        log(f"ERROR executing command: {cmd}")
        log(f"Output: {e.output.decode('utf-8')}")
        raise e
        
def exec_sql_file(file_path, replacements, description=""):
    with open(file_path, 'r') as f:
        content = f.read()
    for k, v in replacements.items():
        content = content.replace(k, str(v))
    
    # Inject work_mem and enable_nestloop to force better plans on huge shards
    content = "SET work_mem = '2GB';\nSET enable_nestloop = off;\n" + content
    
    temp_filename = f"/tmp/rb20_exec_{int(time.time()*1000)}_{os.getpid()}.sql"
    with open(temp_filename, 'w') as f:
        f.write(content)
    try:
        log(f"Executing {description}...")
        run_cmd(f"psql -v ON_ERROR_STOP=1 -f {temp_filename}")
    except Exception as e:
        log(f"FAILED: {description}")
        raise e
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def worker_11(shard_id):
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_id}}": str(shard_id)
    }
    try:
        exec_sql_file(f"{BASE_DIR}/RB20_11/11_window_headtail_shard.sql", replacements, f"Shard {shard_id}: 11 Window")
        return (True, shard_id, None)
    except Exception as e:
        return (False, shard_id, str(e))

def main():
    log(f"Starting missing RB20_11 steps. RUN_ID: {RUN_ID}")
    start_time = time.time()
    
    # Only run for missing shards
    shards = [60, 61, 62, 63]
    
    with multiprocessing.Pool(processes=CONCURRENCY) as pool:
        results = pool.map(worker_11, shards)
        
    failures = [r for r in results if not r[0]]
    if failures:
        log(f"STOP: {len(failures)} shards failed in Pipeline 11.")
        for f in failures:
            log(f"Shard {f[1]} Error: {f[2]}")
        sys.exit(1)
        
    log("=== Phase 7: Final QA ===")
    
    # Replace the vars for QA script but DON'T inject enable_nestloop = off since QA has many queries that need it
    with open(f"{BASE_DIR}/RB20_99/99_qa_assert.sql", 'r') as f:
        qa_content = f.read()
    qa_replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_cnt}}": str(SHARD_CNT)
    }
    for k, v in qa_replacements.items():
        qa_content = qa_content.replace(k, str(v))
    qa_temp = f"/tmp/rb20_qa_{int(time.time()*1000)}.sql"
    with open(qa_temp, 'w') as f:
        f.write(qa_content)
        
    log("Executing QA Assert...")
    try:
        run_cmd(f"psql -v ON_ERROR_STOP=1 -f {qa_temp}")
    finally:
        if os.path.exists(qa_temp):
            os.remove(qa_temp)
    
    cmd = f'psql -t -c "SELECT assert_name, severity, pass_flag, details FROM rb20_v2_5.qa_assert WHERE run_id=\'{RUN_ID}\' AND NOT pass_flag ORDER BY assert_name"'
    failed_qa = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
    
    if failed_qa:
        log("STOP: QA Assertions FAILED.")
        print(failed_qa)
        sys.exit(1)
    else:
        log("QA Assertions PASSED.")

    end_time = time.time()
    log(f"=== SUCCESS ===")
    log(f"Total Time: {int(end_time - start_time)} seconds")

if __name__ == "__main__":
    main()
