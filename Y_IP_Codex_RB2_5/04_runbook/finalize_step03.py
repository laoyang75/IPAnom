
import os
import psycopg2
import logging
import sys

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

def run_sql(cur, sql):
    try:
        cur.execute(sql)
    except Exception as e:
        log(f"SQL Error: {e}")
        raise e

def run_file(cur, file_path, replacements):
    with open(file_path, 'r') as f:
        sql = f.read()
        for k, v in replacements.items():
            sql = sql.replace(k, str(v))
        cur.execute(sql)

def main():
    log(f"=== Finalizing Step 03 Results (Run: {RUN_ID}) ===")
    conn = get_db_conn()
    cur = conn.cursor()
    
    # 1. Commit Staged Data to Final Table
    log("Committing valid buckets from stage to final profile_pre...")
    
    # Clear existing data for this run to avoid duplicates (idempotent)
    run_sql(cur, f"DELETE FROM rb20_v2_5.profile_pre WHERE run_id='{RUN_ID}'")
    
    # Insert from stage
    run_sql(cur, f"""
    INSERT INTO rb20_v2_5.profile_pre 
    SELECT * FROM rb20_v2_5.profile_pre_stage 
    WHERE run_id='{RUN_ID}' 
    """)
    rows = cur.rowcount
    log(f"Committed {rows} rows to profile_pre.")
    
    # 2. Run Post-Processing for EACH Shard that has data
    # (The SQL file takes {{shard_id}}, so we loop 0..63)
    log("Running 03_post_process.sql for all shards...")
    
    post_process_sql_path = "/Users/yangcongan/cursor/2/Y_IP_Codex_RB2_5/03_sql/RB20_03/03_post_process.sql"
    
    for shard_id in range(64):
        replacements = {
            "{{run_id}}": RUN_ID,
            "{{contract_version}}": CONTRACT_VERSION,
            "{{shard_id}}": shard_id
        }
        try:
            run_file(cur, post_process_sql_path, replacements)
            # log(f"Shard {shard_id} post-processed.")
        except Exception as e:
            log(f"Shard {shard_id} Post-Process Failed: {e}")
            conn.rollback() # Rollback transaction if needed? 
            # Ideally we want partial success?
            # committing valid shards.
    
    conn.commit()
    conn.close()
    log("=== Finalization Complete ===")

if __name__ == "__main__":
    main()
