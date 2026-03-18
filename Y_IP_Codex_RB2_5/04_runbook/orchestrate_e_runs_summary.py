import os
import time
import psycopg2
import math
from multiprocessing import Pool
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
SHARD_CNT = 64
CONCURRENCY = 16

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S', stream=sys.stdout)

def log(msg):
    logging.info(msg)
    sys.stdout.flush()

def get_db_conn():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        log(f"DB Connection Error: {e}")
        raise

def prep_table():
    log("Prepping e_runs_summary table...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS rb20_v2_5.e_runs_summary")
    
    ddl = """
    CREATE UNLOGGED TABLE rb20_v2_5.e_runs_summary (
      run_id varchar,
      shard_id smallint,
      e_run_id varchar,
      ip_count int,
      avg_devices numeric,
      total_reports bigint,
      daa_reports bigint,
      dna_reports bigint,
      avg_manufacturers numeric,
      top_operator varchar,
      PRIMARY KEY(run_id, shard_id, e_run_id)
    );
    """
    cur.execute(ddl)
    log("e_runs_summary table prep done.")
    conn.close()

def process_shard(shard_id):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        
        log(f"Shard {shard_id}: Starting summary aggregation...")
        # Since source_members is a partitioned table / distributed across shards,
        # aggregating within one shard using PostgreSQL window/group by is efficient.
        
        sql = f"""
        BEGIN;
        SET LOCAL work_mem = '512MB';
        
        WITH shard_stats AS (
            SELECT
                em.e_run_id,
                em.ip_long,
                sm."设备数量" AS devices,
                sm."上报次数" AS reports,
                sm."DAA业务上报次数" AS daa_reports,
                sm."DNA业务上报次数" AS dna_reports,
                sm."制造商数量" AS manufacturers,
                sm."IP归属运营商" AS operator
            FROM rb20_v2_5.e_members em
            JOIN rb20_v2_5.source_members sm
              ON sm.run_id = em.run_id AND sm.shard_id = em.shard_id AND sm.ip_long = em.ip_long
            WHERE em.run_id = '{RUN_ID}' AND em.shard_id = {shard_id}
        ),
        op_counts AS (
            SELECT e_run_id, operator, COUNT(*) as cnt,
                   ROW_NUMBER() OVER(PARTITION BY e_run_id ORDER BY COUNT(*) DESC) as rn
            FROM shard_stats
            WHERE operator IS NOT NULL AND operator != ''
            GROUP BY e_run_id, operator
        ),
        top_ops AS (
            SELECT e_run_id, operator as top_operator
            FROM op_counts
            WHERE rn = 1
        )
        INSERT INTO rb20_v2_5.e_runs_summary (
            run_id, shard_id, e_run_id, ip_count, avg_devices, total_reports,
            daa_reports, dna_reports, avg_manufacturers, top_operator
        )
        SELECT
            '{RUN_ID}',
            {shard_id},
            s.e_run_id,
            COUNT(s.ip_long) as ip_count,
            ROUND(AVG(s.devices), 2) as avg_devices,
            SUM(s.reports) as total_reports,
            SUM(s.daa_reports) as daa_reports,
            SUM(s.dna_reports) as dna_reports,
            ROUND(AVG(s.manufacturers), 2) as avg_manufacturers,
            MAX(t.top_operator) as top_operator
        FROM shard_stats s
        LEFT JOIN top_ops t ON s.e_run_id = t.e_run_id
        GROUP BY s.e_run_id;
        
        COMMIT;
        """
        cur.execute(sql)
        log(f"Shard {shard_id}: Done.")
        conn.close()
        return True
    except Exception as e:
        log(f"Shard {shard_id} Failed: {e}")
        return False

def build_indexes():
    log("Building indexes on e_runs_summary...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_e_runs_summary_run_id ON rb20_v2_5.e_runs_summary(run_id);")
    cur.execute("ANALYZE rb20_v2_5.e_runs_summary;")
    conn.close()
    log("Indexes built.")

def main():
    start = time.time()
    log(f"=== Starting E Runs Summary table extraction (Run: {RUN_ID}) ===")
    
    prep_table()
    
    shards = list(range(SHARD_CNT))
    log(f"Processing {SHARD_CNT} shards with concurrency {CONCURRENCY}...")
    
    pool = Pool(CONCURRENCY)
    pool.map(process_shard, shards)
    pool.close()
    pool.join()
    
    build_indexes()
    log(f"=== E Runs Summary extraction complete in {time.time()-start:.2f}s ===")

if __name__ == "__main__":
    main()
