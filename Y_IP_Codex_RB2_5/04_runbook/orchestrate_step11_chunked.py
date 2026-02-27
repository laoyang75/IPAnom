
import os
import time
import psycopg2
import math
from multiprocessing import Pool, cpu_count
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
SHARD_CNT = 64
BLOCK_CHUNK_SIZE = 500  # Number of blocks per SQL execution
CONCURRENCY = 8

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

def run_sql(sql, args=None, fetch=False):
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, args)
        if fetch:
            res = cur.fetchall()
            conn.commit()
            return res
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def prep_slim_table_v2():
    log("Checking/Prepping source_members_slim (V2 with operator)...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    
    # Check if 'operator' column exists
    cur.execute("""
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema='rb20_v2_5' AND table_name='source_members_slim' AND column_name='operator'
    """)
    if cur.fetchone():
        log("source_members_slim already has operator column. skipping.")
        return

    log("Recreating source_members_slim to include operator...")
    cur.execute("DROP TABLE IF EXISTS rb20_v2_5.source_members_slim")
    
    ddl = f"""
    CREATE UNLOGGED TABLE rb20_v2_5.source_members_slim AS
    SELECT
      run_id, shard_id, ip_long,
      is_valid,
      "设备数量" as devices,
      "上报次数" as reports,
      "移动网络设备数量" as mobile_devices,
      "WiFi设备数量" as wifi_devices,
      "VPN设备数量" as vpn_devices,
      "有线网络设备数量" as wired_devices,
      "异常网络设备数量" as abnormal_net_devices,
      "空网络状态设备数量" as empty_net_devices,
      "工作时上报次数" as worktime_reports,
      "工作日上报次数" as workday_reports,
      "周末上报次数" as weekend_reports,
      "深夜上报次数" as late_night_reports,
      "IP归属运营商" as operator
    FROM rb20_v2_5.source_members
    WHERE run_id='{RUN_ID}';
    """
    cur.execute(ddl)
    log("Creating Index on slim table...")
    cur.execute("CREATE INDEX idx_sm_slim_run_shard_ip ON rb20_v2_5.source_members_slim(run_id, shard_id, ip_long);")
    cur.execute("ANALYZE rb20_v2_5.source_members_slim;")
    log("Slim table V2 prep done.")
    conn.close()

def process_shard_chunked(shard_id):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        
        # 1. Clear previous results for this shard
        cur.execute(f"DELETE FROM rb20_v2_5.window_headtail_64 WHERE run_id='{RUN_ID}' AND shard_id={shard_id}")
        conn.commit()
        
        # 2. Fetch all blocks
        cur.execute(f"SELECT block_id_natural FROM rb20_v2_5.preh_blocks WHERE run_id='{RUN_ID}' AND shard_id={shard_id}")
        blocks = [r[0] for r in cur.fetchall()]
        
        if not blocks:
            log(f"Shard {shard_id}: No PreH blocks found.")
            conn.close()
            return True

        log(f"Shard {shard_id}: Processing {len(blocks)} blocks in chunks of {BLOCK_CHUNK_SIZE}...")
        
        # 3. Process in Chunks
        total_chunks = math.ceil(len(blocks) / BLOCK_CHUNK_SIZE)
        
        for i in range(total_chunks):
            chunk = blocks[i*BLOCK_CHUNK_SIZE : (i+1)*BLOCK_CHUNK_SIZE]
            
            # Optimization: pass chunk as simple list string or temp table? 
            # List string is fine for 500 IDs.
            # Safety: Sanitize input (they are strings)
            block_list_sql = ", ".join(f"'{b}'" for b in chunk)
            
            sql = f"""
            BEGIN;
            SET LOCAL enable_hashagg = on;
            SET LOCAL work_mem = '256MB';
            
            WITH preh AS (
              SELECT unnest(ARRAY[{block_list_sql}]) as block_id_natural
            ),
            base AS (
              SELECT
                map.block_id_natural,
                sm.ip_long / 64 as bucket64,
                sm.ip_long,
                sm.is_valid,
                sm.reports,
                sm.mobile_devices,
                sm.operator
              FROM preh p
              JOIN rb20_v2_5.map_member_block_natural map
                ON map.run_id='{RUN_ID}' AND map.shard_id={shard_id} AND map.block_id_natural=p.block_id_natural
              JOIN rb20_v2_5.source_members_slim sm
                ON sm.run_id=map.run_id AND sm.shard_id=map.shard_id AND sm.ip_long=map.ip_long
            ),
            bucket_set AS (
              SELECT DISTINCT block_id_natural, bucket64 FROM base
            ),
            cand AS (
              SELECT b.block_id_natural, b.bucket64
              FROM bucket_set b
              JOIN bucket_set b2 ON b2.block_id_natural=b.block_id_natural AND b2.bucket64=b.bucket64 + 1
            ),
            m AS (SELECT * FROM base WHERE is_valid),
            left_k AS (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY block_id_natural, bucket64 ORDER BY ip_long DESC) AS rn
              FROM m
            ),
            left_agg AS (
              SELECT block_id_natural, bucket64,
                     COUNT(*) FILTER (WHERE rn <= 5) as left_cnt_valid,
                     COALESCE(SUM(reports) FILTER (WHERE rn <= 5),0) as left_reports_sum_valid,
                     COALESCE(SUM(mobile_devices) FILTER (WHERE rn <= 5),0) as left_mobile_devices_sum_valid,
                     COUNT(DISTINCT operator) FILTER (WHERE rn <= 5 AND operator IS NOT NULL) as left_op_distinct,
                     MAX(operator) FILTER (WHERE rn <= 5) as left_op_any
              FROM left_k
              GROUP BY 1,2
            ),
            right_k AS (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY block_id_natural, bucket64 ORDER BY ip_long ASC) AS rn
              FROM m
            ),
            right_agg AS (
              SELECT block_id_natural, (bucket64 - 1) as bucket64,
                     COUNT(*) FILTER (WHERE rn <= 5) as right_cnt_valid,
                     COALESCE(SUM(reports) FILTER (WHERE rn <= 5),0) as right_reports_sum_valid,
                     COALESCE(SUM(mobile_devices) FILTER (WHERE rn <= 5),0) as right_mobile_devices_sum_valid,
                     COUNT(DISTINCT operator) FILTER (WHERE rn <= 5 AND operator IS NOT NULL) as right_op_distinct,
                     MAX(operator) FILTER (WHERE rn <= 5) as right_op_any
              FROM right_k
              GROUP BY 1,2
            )
            INSERT INTO rb20_v2_5.window_headtail_64(
               run_id, contract_version, shard_id, block_id_natural, bucket64, k,
               left_cnt_valid, right_cnt_valid, left_reports_sum_valid, right_reports_sum_valid,
               left_mobile_devices_sum_valid, right_mobile_devices_sum_valid,
               left_operator_unique, right_operator_unique
            )
            SELECT
              '{RUN_ID}','{CONTRACT_VERSION}', {shard_id},
              c.block_id_natural, c.bucket64, 5,
              COALESCE(l.left_cnt_valid,0), COALESCE(r.right_cnt_valid,0),
              COALESCE(l.left_reports_sum_valid,0), COALESCE(r.right_reports_sum_valid,0),
              COALESCE(l.left_mobile_devices_sum_valid,0), COALESCE(r.right_mobile_devices_sum_valid,0),
              CASE WHEN COALESCE(l.left_op_distinct,0) = 1 THEN l.left_op_any ELSE NULL END,
              CASE WHEN COALESCE(r.right_op_distinct,0) = 1 THEN r.right_op_any ELSE NULL END
            FROM cand c
            LEFT JOIN left_agg l USING (block_id_natural, bucket64)
            LEFT JOIN right_agg r USING (block_id_natural, bucket64);
            
            COMMIT;
            """
            cur.execute(sql)
            
        log(f"Shard {shard_id}: Done.")
        conn.close()
        return True
    except Exception as e:
        log(f"Shard {shard_id} Failed: {e}")
        return False

def main():
    log(f"=== Step 11 Chunked Optimization (Run: {RUN_ID}) ===")
    
    # 1. Prep V2 Table
    prep_slim_table_v2()
    
    # 2. Processing
    shards = list(range(SHARD_CNT))
    log(f"Processing {SHARD_CNT} shards with concurrency {CONCURRENCY}...")
    
    pool = Pool(CONCURRENCY)
    pool.map(process_shard_chunked, shards)
    pool.close()
    pool.join()
    
    log("=== Step 11 Processing Complete ===")

if __name__ == "__main__":
    main()
