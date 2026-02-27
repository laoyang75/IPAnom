
import os
import time
import psycopg2
import math
from multiprocessing import Pool, cpu_count
import logging

# Configuration
# Database Connection Info (Copied from orchestrate_resume_phase4.py)
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


# Run Configuration
RUN_ID = "rb20v2_20260202_191900_sg_001"
CONTRACT_VERSION = "contract_v1"
TARGET_SHARD = 5  # The problematic shard
TARGET_ROWS_PER_BUCKET = 200000
CONCURRENCY = 8  # Reduced from 32

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')

def log(msg):
    logging.info(msg)

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

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

def log_task_status(shard_id, bucket_id, status):
    sql = """
    UPDATE rb20_v2_5.step03_task_plan 
    SET status = %s, 
        started_at = CASE WHEN %s = 'RUNNING' THEN now() ELSE started_at END,
        finished_at = CASE WHEN %s = 'DONE' THEN now() ELSE finished_at END
    WHERE run_id = %s AND shard_id = %s AND bucket_id = %s
    """
    run_sql(sql, (status, status, status, RUN_ID, shard_id, bucket_id))

def prep_slim_table():
    log("Prepping source_members_slim table...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    # Check if exists and has data
    cur.execute("SELECT 1 FROM pg_tables WHERE schemaname='rb20_v2_5' AND tablename='source_members_slim'")
    if cur.fetchone():
        log("source_members_slim exists. Skipping creation (assume populated or user manual drop needed if stale).")
        return

    log("Creating UNLOGGED source_members_slim...")
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
      "深夜上报次数" as late_night_reports
    FROM rb20_v2_5.source_members
    WHERE run_id='{RUN_ID}';
    """
    cur.execute(ddl)
    log("Creating Index on slim table...")
    cur.execute("CREATE INDEX idx_sm_slim_run_shard_ip ON rb20_v2_5.source_members_slim(run_id, shard_id, ip_long);")
    cur.execute("ANALYZE rb20_v2_5.source_members_slim;")
    log("Slim table prep done.")
    conn.close()

def generate_bucket_plan():
    log("Generating Bucket Plan...")
    
    # Check if plan already exists for this shard
    res = run_sql("SELECT count(*) FROM rb20_v2_5.step03_task_plan WHERE run_id=%s AND shard_id=%s", (RUN_ID, TARGET_SHARD), fetch=True)
    if res[0][0] > 0:
        log("Plan already exists for Shard 5. Skipping generation.")
        return

    # 1. Get Block Sizes
    log("Fetching block sizes...")
    sql_blocks = f"""
    SELECT block_id_natural, count(*) as cnt 
    FROM rb20_v2_5.map_member_block_natural 
    WHERE run_id='{RUN_ID}' AND shard_id={TARGET_SHARD}
    GROUP BY block_id_natural
    """
    blocks = run_sql(sql_blocks, fetch=True) # [(block_id, cnt), ...]
    
    # 2. LPT Algorithm (Greedy)
    blocks = sorted(blocks, key=lambda x: x[1], reverse=True)
    total_rows = sum(x[1] for x in blocks)
    est_buckets = max(1, math.ceil(total_rows / TARGET_ROWS_PER_BUCKET))
    log(f"Shard {TARGET_SHARD}: {len(blocks)} blocks, {total_rows} rows. Est Buckets: {est_buckets}")

    buckets = [{'id': i+1, 'load': 0, 'blocks': []} for i in range(est_buckets)]
    
    import heapq
    # Min-heap based on load (load, bucket_index, bucket_obj)
    heap = [(0, i, buckets[i]) for i in range(len(buckets))]
    
    for blk_id, cnt in blocks:
        load, idx, bucket = heapq.heappop(heap)
        bucket['load'] += cnt
        bucket['blocks'].append((blk_id, cnt))
        heapq.heappush(heap, (bucket['load'], idx, bucket))
    
    # 3. Write to DB
    log("Writing plan to DB...")
    
    # Bulk insert for speed? For now simple loops are okay for 1 shard
    batch_map = []
    batch_plan = []
    
    for b in buckets:
        if not b['blocks']: continue
        batch_plan.append((RUN_ID, TARGET_SHARD, b['id'], b['load'], len(b['blocks'])))
        for blk_id, cnt in b['blocks']:
            batch_map.append((RUN_ID, TARGET_SHARD, blk_id, b['id'], cnt))
            
    conn = get_db_conn()
    cur = conn.cursor()
    
    # Insert Plan
    cur.executemany("INSERT INTO rb20_v2_5.step03_task_plan (run_id, shard_id, bucket_id, est_member_rows, est_block_cnt) VALUES (%s, %s, %s, %s, %s)", batch_plan)
    
    # Insert Map
    # Split big batch
    chunk_size = 1000
    for i in range(0, len(batch_map), chunk_size):
        chunk = batch_map[i:i+chunk_size]
        cur.executemany("INSERT INTO rb20_v2_5.step03_block_bucket (run_id, shard_id, block_id_natural, bucket_id, est_members) VALUES (%s, %s, %s, %s, %s)", chunk)
        
    conn.commit()
    conn.close()
    log("Plan generation complete.")

def worker_execute_bucket(args):
    shard_id, bucket_id = args
    try:
        log_task_status(shard_id, bucket_id, 'RUNNING')
        
        # SQL Template from wenti.md Section 3.5
        sql = f"""
        BEGIN;
        
        SET LOCAL enable_hashagg = on;
        SET LOCAL jit = off;
        SET LOCAL work_mem = '256MB';
        
        WITH m AS (
          SELECT
            map.block_id_natural,
            sm.ip_long,
            sm.is_valid,
            sm.devices,
            sm.reports,
            sm.mobile_devices,
            sm.wifi_devices,
            sm.vpn_devices,
            sm.wired_devices,
            sm.abnormal_net_devices,
            sm.empty_net_devices,
            sm.worktime_reports,
            sm.workday_reports,
            sm.weekend_reports,
            sm.late_night_reports
          FROM rb20_v2_5.step03_block_bucket bb
          JOIN rb20_v2_5.map_member_block_natural map
            ON map.run_id=bb.run_id
           AND map.shard_id=bb.shard_id
           AND map.block_id_natural=bb.block_id_natural
          JOIN rb20_v2_5.source_members_slim sm
            ON sm.run_id=map.run_id
           AND sm.shard_id=map.shard_id
           AND sm.ip_long=map.ip_long
          WHERE bb.run_id='{RUN_ID}'
            AND bb.shard_id={shard_id}
            AND bb.bucket_id={bucket_id}
        ),
        agg AS (
          SELECT
            block_id_natural,
            COUNT(*)::bigint AS member_cnt_total,
            COUNT(*) FILTER (WHERE is_valid)::bigint AS valid_cnt,
        
            SUM(COALESCE(reports,0))::bigint AS reports_sum_total,
            COALESCE(SUM(COALESCE(reports,0)) FILTER (WHERE is_valid), 0)::bigint AS reports_sum_valid,
        
            SUM(COALESCE(devices,0))::bigint AS devices_sum_total,
            COALESCE(SUM(COALESCE(devices,0)) FILTER (WHERE is_valid), 0)::bigint AS devices_sum_valid,
        
            SUM(COALESCE(mobile_devices,0))::bigint AS mobile_devices_sum_total,
            COALESCE(SUM(COALESCE(mobile_devices,0)) FILTER (WHERE is_valid), 0)::bigint AS mobile_devices_sum_valid,
        
            SUM(COALESCE(wifi_devices,0))::bigint AS wifi_devices_sum_total,
            COALESCE(SUM(COALESCE(wifi_devices,0)) FILTER (WHERE is_valid), 0)::bigint AS wifi_devices_sum_valid,
        
            SUM(COALESCE(vpn_devices,0))::bigint AS vpn_devices_sum_total,
            COALESCE(SUM(COALESCE(vpn_devices,0)) FILTER (WHERE is_valid), 0)::bigint AS vpn_devices_sum_valid,
        
            SUM(COALESCE(wired_devices,0))::bigint AS wired_devices_sum_total,
            COALESCE(SUM(COALESCE(wired_devices,0)) FILTER (WHERE is_valid), 0)::bigint AS wired_devices_sum_valid,
        
            SUM(COALESCE(abnormal_net_devices,0))::bigint AS abnormal_net_devices_sum_total,
            COALESCE(SUM(COALESCE(abnormal_net_devices,0)) FILTER (WHERE is_valid), 0)::bigint AS abnormal_net_devices_sum_valid,
        
            SUM(COALESCE(empty_net_devices,0))::bigint AS empty_net_devices_sum_total,
            COALESCE(SUM(COALESCE(empty_net_devices,0)) FILTER (WHERE is_valid), 0)::bigint AS empty_net_devices_sum_valid,
        
            SUM(COALESCE(worktime_reports,0))::bigint AS worktime_reports_sum_total,
            COALESCE(SUM(COALESCE(worktime_reports,0)) FILTER (WHERE is_valid), 0)::bigint AS worktime_reports_sum_valid,
        
            SUM(COALESCE(workday_reports,0))::bigint AS workday_reports_sum_total,
            COALESCE(SUM(COALESCE(workday_reports,0)) FILTER (WHERE is_valid), 0)::bigint AS workday_reports_sum_valid,
        
            SUM(COALESCE(weekend_reports,0))::bigint AS weekend_reports_sum_total,
            COALESCE(SUM(COALESCE(weekend_reports,0)) FILTER (WHERE is_valid), 0)::bigint AS weekend_reports_sum_valid,
        
            SUM(COALESCE(late_night_reports,0))::bigint AS late_night_reports_sum_total,
            COALESCE(SUM(COALESCE(late_night_reports,0)) FILTER (WHERE is_valid), 0)::bigint AS late_night_reports_sum_valid
          FROM m
          GROUP BY 1
        ),
        score AS (
          SELECT
            a.*,
            (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) AS density,
            (a.reports_sum_valid::numeric / NULLIF(a.valid_cnt,0)) AS report_density_valid,
            CASE
              WHEN a.valid_cnt = 0 THEN NULL
              WHEN a.valid_cnt BETWEEN 1 AND 16 THEN 1
              WHEN a.valid_cnt BETWEEN 17 AND 48 THEN 2
              WHEN a.valid_cnt BETWEEN 49 AND 128 THEN 4
              WHEN a.valid_cnt BETWEEN 129 AND 512 THEN 8
              ELSE 16
            END AS wA,
            CASE
              WHEN a.valid_cnt = 0 THEN NULL
              WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 3.5 THEN 1
              WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 6.5 THEN 2
              WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 30 THEN 4
              WHEN (a.devices_sum_valid::numeric / NULLIF(a.valid_cnt,0)) <= 200 THEN 16
              ELSE 32
            END AS wD
          FROM agg a
        ),
        tier AS (
          SELECT
            s.*,
            CASE WHEN s.valid_cnt = 0 THEN NULL ELSE (s.wA + s.wD) END AS simple_score,
            CASE
              WHEN s.valid_cnt = 0 THEN '无效块'
              WHEN (s.wA + s.wD) >= 40 THEN '超大网络'
              WHEN (s.wA + s.wD) >= 30 THEN '大型网络'
              WHEN (s.wA + s.wD) >= 20 THEN '中型网络'
              WHEN (s.wA + s.wD) >= 10 THEN '小型网络'
              ELSE '微型网络'
            END AS network_tier_pre,
            CASE WHEN s.valid_cnt = 0 THEN false ELSE true END AS keep_flag,
            CASE WHEN s.valid_cnt = 0 THEN 'ALL_ABNORMAL_BLOCK' ELSE NULL END AS drop_reason
          FROM score s
        )
        INSERT INTO rb20_v2_5.profile_pre_stage(
          run_id, contract_version, shard_id, block_id_natural,
          keep_flag, drop_reason,
          member_cnt_total, valid_cnt, devices_sum_valid,
          density, wA, wD, simple_score, network_tier_pre,
          reports_sum_total, reports_sum_valid,
          devices_sum_total, mobile_devices_sum_total, mobile_devices_sum_valid,
          wifi_devices_sum_total, wifi_devices_sum_valid,
          vpn_devices_sum_total, vpn_devices_sum_valid,
          wired_devices_sum_total, wired_devices_sum_valid,
          abnormal_net_devices_sum_total, abnormal_net_devices_sum_valid,
          empty_net_devices_sum_total, empty_net_devices_sum_valid,
          worktime_reports_sum_total, worktime_reports_sum_valid,
          workday_reports_sum_total, workday_reports_sum_valid,
          weekend_reports_sum_total, weekend_reports_sum_valid,
          late_night_reports_sum_total, late_night_reports_sum_valid,
          report_density_valid
        )
        SELECT 
           '{RUN_ID}', '{CONTRACT_VERSION}', {shard_id}, block_id_natural,
           keep_flag, drop_reason,
           member_cnt_total, valid_cnt, devices_sum_valid,
           density, wA, wD, simple_score, network_tier_pre,
           reports_sum_total, reports_sum_valid,
           devices_sum_total, mobile_devices_sum_total, mobile_devices_sum_valid,
           wifi_devices_sum_total, wifi_devices_sum_valid,
           vpn_devices_sum_total, vpn_devices_sum_valid,
           wired_devices_sum_total, wired_devices_sum_valid,
           abnormal_net_devices_sum_total, abnormal_net_devices_sum_valid,
           empty_net_devices_sum_total, empty_net_devices_sum_valid,
           worktime_reports_sum_total, worktime_reports_sum_valid,
           workday_reports_sum_total, workday_reports_sum_valid,
           weekend_reports_sum_total, weekend_reports_sum_valid,
           late_night_reports_sum_total, late_night_reports_sum_valid,
           report_density_valid
        FROM tier;
        
        COMMIT;
        """
        
        run_sql(sql)
        log_task_status(shard_id, bucket_id, 'DONE')
        log(f"Shard {shard_id} Bucket {bucket_id}: DONE")
        return True
    except Exception as e:
        log_task_status(shard_id, bucket_id, 'FAILED')
        log(f"Shard {shard_id} Bucket {bucket_id} FAILED: {str(e)}")
        return False

def main():
    log("=== Starting Step 03 Bucket Optimization Test ===")
    
    # 1. Prep
    prep_slim_table()
    
    # Init Staging (Upsert run_id clear)
    log("Clearing profile_pre_stage for run...")
    run_sql(f"DELETE FROM rb20_v2_5.profile_pre_stage WHERE run_id='{RUN_ID}' AND shard_id={TARGET_SHARD}")
    
    # 2. Plan
    generate_bucket_plan()
    
    # 3. Execute
    tasks = run_sql(f"SELECT shard_id, bucket_id FROM rb20_v2_5.step03_task_plan WHERE run_id='{RUN_ID}' AND shard_id={TARGET_SHARD} AND status != 'DONE'", fetch=True)
    
    if not tasks:
        log("No pending tasks for this shard.")
        return

    log(f"Executing {len(tasks)} buckets with concurrency {CONCURRENCY}...")
    start_t = time.time()
    
    with Pool(CONCURRENCY) as p:
        p.map(worker_execute_bucket, tasks)
        
    end_t = time.time()
    log(f"=== Optimization Test Complete in {end_t - start_t:.2f} seconds ===")

if __name__ == "__main__":
    main()
