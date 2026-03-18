
import os
import time
import psycopg2
import math
import sys
from multiprocessing import Pool, cpu_count
import logging

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

# Run Configuration
RUN_ID = os.getenv("RUN_ID", "rb20v2_20260202_191900_sg_001")
CONTRACT_VERSION = os.getenv("CONTRACT_VERSION", "contract_v1")
SHARD_CNT = int(os.getenv("SHARD_CNT", "64"))
TARGET_ROWS_PER_BUCKET = 200000
CONCURRENCY = 8  # Global concurrency for the script

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


def get_shard_ids():
    """Load actual shard ids for the target run from shard_plan."""
    rows = run_sql(
        "SELECT shard_id FROM rb20_v2_5.shard_plan WHERE run_id=%s ORDER BY shard_id",
        (RUN_ID,),
        fetch=True,
    )
    return [row[0] for row in rows]


def clear_run_state():
    log("Clearing step03 transient state for run...")
    run_sql(f"DELETE FROM rb20_v2_5.step03_task_plan WHERE run_id='{RUN_ID}'")
    run_sql(f"DELETE FROM rb20_v2_5.step03_block_bucket WHERE run_id='{RUN_ID}'")
    run_sql(f"DELETE FROM rb20_v2_5.profile_pre_stage WHERE run_id='{RUN_ID}'")

def log_task_status(shard_id, bucket_id, status):
    # Use a fresh connection for status updates
    try:
        sql = """
        UPDATE rb20_v2_5.step03_task_plan 
        SET status = %s, 
            started_at = CASE WHEN %s = 'RUNNING' THEN now() ELSE started_at END,
            finished_at = CASE WHEN %s = 'DONE' THEN now() ELSE finished_at END
        WHERE run_id = %s AND shard_id = %s AND bucket_id = %s
        """
        run_sql(sql, (status, status, status, RUN_ID, shard_id, bucket_id))
    except Exception as e:
        log(f"Error logging status for {shard_id}/{bucket_id}: {e}")

def prep_slim_table():
    log("Checking/Prepping source_members_slim table...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    
    # Check if table exists
    cur.execute("SELECT 1 FROM pg_tables WHERE schemaname='rb20_v2_5' AND tablename='source_members_slim'")
    table_exists = cur.fetchone()
    
    if table_exists:
        # FIX: Check if table has data for current run_id
        cur.execute(f"SELECT count(*) FROM rb20_v2_5.source_members_slim WHERE run_id='{RUN_ID}'")
        row_count = cur.fetchone()[0]
        if row_count > 0:
            log(f"source_members_slim exists with {row_count} rows for this run. Using existing table.")
            conn.close()
            return
        else:
            log("source_members_slim exists but is empty for this run_id. Rebuilding...")
            cur.execute("DROP TABLE rb20_v2_5.source_members_slim")

    log("Creating UNLOGGED source_members_slim (Global)...")
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

def generate_bucket_plan_for_shard(shard_id, conn):
    cur = conn.cursor()
    # 1. Total Rows
    cur.execute("SELECT count(*) FROM rb20_v2_5.step03_task_plan WHERE run_id=%s AND shard_id=%s", (RUN_ID, shard_id))
    if cur.fetchone()[0] > 0:
        return # Already planned

    # 2. Get total block members to calculate buckets
    sql_total = f"""
    SELECT sum(cnt) FROM (
        SELECT count(*) as cnt
        FROM rb20_v2_5.map_member_block_natural
        WHERE run_id='{RUN_ID}' AND shard_id={shard_id}
        GROUP BY block_id_natural
    ) t
    """
    cur.execute(sql_total)
    res = cur.fetchone()
    total_rows = res[0] if res and res[0] else 0
    
    if total_rows == 0:
        log(f"Shard {shard_id}: Empty (0 rows).")
        return

    est_buckets = max(1, math.ceil(total_rows / TARGET_ROWS_PER_BUCKET))
    log(f"Shard {shard_id}: {total_rows} rows -> {est_buckets} buckets (SQL Plan).")
    
    # 3. SQL-Based Partitioning (Cumulative Sum / Width Bucket)
    # Using width_bucket on cum_sum to assign buckets 1..N
    # ordered by cnt DESC to pack large blocks first (heuristic)
    sql_plan = f"""
    WITH agg AS (
        SELECT block_id_natural, count(*) as cnt
        FROM rb20_v2_5.map_member_block_natural
        WHERE run_id='{RUN_ID}' AND shard_id={shard_id}
        GROUP BY 1
    ),
    cum AS (
        SELECT block_id_natural, cnt, 
               sum(cnt) OVER (ORDER BY cnt DESC, block_id_natural) as running_sum
        FROM agg
    ),
    total AS ( SELECT {total_rows}::numeric as tot )
    INSERT INTO rb20_v2_5.step03_block_bucket (run_id, shard_id, block_id_natural, bucket_id, est_members)
    SELECT '{RUN_ID}', {shard_id}, block_id_natural, 
           width_bucket(running_sum, 0, (select tot+1 from total), {est_buckets}),
           cnt
    FROM cum;
    """
    cur.execute(sql_plan)

    # 4. Create Task Plan Entries
    sql_tasks = f"""
    INSERT INTO rb20_v2_5.step03_task_plan (run_id, shard_id, bucket_id, est_member_rows, est_block_cnt)
    SELECT '{RUN_ID}', {shard_id}, bucket_id, sum(est_members), count(*)
    FROM rb20_v2_5.step03_block_bucket
    WHERE run_id='{RUN_ID}' AND shard_id={shard_id}
    GROUP BY bucket_id;
    """
    cur.execute(sql_tasks)
    conn.commit()

def worker_execute_bucket(args):
    shard_id, bucket_id = args
    try:
        log_task_status(shard_id, bucket_id, 'RUNNING')
        
        # SQL Template (Verified + Fix)
        sql = f"""
        BEGIN;
        SET LOCAL enable_hashagg = on;
        SET LOCAL enable_nestloop = off; -- Force Hash Join to avoid hanging on 100k+ tiny blocks
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
        return True
    except Exception as e:
        log_task_status(shard_id, bucket_id, 'FAILED')
        log(f"Shard {shard_id} Bucket {bucket_id} FAILED: {str(e)}")
        return False

def commit_to_final():
    log("Committing results to final table rb20_v2_5.profile_pre...")
    run_sql(f"DELETE FROM rb20_v2_5.profile_pre WHERE run_id='{RUN_ID}'")
    
    # Bulk insert
    sql = f"""
    INSERT INTO rb20_v2_5.profile_pre 
    SELECT * FROM rb20_v2_5.profile_pre_stage 
    WHERE run_id='{RUN_ID}'
    """
    run_sql(sql)
    
    log("Recording Step Stats...")
    run_sql(f"""
    INSERT INTO rb20_v2_5.step_stats(run_id, contract_version, step_id, shard_id, metric_name, metric_value_numeric)
    SELECT run_id, contract_version, 'RB20_03', shard_id, 'pre_profile_rows', count(*)
    FROM rb20_v2_5.profile_pre
    WHERE run_id='{RUN_ID}'
    GROUP BY 1,2,3,4
    ON CONFLICT DO NOTHING
    """)
    log("Commit complete.")

def main():
    log(f"=== Starting Step 03 FULL Optimization Rollout (Run: {RUN_ID}) ===")
    
    # 1. Prep
    clear_run_state()
    prep_slim_table()
    
    # FIX: Validate slim table has data
    count_result = run_sql(f"SELECT count(*) FROM rb20_v2_5.source_members_slim WHERE run_id='{RUN_ID}'", fetch=True)
    slim_count = count_result[0][0] if count_result else 0
    if slim_count == 0:
        log(f"ERROR: source_members_slim is empty for run_id={RUN_ID}. Cannot proceed.")
        sys.exit(1)
    log(f"Verified: source_members_slim has {slim_count} rows")
    
    # 2. Plan Generation (Phase A) - SQL Based
    log("Generating Bucket Plans for all shards (SQL Based)...")
    shards = get_shard_ids()
    if not shards:
        log(f"ERROR: No shard ids found in shard_plan for run_id={RUN_ID}.")
        sys.exit(1)
    
    conn = get_db_conn()
    for sid in shards:
        try:
            generate_bucket_plan_for_shard(sid, conn)
        except Exception as e:
            log(f"Plan Gen Error Shard {sid}: {e}")
    conn.close()
    log("Plan generation complete.")
    
    # 3. Execution (Phase B)
    tasks = run_sql(f"SELECT shard_id, bucket_id FROM rb20_v2_5.step03_task_plan WHERE run_id='{RUN_ID}' AND status != 'DONE' ORDER BY est_member_rows DESC", fetch=True)
    
    if not tasks:
        log("No pending tasks found.")
    else:
        log(f"Executing {len(tasks)} buckets across {len(shards)} shards (Concurrency: {CONCURRENCY})...")
        start_t = time.time()
        
        with Pool(CONCURRENCY) as p:
            for i, _ in enumerate(p.imap_unordered(worker_execute_bucket, tasks), 1):
                if i % 20 == 0:
                    log(f"Progress: {i}/{len(tasks)} buckets completed.")
                    
        end_t = time.time()
        log(f"Bucket execution complete in {end_t - start_t:.2f} seconds.")

    # 4. Commit (Phase C)
    commit_to_final()
    log("=== Full Rollout Complete ===")

if __name__ == "__main__":
    main()
