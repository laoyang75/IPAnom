import os
import subprocess
import time
from datetime import datetime

# ==========================================
# Configuration & Constants
# ==========================================

RUN_ID = "rb20v2_20260107_194500_sg_001"
CONTRACT_VERSION = "contract_v1"

# Database Connection Info
os.environ["PGHOST"] = "192.168.200.217"
os.environ["PGPORT"] = "5432"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "123456"
os.environ["PGDATABASE"] = "ip_loc2"
os.environ["PGCLIENTENCODING"] = "UTF8"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_cmd(cmd, shell=True):
    try:
        subprocess.check_output(cmd, shell=shell, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        log(f"ERROR executing command: {cmd}")
        log(f"Output: {e.output.decode('utf-8')}")
        raise e

# Fully embedded SQL to avoid file reading issues
# Updated Assertions to avoid Constant Folding optimization 1/0
SQL_CONTENT = r"""
\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS rb20_v2_5;

-- 0) 幂等清理
DELETE FROM rb20_v2_5.shard_plan WHERE run_id = '{run_id}';

-- 1) 参数与数据范围（Hex Safe China Filter）
DROP TABLE IF EXISTS sp_params;
CREATE TEMP TABLE sp_params AS
SELECT
  '{run_id}'::text AS run_id,
  '{contract_version}'::text AS contract_version,
  0.10::float8 AS eps,
  COUNT(*)::bigint AS total_rows,
  MIN(ip_long)::bigint AS min_ip,
  (MAX(ip_long) + 1)::bigint AS max_excl
FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
WHERE "IP归属国家" = convert_from('\xe4b8ade59bbd'::bytea, 'UTF8');

SELECT total_rows FROM sp_params;

-- 断言: total_rows >= 64
SELECT 1 / (CASE WHEN (SELECT total_rows FROM sp_params) >= 64 THEN 1 ELSE 0 END) AS assert_rows_ge_64;

-- 2) 64 分位切分
DROP TABLE IF EXISTS sp_agg;
CREATE TEMP TABLE sp_agg (
  shard_id int PRIMARY KEY,
  ip_min bigint NOT NULL,
  cnt bigint NOT NULL
);

INSERT INTO sp_agg(shard_id, ip_min, cnt)
WITH base AS (
  SELECT ip_long
  FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
  WHERE "IP归属国家" = convert_from('\xe4b8ade59bbd'::bytea, 'UTF8')
),
tiled AS (
  SELECT
    ip_long,
    (NTILE(64) OVER (ORDER BY ip_long) - 1)::int AS shard_id
  FROM base
)
SELECT
  shard_id,
  MIN(ip_long)::bigint AS ip_min,
  COUNT(*)::bigint AS cnt
FROM tiled
GROUP BY 1
ORDER BY 1;

-- 断言
SELECT 1 / (CASE WHEN (SELECT COUNT(*) FROM sp_agg) = 64 THEN 1 ELSE 0 END) AS assert_shard_cnt_64;
SELECT 1 / (CASE WHEN NOT EXISTS (SELECT 1 FROM sp_agg WHERE cnt = 0) THEN 1 ELSE 0 END) AS assert_no_empty_shard;

-- 3) 生成连续不重叠区间
DROP TABLE IF EXISTS sp_bounds;
CREATE TEMP TABLE sp_bounds (
  shard_id int PRIMARY KEY,
  ip_long_start bigint NOT NULL,
  ip_long_end bigint NOT NULL
);

INSERT INTO sp_bounds(shard_id, ip_long_start, ip_long_end)
SELECT
  a.shard_id,
  a.ip_min AS ip_long_start,
  COALESCE(
    LEAD(a.ip_min) OVER (ORDER BY a.shard_id),
    (SELECT max_excl FROM sp_params)
  ) AS ip_long_end
FROM sp_agg a
ORDER BY a.shard_id;

-- 断言
SELECT 1 / (CASE WHEN NOT EXISTS (SELECT 1 FROM sp_bounds WHERE ip_long_start >= ip_long_end) THEN 1 ELSE 0 END) AS assert_nonempty_range;

-- 4) 写入 shard_plan
INSERT INTO rb20_v2_5.shard_plan(
  run_id, contract_version, shard_id,
  ip_long_start, ip_long_end,
  est_rows, plan_round, created_at
)
SELECT
  p.run_id,
  p.contract_version,
  b.shard_id::smallint,
  b.ip_long_start,
  b.ip_long_end,
  a.cnt,
  0::smallint AS plan_round,
  now()
FROM sp_bounds b
JOIN sp_agg a USING (shard_id)
CROSS JOIN sp_params p
ORDER BY b.shard_id;
"""

def exec_sql_embedded():
    formatted_sql = SQL_CONTENT.format(run_id=RUN_ID, contract_version=CONTRACT_VERSION)
    
    temp_filename = f"/tmp/rb20_fix_v4_{int(time.time()*1000)}.sql"
    with open(temp_filename, 'w', encoding='utf-8') as f:
        f.write(formatted_sql)
        
    try:
        log(f"Executing RB20_00D fix v4 (Improved Assertions) with RUN_ID={RUN_ID}...")
        run_cmd(f"psql -a -v ON_ERROR_STOP=1 -f {temp_filename}")
        log("Execution successful.")
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def verify():
    log("Verifying results...")
    cmd_cnt = f"psql -t -c \"SELECT COUNT(*) FROM rb20_v2_5.shard_plan WHERE run_id='{RUN_ID}';\""
    cnt = subprocess.check_output(cmd_cnt, shell=True).decode('utf-8').strip()
    
    cmd_empty = f"psql -t -c \"SELECT COUNT(*) FROM rb20_v2_5.shard_plan WHERE run_id='{RUN_ID}' AND COALESCE(est_rows,0)=0;\""
    empty = subprocess.check_output(cmd_empty, shell=True).decode('utf-8').strip()
    
    log(f"Shard Count: {cnt} (Expected: 64)")
    log(f"Empty Shards: {empty} (Expected: 0)")
    
    if cnt and int(cnt) == 64 and empty and int(empty) == 0:
        log("VALIDATION PASS.")
    else:
        log("VALIDATION FAIL.")
        exit(1)

if __name__ == "__main__":
    exec_sql_embedded()
    verify()
