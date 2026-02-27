import os
import subprocess
from datetime import datetime

# Database Connection Info
os.environ["PGHOST"] = "192.168.200.217"
os.environ["PGPORT"] = "5432"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "123456"
os.environ["PGDATABASE"] = "ip_loc2"
os.environ["PGCLIENTENCODING"] = "UTF8"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_diagnostics():
    sql = r"""
    SELECT 
        "IP归属国家", 
        encode("IP归属国家"::bytea, 'hex') as hex_val, 
        count(*) 
    FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1" 
    WHERE "IP归属国家" LIKE '%中国%' OR "IP归属国家" = '中国'
    GROUP BY 1, 2;
    """
    
    cmd = f'psql -c "{sql}"'
    log(f"Running diagnostics: {cmd}")
    try:
        output = subprocess.check_output(cmd, shell=True).decode('utf-8')
        log("Output:")
        print(output)
    except Exception as e:
        log(f"Error: {e}")

if __name__ == "__main__":
    run_diagnostics()
