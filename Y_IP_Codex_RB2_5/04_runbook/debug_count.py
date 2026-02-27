import os
import subprocess
from datetime import datetime

# Database Connection Info
os.environ["PGHOST"] = "192.168.200.217"
os.environ["PGPORT"] = "5432"
os.environ["PGUSER"] = "postgres"
os.environ["PGPASSWORD"] = "123456"
os.environ["PGDATABASE"] = "ip_loc2"
# Do NOT force UTF8 yet, let's see default behavior or try to detect
# os.environ["PGCLIENTENCODING"] = "UTF8"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def run_cmd(cmd):
    log(f"CMD: {cmd}")
    try:
        output = subprocess.check_output(cmd, shell=True).decode('utf-8')
        log(f"OUTPUT:\n{output}")
    except subprocess.CalledProcessError as e:
        log(f"ERROR: {e}")
        log(f"OUTPUT: {e.output.decode('utf-8')}")

if __name__ == "__main__":
    # 1. Check Total Count
    run_cmd('psql -c "SELECT count(*) FROM public.\"ip库构建项目_ip源表_20250811_20250824_v2_1\""')
    
    # 2. Check China Count (Plain)
    run_cmd('psql -c "SELECT count(*) FROM public.\"ip库构建项目_ip源表_20250811_20250824_v2_1\" WHERE \"IP归属国家\" = \'中国\'"')

    # 3. Check China Count (Hex)
    run_cmd("psql -c \"SELECT count(*) FROM public.\\\"ip库构建项目_ip源表_20250811_20250824_v2_1\\\" WHERE \\\"IP归属国家\\\" = convert_from('\\xe4b8ade59bbd'::bytea, 'UTF8')\"")
