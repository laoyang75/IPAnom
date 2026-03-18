
import os
import psycopg2
import sys
from pathlib import Path

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
RUN_ID = os.getenv("RUN_ID", "rb20v2_20260202_191900_sg_001")
SQL_FILE = str(Path(__file__).resolve().parent.parent / "03_sql" / "00_cleanup_v2.sql")

def main():
    print(f"Executing Cleanup for {RUN_ID}...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    with open(SQL_FILE, 'r', encoding='utf-8') as f:
        sql_template = f.read()
    
    sql = sql_template.replace("{{run_id}}", RUN_ID)
    
    # Execute statement by statement to see impacts
    statements = sql.split(";")
    total_deleted = 0
    
    for stmt in statements:
        if not stmt.strip(): continue
        try:
            cur.execute(stmt)
            conn.commit()  # Commit immediately after each statement
            if cur.rowcount > 0:
                print(f"Deleted {cur.rowcount} rows: {stmt.strip().splitlines()[0]}")
                total_deleted += cur.rowcount
        except Exception as e:
            print(f"Skipping (error): {e}")
            conn.rollback()  # Only rollback the failed statement
            continue
            
    conn.close()
    print(f"Cleanup Complete. Total rows deleted: {total_deleted}")

if __name__ == "__main__":
    main()
