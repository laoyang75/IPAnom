import os
import subprocess
import sys
import multiprocessing
import time
from datetime import datetime
from pathlib import Path

import psycopg2


RUN_ID = os.getenv("RUN_ID", "rb20v2_20260313_200300_sg_dynamic_fix04")
CONTRACT_VERSION = os.getenv("CONTRACT_VERSION", "contract_v1")
PHASE7_CONCURRENCY = int(os.getenv("PHASE7_CONCURRENCY", "4"))

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
    "password": os.environ["PGPASSWORD"],
}

RUNBOOK_DIR = str(Path(__file__).resolve().parent)
BASE_DIR = str(Path(RUNBOOK_DIR).parent / "03_sql")

SQL_FILES = {
    "p7_06": f"{BASE_DIR}/RB20_06/06_r1_members_shard.sql",
    "p7_07": f"{BASE_DIR}/RB20_07/07_e_atoms_runs_members_shard.sql",
    "p7_08": f"{BASE_DIR}/RB20_08/08_f_members_shard.sql",
    "qa": f"{BASE_DIR}/RB20_99/99_qa_assert.sql",
}


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def get_shard_ids():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT shard_id FROM rb20_v2_5.shard_plan WHERE run_id=%s ORDER BY shard_id",
        (RUN_ID,),
    )
    rows = [row[0] for row in cur.fetchall()]
    conn.close()
    return rows


def get_finished_phase7_shards():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT shard_id
        FROM rb20_v2_5.step_stats
        WHERE run_id=%s AND step_id='RB20_08'
        ORDER BY shard_id
        """,
        (RUN_ID,),
    )
    rows = [row[0] for row in cur.fetchall()]
    conn.close()
    return rows


def exec_sql_file(file_path, replacements, description=""):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    for k, v in replacements.items():
        content = content.replace(k, str(v))

    temp_filename = f"/tmp/rb20_exec_{int(time.time()*1000)}_{os.getpid()}.sql"
    with open(temp_filename, "w", encoding="utf-8") as f:
        f.write(content)

    try:
        subprocess.check_output(
            f"psql -X -v ON_ERROR_STOP=1 -f {temp_filename}",
            shell=True,
            stderr=subprocess.STDOUT,
        )
        log(f"DONE: {description}")
    except subprocess.CalledProcessError as e:
        output = e.output.decode("utf-8", errors="replace")
        log(f"FAILED: {description}")
        log(output)
        raise
    finally:
        if os.path.exists(temp_filename):
            os.remove(temp_filename)


def worker_phase7(shard_id):
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": CONTRACT_VERSION,
        "{{shard_id}}": str(shard_id),
    }
    try:
        exec_sql_file(SQL_FILES["p7_06"], replacements, f"Shard {shard_id}: 06 R1 Members")
        exec_sql_file(SQL_FILES["p7_07"], replacements, f"Shard {shard_id}: 07 E Atoms")
        exec_sql_file(SQL_FILES["p7_08"], replacements, f"Shard {shard_id}: 08 F Members")
        return (True, shard_id, None)
    except Exception as e:
        return (False, shard_id, str(e))


def main():
    log(f"=== Phase 7 Resume Start (Run: {RUN_ID}) ===")
    all_shards = get_shard_ids()
    if not all_shards:
        log("STOP: shard_plan missing.")
        sys.exit(1)
    finished = set(get_finished_phase7_shards())
    shards = [sid for sid in all_shards if sid not in finished]
    log(
        f"Phase 7 shard summary: total={len(all_shards)}, "
        f"finished={len(finished)}, pending={len(shards)}"
    )
    if not shards:
        log("No pending shards in Phase 7. Running QA only.")
        exec_sql_file(
            SQL_FILES["qa"],
            {
                "{{run_id}}": RUN_ID,
                "{{contract_version}}": CONTRACT_VERSION,
                "{{shard_cnt}}": str(len(all_shards)),
            },
            "QA Assert",
        )
        log("=== Phase 7 Resume Success ===")
        return

    log(f"Running Phase 7 for {len(shards)} shards with concurrency {PHASE7_CONCURRENCY}...")
    with multiprocessing.Pool(PHASE7_CONCURRENCY) as p:
        results = p.map(worker_phase7, shards)

    failures = [r for r in results if not r[0]]
    if failures:
        log(f"STOP: {len(failures)} shards failed in Phase 7.")
        for ok, shard_id, err in failures[:20]:
            log(f"Shard {shard_id} Error: {err}")
        sys.exit(1)

    log("Phase 7 complete. Running QA...")
    exec_sql_file(
        SQL_FILES["qa"],
        {
            "{{run_id}}": RUN_ID,
            "{{contract_version}}": CONTRACT_VERSION,
            "{{shard_cnt}}": str(len(all_shards)),
        },
        "QA Assert",
    )
    log("=== Phase 7 Resume Success ===")


if __name__ == "__main__":
    main()
