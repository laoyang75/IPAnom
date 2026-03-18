import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg2


PROJECT_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_DIR / "03_sql"
RUNBOOK_DIR = PROJECT_DIR / "04_runbook"

DEFAULT_BASE_RUN_ID = "rb20v2_20260202_191900_sg_004"
DEFAULT_SOURCE_SHARDS = "143,145"
DEFAULT_CONTRACT_VERSION = "contract_v1"

os.environ.setdefault("PGHOST", "192.168.200.217")
os.environ.setdefault("PGPORT", "5432")
os.environ.setdefault("PGUSER", "postgres")
os.environ.setdefault("PGPASSWORD", "123456")
os.environ.setdefault("PGDATABASE", "ip_loc2")

DB_CONFIG = {
    "host": os.environ["PGHOST"],
    "port": os.environ["PGPORT"],
    "database": os.environ["PGDATABASE"],
    "user": os.environ["PGUSER"],
    "password": os.environ["PGPASSWORD"],
}

SQL_FILES = {
    "run_init": SQL_DIR / "00_contracts/00_run_init.sql",
    "abnormal_dedup": SQL_DIR / "RB20_01/01A_abnormal_dedup.sql",
    "source_members": SQL_DIR / "RB20_01/01_source_members_shard.sql",
    "natural_blocks": SQL_DIR / "RB20_02/02_natural_blocks_shard.sql",
    "post_process": SQL_DIR / "RB20_03/03_post_process.sql",
    "split_final": SQL_DIR / "RB20_04/04_split_and_final_blocks_shard.sql",
    "final_profile": SQL_DIR / "RB20_04P/04P_final_profile_shard.sql",
    "h_blocks": SQL_DIR / "RB20_05/05_h_blocks_and_members.sql",
    "r1_members": SQL_DIR / "RB20_06/06_r1_members_shard.sql",
    "e_atoms": SQL_DIR / "RB20_07/07_e_atoms_runs_members_shard.sql",
    "f_members": SQL_DIR / "RB20_08/08_f_members_shard.sql",
    "qa_assert": SQL_DIR / "RB20_99/99_qa_assert.sql",
}

STEP03_SCRIPT = RUNBOOK_DIR / "orchestrate_step03_bucket_full.py"
STEP11_SCRIPT = RUNBOOK_DIR / "orchestrate_step11_chunked.py"
H_SUMMARY_SCRIPT = RUNBOOK_DIR / "build_h_block_summary.py"
E_SUMMARY_SCRIPT = RUNBOOK_DIR / "build_e_cidr_summary.py"
F_SUMMARY_SCRIPT = RUNBOOK_DIR / "rebuild_f_and_summary.py"


def log(message: str) -> None:
    print(message, flush=True)


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def parse_shards(raw: str):
    shards = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        shards.append(int(item))
    if not shards:
        raise ValueError("source_shards is empty")
    return shards


def render_sql(file_path: Path, replacements: dict) -> str:
    content = file_path.read_text(encoding="utf-8")
    for key, value in replacements.items():
        content = content.replace(key, str(value))
    return content


def exec_sql_text(sql: str, label: str) -> None:
    start = time.perf_counter()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    log(f"[sql] {label}: {time.perf_counter() - start:.2f}s")


def exec_sql_file(file_path: Path, replacements: dict, label: str) -> None:
    exec_sql_text(render_sql(file_path, replacements), label)


def fetch_one(sql: str, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL enable_nestloop = off")
            cur.execute("SET LOCAL work_mem = '256MB'")
            cur.execute("SET LOCAL statement_timeout = '15min'")
            cur.execute(sql, params)
            return cur.fetchone()


def fetch_all(sql: str, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL enable_nestloop = off")
            cur.execute("SET LOCAL work_mem = '256MB'")
            cur.execute("SET LOCAL statement_timeout = '15min'")
            cur.execute(sql, params)
            return cur.fetchall()


def setup_subset_shard_plan(run_id: str, contract_version: str, base_run_id: str, source_shards):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rb20_v2_5.shard_plan WHERE run_id = %s", (run_id,))
            cur.execute(
                """
                WITH src AS (
                  SELECT
                    shard_id AS source_shard_id,
                    ip_long_start,
                    ip_long_end,
                    est_rows,
                    ROW_NUMBER() OVER (ORDER BY shard_id) - 1 AS new_shard_id
                  FROM rb20_v2_5.shard_plan
                  WHERE run_id = %s
                    AND shard_id = ANY(%s::smallint[])
                  ORDER BY shard_id
                )
                INSERT INTO rb20_v2_5.shard_plan(
                  run_id, contract_version, shard_id,
                  ip_long_start, ip_long_end, est_rows, plan_round
                )
                SELECT
                  %s, %s, new_shard_id::smallint,
                  ip_long_start, ip_long_end, est_rows, 0::smallint
                FROM src
                RETURNING shard_id, ip_long_start, ip_long_end, est_rows
                """,
                (base_run_id, source_shards, run_id, contract_version),
            )
            inserted = cur.fetchall()
    if len(inserted) != len(source_shards):
        raise RuntimeError(
            f"subset shard_plan insert mismatch: expected {len(source_shards)}, got {len(inserted)}"
        )
    return inserted


def run_python_script(script_path: Path, env: dict, label: str):
    start = time.perf_counter()
    subprocess.run([sys.executable, str(script_path)], check=True, env=env, cwd=str(PROJECT_DIR))
    elapsed = time.perf_counter() - start
    log(f"[py] {label}: {elapsed:.2f}s")
    return elapsed


def validate_mainline(run_id: str):
    sql = f"""
    WITH h_overlap_e AS (
      SELECT COUNT(*)::bigint AS cnt
      FROM rb20_v2_5.h_members h
      JOIN rb20_v2_5.e_members e ON e.run_id=h.run_id AND e.ip_long=h.ip_long
      WHERE h.run_id = '{run_id}'
    ),
    h_overlap_f AS (
      SELECT COUNT(*)::bigint AS cnt
      FROM rb20_v2_5.h_members h
      JOIN rb20_v2_5.f_members f ON f.run_id=h.run_id AND f.ip_long=h.ip_long
      WHERE h.run_id = '{run_id}'
    ),
    e_overlap_f AS (
      SELECT COUNT(*)::bigint AS cnt
      FROM rb20_v2_5.e_members e
      JOIN rb20_v2_5.f_members f ON f.run_id=e.run_id AND f.ip_long=e.ip_long
      WHERE e.run_id = '{run_id}'
    ),
    keep_cnt AS (
      SELECT COALESCE((
        SELECT SUM(metric_value_numeric)::bigint
        FROM rb20_v2_5.step_stats
        WHERE run_id = '{run_id}'
          AND step_id = 'RB20_03'
          AND metric_name = 'keep_member_cnt'
      ), (
        SELECT COUNT(*)::bigint
        FROM rb20_v2_5.keep_members
        WHERE run_id = '{run_id}'
      )) AS cnt
    ),
    h_cnt AS (
      SELECT COALESCE((
        SELECT metric_value_numeric::bigint
        FROM rb20_v2_5.core_numbers
        WHERE run_id = '{run_id}' AND metric_name = 'h_member_cnt'
      ), (
        SELECT COUNT(*)::bigint
        FROM rb20_v2_5.h_members
        WHERE run_id = '{run_id}'
      )) AS cnt
    ),
    r1_cnt AS (
      SELECT COALESCE((
        SELECT SUM(metric_value_numeric)::bigint
        FROM rb20_v2_5.step_stats
        WHERE run_id = '{run_id}'
          AND step_id = 'RB20_06'
          AND metric_name = 'r1_member_cnt'
      ), (
        SELECT COUNT(*)::bigint
        FROM rb20_v2_5.r1_members
        WHERE run_id = '{run_id}'
      )) AS cnt
    ),
    e_cnt AS (
      SELECT COALESCE((
        SELECT SUM(metric_value_numeric)::bigint
        FROM rb20_v2_5.step_stats
        WHERE run_id = '{run_id}'
          AND step_id = 'RB20_07'
          AND metric_name = 'e_member_cnt'
      ), (
        SELECT COUNT(*)::bigint
        FROM rb20_v2_5.e_members
        WHERE run_id = '{run_id}'
      )) AS cnt
    ),
    f_cnt AS (
      SELECT COALESCE((
        SELECT SUM(metric_value_numeric)::bigint
        FROM rb20_v2_5.step_stats
        WHERE run_id = '{run_id}'
          AND step_id = 'RB20_08'
          AND metric_name = 'f_member_cnt'
      ), (
        SELECT COUNT(*)::bigint
        FROM rb20_v2_5.f_members
        WHERE run_id = '{run_id}'
      )) AS cnt
    ),
    hef_cnt AS (
      SELECT ((SELECT cnt FROM h_cnt) + (SELECT cnt FROM e_cnt) + (SELECT cnt FROM f_cnt)) AS cnt
    ),
    valid0_h AS (
      SELECT COUNT(*)::bigint AS cnt
      FROM rb20_v2_5.h_blocks
      WHERE run_id = '{run_id}' AND valid_cnt = 0
    ),
    h_lt4 AS (
      SELECT COUNT(*)::bigint AS cnt
      FROM rb20_v2_5.h_blocks
      WHERE run_id = '{run_id}' AND member_cnt_total < 4
    ),
    h_valid_lt4 AS (
      SELECT COUNT(*)::bigint AS cnt
      FROM rb20_v2_5.h_blocks
      WHERE run_id = '{run_id}' AND valid_cnt < 4
    ),
    short_run_e AS (
      SELECT COUNT(*)::bigint AS cnt
      FROM rb20_v2_5.e_members em
      JOIN rb20_v2_5.e_runs er
        ON er.run_id = em.run_id
       AND er.shard_id = em.shard_id
       AND er.e_run_id = em.e_run_id
      WHERE em.run_id = '{run_id}'
        AND er.short_run = true
    ),
qa_fail AS (
  SELECT COUNT(*)::bigint AS cnt
  FROM rb20_v2_5.qa_assert
  WHERE run_id = '{run_id}' AND pass_flag = false AND severity = 'STOP'
)
    SELECT
      (SELECT cnt FROM h_overlap_e) AS h_overlap_e,
      (SELECT cnt FROM h_overlap_f) AS h_overlap_f,
      (SELECT cnt FROM e_overlap_f) AS e_overlap_f,
      (SELECT cnt FROM keep_cnt) AS keep_cnt,
      (SELECT cnt FROM h_cnt) AS h_cnt,
      (SELECT cnt FROM r1_cnt) AS r1_cnt,
      (SELECT cnt FROM e_cnt) AS e_cnt,
      (SELECT cnt FROM f_cnt) AS f_cnt,
      (SELECT cnt FROM hef_cnt) AS hef_cnt,
      (SELECT cnt FROM valid0_h) AS valid0_h,
      (SELECT cnt FROM h_lt4) AS h_lt4,
      (SELECT cnt FROM h_valid_lt4) AS h_valid_lt4,
      (SELECT cnt FROM short_run_e) AS short_run_e,
      (SELECT cnt FROM qa_fail) AS qa_fail
    ;
    """
    row = fetch_one(sql)
    keys = [
        "h_overlap_e",
        "h_overlap_f",
        "e_overlap_f",
        "keep_cnt",
        "h_cnt",
        "r1_cnt",
        "e_cnt",
        "f_cnt",
        "hef_cnt",
        "valid0_h",
        "h_lt4",
        "h_valid_lt4",
        "short_run_e",
        "qa_fail",
    ]
    result = dict(zip(keys, row))
    result["keep_equals_hef"] = result["keep_cnt"] == result["hef_cnt"]
    result["keep_equals_h_plus_r1"] = result["keep_cnt"] == (result["h_cnt"] + result["r1_cnt"])
    result["r1_equals_e_plus_f"] = result["r1_cnt"] == (result["e_cnt"] + result["f_cnt"])
    return result


def validate_summary(run_id: str):
    sql = f"""
    SELECT
      (SELECT COUNT(*) FROM rb20_v2_5.h_blocks WHERE run_id = '{run_id}') AS h_blocks,
      (SELECT COUNT(*) FROM rb20_v2_5.h_block_summary WHERE run_id = '{run_id}') AS h_summary,
      (SELECT COUNT(*) FROM rb20_v2_5.e_runs WHERE run_id = '{run_id}') AS e_runs,
      (SELECT COUNT(*) FROM rb20_v2_5.e_cidr_summary WHERE run_id = '{run_id}') AS e_summary,
      (SELECT COUNT(*) FROM rb20_v2_5.f_members WHERE run_id = '{run_id}') AS f_members,
      (SELECT COUNT(*) FROM rb20_v2_5.f_ip_summary WHERE run_id = '{run_id}') AS f_summary
    ;
    """
    row = fetch_one(sql)
    keys = ["h_blocks", "h_summary", "e_runs", "e_summary", "f_members", "f_summary"]
    return dict(zip(keys, row))


def main():
    parser = argparse.ArgumentParser(description="Run a subset validation pipeline for RB20 v2.5.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--base-run-id", default=DEFAULT_BASE_RUN_ID)
    parser.add_argument("--source-shards", default=DEFAULT_SOURCE_SHARDS)
    parser.add_argument("--contract-version", default=DEFAULT_CONTRACT_VERSION)
    parser.add_argument("--with-summary", action="store_true")
    args = parser.parse_args()

    source_shards = parse_shards(args.source_shards)
    shard_cnt = len(source_shards)
    env = os.environ.copy()
    env["RUN_ID"] = args.run_id
    env["CONTRACT_VERSION"] = args.contract_version
    env["SHARD_CNT"] = str(shard_cnt)

    timings = {}
    overall_start = time.perf_counter()

    exec_sql_file(
        SQL_FILES["run_init"],
        {
            "{{run_id}}": args.run_id,
            "{{contract_version}}": args.contract_version,
        },
        "run_init",
    )

    inserted = setup_subset_shard_plan(
        args.run_id,
        args.contract_version,
        args.base_run_id,
        source_shards,
    )
    log(f"[plan] subset shard_plan: {json.dumps(inserted, ensure_ascii=False)}")

    exec_sql_file(
        SQL_FILES["abnormal_dedup"],
        {
            "{{run_id}}": args.run_id,
            "{{contract_version}}": args.contract_version,
        },
        "01A_abnormal_dedup",
    )

    for shard_id in range(shard_cnt):
        replacements = {
            "{{run_id}}": args.run_id,
            "{{contract_version}}": args.contract_version,
            "{{shard_id}}": str(shard_id),
        }
        exec_sql_file(SQL_FILES["source_members"], replacements, f"01_source_members_shard_{shard_id}")
        exec_sql_file(SQL_FILES["natural_blocks"], replacements, f"02_natural_blocks_shard_{shard_id}")

    timings["step03"] = run_python_script(STEP03_SCRIPT, env, "step03_bucket_full")

    for shard_id in range(shard_cnt):
        replacements = {
            "{{run_id}}": args.run_id,
            "{{contract_version}}": args.contract_version,
            "{{shard_id}}": str(shard_id),
        }
        exec_sql_file(SQL_FILES["post_process"], replacements, f"03_post_process_shard_{shard_id}")

    timings["step11"] = run_python_script(STEP11_SCRIPT, env, "step11_chunked")

    for shard_id in range(shard_cnt):
        replacements = {
            "{{run_id}}": args.run_id,
            "{{contract_version}}": args.contract_version,
            "{{shard_id}}": str(shard_id),
        }
        exec_sql_file(SQL_FILES["split_final"], replacements, f"04_split_final_shard_{shard_id}")
        exec_sql_file(SQL_FILES["final_profile"], replacements, f"04P_final_profile_shard_{shard_id}")

    exec_sql_file(
        SQL_FILES["h_blocks"],
        {
            "{{run_id}}": args.run_id,
            "{{contract_version}}": args.contract_version,
        },
        "05_h_blocks_and_members",
    )

    for shard_id in range(shard_cnt):
        replacements = {
            "{{run_id}}": args.run_id,
            "{{contract_version}}": args.contract_version,
            "{{shard_id}}": str(shard_id),
        }
        exec_sql_file(SQL_FILES["r1_members"], replacements, f"06_r1_members_shard_{shard_id}")
        exec_sql_file(SQL_FILES["e_atoms"], replacements, f"07_e_atoms_shard_{shard_id}")
        exec_sql_file(SQL_FILES["f_members"], replacements, f"08_f_members_shard_{shard_id}")

    exec_sql_file(
        SQL_FILES["qa_assert"],
        {
            "{{run_id}}": args.run_id,
            "{{contract_version}}": args.contract_version,
            "{{shard_cnt}}": str(shard_cnt),
        },
        "99_qa_assert",
    )

    main_validation = validate_mainline(args.run_id)
    log(f"[validate-main] {json.dumps(main_validation, ensure_ascii=False)}")

    if args.with_summary:
        timings["build_h_block_summary"] = run_python_script(H_SUMMARY_SCRIPT, env, "build_h_block_summary")
        timings["build_e_cidr_summary"] = run_python_script(E_SUMMARY_SCRIPT, env, "build_e_cidr_summary")
        timings["rebuild_f_and_summary"] = run_python_script(F_SUMMARY_SCRIPT, env, "rebuild_f_and_summary")
        summary_validation = validate_summary(args.run_id)
        log(f"[validate-summary] {json.dumps(summary_validation, ensure_ascii=False)}")

    timings["total"] = time.perf_counter() - overall_start
    log(f"[timings] {json.dumps(timings, ensure_ascii=False)}")

    if any(
        [
            main_validation["h_overlap_e"] != 0,
            main_validation["h_overlap_f"] != 0,
            main_validation["e_overlap_f"] != 0,
            not main_validation["keep_equals_hef"],
            not main_validation["keep_equals_h_plus_r1"],
            not main_validation["r1_equals_e_plus_f"],
            main_validation["valid0_h"] != 0,
            main_validation["h_lt4"] != 0,
            main_validation["h_valid_lt4"] != 0,
            main_validation["short_run_e"] != 0,
            main_validation["qa_fail"] != 0,
        ]
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
