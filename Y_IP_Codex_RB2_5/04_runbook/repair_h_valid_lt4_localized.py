"""
Localized repair for H blocks with valid_cnt < 4.

Business intent:
1. Remove invalid H blocks whose valid IP coverage is below the H threshold.
2. Release their members back to the residue flow.
3. Only rerun E where the released IPs can affect local /27 atoms or nearby E runs.
4. Rebuild F only for impacted shards.

This is intentionally smaller than a full rerun:
- H is repaired globally only for the bad blocks.
- RB20_06 / RB20_08 rerun only on impacted shards.
- RB20_07 reruns only on shards that can affect E.
- E/F summaries rebuild only on the same local shard scope.
"""
import os
import subprocess
import sys
import multiprocessing
import time
from datetime import datetime
from pathlib import Path

import psycopg2


RUN_ID = os.getenv("RUN_ID", "rb20v2_20260313_200300_sg_dynamic_fix04")
CONTRACT_VERSION = os.getenv("CONTRACT_VERSION", "").strip()
MIN_H_VALID = int(os.getenv("MIN_H_VALID", "4"))
PHASE_CONCURRENCY = int(os.getenv("PHASE_CONCURRENCY", "4"))
SUMMARY_CONCURRENCY = int(os.getenv("SUMMARY_CONCURRENCY", "4"))
DRY_RUN = os.getenv("DRY_RUN", "0").strip() == "1"
AFFECTED_SHARDS_OVERRIDE = os.getenv("AFFECTED_SHARDS", "").strip()
E_REBUILD_SHARDS_OVERRIDE = os.getenv("E_REBUILD_SHARDS", "").strip()

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

RUNBOOK_DIR = Path(__file__).resolve().parent
BASE_SQL_DIR = RUNBOOK_DIR.parent / "03_sql"
SQL_FILES = {
    "06": str(BASE_SQL_DIR / "RB20_06" / "06_r1_members_shard.sql"),
    "07": str(BASE_SQL_DIR / "RB20_07" / "07_e_atoms_runs_members_shard.sql"),
    "08": str(BASE_SQL_DIR / "RB20_08" / "08_f_members_shard.sql"),
    "99": str(BASE_SQL_DIR / "RB20_99" / "99_qa_assert.sql"),
}
E_SUMMARY_SCRIPT = str(RUNBOOK_DIR / "build_e_cidr_summary.py")
F_REBUILD_SCRIPT = str(RUNBOOK_DIR / "rebuild_f_and_summary.py")


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def fetch_one(sql, params=None):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(sql, params or ())
    row = cur.fetchone()
    conn.close()
    return row


def fetch_all(sql, params=None):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    conn.close()
    return rows


def parse_int_csv(value):
    if not value:
        return []
    result = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        result.append(int(part))
    return sorted(set(result))


def get_contract_version():
    if CONTRACT_VERSION:
        return CONTRACT_VERSION

    for sql in (
        "SELECT contract_version FROM rb20_v2_5.h_blocks WHERE run_id = %s LIMIT 1",
        "SELECT contract_version FROM rb20_v2_5.shard_plan WHERE run_id = %s LIMIT 1",
        "SELECT contract_version FROM rb20_v2_5.source_members WHERE run_id = %s LIMIT 1",
    ):
        row = fetch_one(sql, (RUN_ID,))
        if row and row[0]:
            return row[0]
    return "contract_v1"


def get_shard_count():
    row = fetch_one(
        "SELECT COUNT(*)::int FROM rb20_v2_5.shard_plan WHERE run_id = %s",
        (RUN_ID,),
    )
    return row[0] if row else 0


def fetch_bad_blocks():
    rows = fetch_all(
        """
        SELECT block_id_final
        FROM rb20_v2_5.h_blocks
        WHERE run_id = %s AND valid_cnt < %s
        ORDER BY block_id_final
        """,
        (RUN_ID, MIN_H_VALID),
    )
    return [row[0] for row in rows]


def fetch_scope():
    override_affected = parse_int_csv(AFFECTED_SHARDS_OVERRIDE)
    override_e = parse_int_csv(E_REBUILD_SHARDS_OVERRIDE)

    stats = fetch_one(
        """
        WITH bad_h AS (
          SELECT block_id_final
          FROM rb20_v2_5.h_blocks
          WHERE run_id = %s AND valid_cnt < %s
        )
        SELECT
          COUNT(*)::bigint AS bad_h_blocks,
          COALESCE(SUM(hb.member_cnt_total), 0)::bigint AS bad_h_member_cnt_total,
          COALESCE(SUM(hb.valid_cnt), 0)::bigint AS bad_h_valid_cnt_total
        FROM rb20_v2_5.h_blocks hb
        JOIN bad_h b USING (block_id_final)
        WHERE hb.run_id = %s
        """,
        (RUN_ID, MIN_H_VALID, RUN_ID),
    )

    if override_affected and override_e:
        return {
            "bad_h_blocks": stats[0],
            "bad_h_member_cnt_total": stats[1],
            "bad_h_valid_cnt_total": stats[2],
            "affected_shards": override_affected,
            "e_rebuild_shards": override_e,
            "f_only_shards": [sid for sid in override_affected if sid not in set(override_e)],
            "affected_real_e_runs": 0,
            "affected_short_runs": 0,
            "missing_keep_members": 0,
        }

    affected_shards = [
        row[0]
        for row in fetch_all(
            """
            WITH bad_h AS (
              SELECT block_id_final
              FROM rb20_v2_5.h_blocks
              WHERE run_id = %s AND valid_cnt < %s
            )
            SELECT DISTINCT sm.shard_id
            FROM rb20_v2_5.h_members hm
            JOIN bad_h b
              ON b.block_id_final = hm.block_id_final
            JOIN rb20_v2_5.source_members sm
              ON sm.run_id = hm.run_id
             AND sm.ip_long = hm.ip_long
            WHERE hm.run_id = %s
            ORDER BY sm.shard_id
            """,
            (RUN_ID, MIN_H_VALID, RUN_ID),
        )
    ]
    if override_affected:
        affected_shards = override_affected

    if override_e:
        e_rebuild_shards = override_e
    else:
        e_rebuild_shards = [
            row[0]
            for row in fetch_all(
                """
                WITH bad_h AS (
                  SELECT block_id_final
                  FROM rb20_v2_5.h_blocks
                  WHERE run_id = %s AND valid_cnt < %s
                ),
                bad_ips AS (
                  SELECT
                    sm.shard_id,
                    hm.ip_long,
                    (hm.ip_long / 32)::bigint AS atom27_id
                  FROM rb20_v2_5.h_members hm
                  JOIN bad_h b
                    ON b.block_id_final = hm.block_id_final
                  JOIN rb20_v2_5.source_members sm
                    ON sm.run_id = hm.run_id
                   AND sm.ip_long = hm.ip_long
                  WHERE hm.run_id = %s
                ),
                neighbor_e AS (
                  SELECT DISTINCT ba.shard_id
                  FROM bad_ips ba
                  JOIN rb20_v2_5.e_atoms ea
                    ON ea.run_id = %s
                   AND ea.shard_id = ba.shard_id
                   AND ea.atom27_id BETWEEN ba.atom27_id - 2 AND ba.atom27_id + 2
                ),
                source_for_sim AS (
                  SELECT
                    r1.shard_id,
                    r1.atom27_id,
                    sm.is_valid
                  FROM rb20_v2_5.r1_members r1
                  JOIN rb20_v2_5.source_members sm
                    ON sm.run_id = r1.run_id
                   AND sm.shard_id = r1.shard_id
                   AND sm.ip_long = r1.ip_long
                  WHERE r1.run_id = %s
                  UNION ALL
                  SELECT
                    bi.shard_id,
                    bi.atom27_id,
                    sm.is_valid
                  FROM bad_ips bi
                  JOIN rb20_v2_5.source_members sm
                    ON sm.run_id = %s
                   AND sm.shard_id = bi.shard_id
                   AND sm.ip_long = bi.ip_long
                ),
                agg AS (
                  SELECT
                    shard_id,
                    atom27_id,
                    COUNT(*) FILTER (WHERE is_valid)::int AS valid_ip_cnt
                  FROM source_for_sim
                  GROUP BY 1, 2
                ),
                new_pass AS (
                  SELECT DISTINCT a.shard_id
                  FROM agg a
                  JOIN (
                    SELECT DISTINCT shard_id, atom27_id
                    FROM bad_ips
                  ) bi
                    ON bi.shard_id = a.shard_id
                   AND bi.atom27_id = a.atom27_id
                  WHERE a.valid_ip_cnt >= 7
                )
                SELECT DISTINCT shard_id
                FROM (
                  SELECT shard_id FROM neighbor_e
                  UNION
                  SELECT shard_id FROM new_pass
                ) s
                ORDER BY shard_id
                """,
                (RUN_ID, MIN_H_VALID, RUN_ID, RUN_ID, RUN_ID, RUN_ID),
            )
        ]

    impact = fetch_one(
        """
        WITH bad_h AS (
          SELECT block_id_final
          FROM rb20_v2_5.h_blocks
          WHERE run_id = %s AND valid_cnt < %s
        ),
        bad_atoms AS (
          SELECT DISTINCT
            sm.shard_id,
            (hm.ip_long / 32)::bigint AS atom27_id
          FROM rb20_v2_5.h_members hm
          JOIN bad_h b
            ON b.block_id_final = hm.block_id_final
          JOIN rb20_v2_5.source_members sm
            ON sm.run_id = hm.run_id
           AND sm.ip_long = hm.ip_long
          WHERE hm.run_id = %s
        ),
        touched AS (
          SELECT DISTINCT
            er.e_run_id,
            er.short_run
          FROM bad_atoms ba
          JOIN rb20_v2_5.e_runs er
            ON er.run_id = %s
           AND er.shard_id = ba.shard_id
           AND ba.atom27_id BETWEEN er.atom27_start - 2 AND er.atom27_end + 2
        )
        SELECT
          COUNT(*) FILTER (WHERE short_run = false)::int AS affected_real_e_runs,
          COUNT(*) FILTER (WHERE short_run = true)::int AS affected_short_runs
        FROM touched
        """,
        (RUN_ID, MIN_H_VALID, RUN_ID, RUN_ID),
    )

    keep_gap = fetch_one(
        """
        WITH bad_h AS (
          SELECT block_id_final
          FROM rb20_v2_5.h_blocks
          WHERE run_id = %s AND valid_cnt < %s
        )
        SELECT COUNT(*)::bigint
        FROM rb20_v2_5.h_members hm
        LEFT JOIN rb20_v2_5.keep_members km
          ON km.run_id = hm.run_id
         AND km.ip_long = hm.ip_long
        JOIN bad_h b
          ON b.block_id_final = hm.block_id_final
        WHERE hm.run_id = %s
          AND km.ip_long IS NULL
        """,
        (RUN_ID, MIN_H_VALID, RUN_ID),
    )

    return {
        "bad_h_blocks": stats[0],
        "bad_h_member_cnt_total": stats[1],
        "bad_h_valid_cnt_total": stats[2],
        "affected_shards": affected_shards,
        "e_rebuild_shards": e_rebuild_shards,
        "f_only_shards": [sid for sid in affected_shards if sid not in set(e_rebuild_shards)],
        "affected_real_e_runs": impact[0] or 0,
        "affected_short_runs": impact[1] or 0,
        "missing_keep_members": keep_gap[0] if keep_gap else 0,
    }


def exec_sql_file(file_path, replacements, description):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    for key, value in replacements.items():
        content = content.replace(key, str(value))

    tmp_path = f"/tmp/rb20_local_fix_{int(time.time() * 1000)}_{os.getpid()}.sql"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(content)

    try:
        subprocess.check_output(
            ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-f", tmp_path],
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
        log(f"DONE: {description}")
    except subprocess.CalledProcessError as exc:
        output = exc.output.decode("utf-8", errors="replace")
        log(f"FAILED: {description}")
        log(output)
        raise
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def delete_bad_h_objects(block_ids, contract_version):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM rb20_v2_5.h_block_summary
            WHERE run_id = %s AND block_id_final = ANY(%s)
            """,
            (RUN_ID, block_ids),
        )
        h_summary_deleted = cur.rowcount

        cur.execute(
            """
            DELETE FROM rb20_v2_5.h_members
            WHERE run_id = %s AND block_id_final = ANY(%s)
            """,
            (RUN_ID, block_ids),
        )
        h_members_deleted = cur.rowcount

        cur.execute(
            """
            DELETE FROM rb20_v2_5.h_blocks
            WHERE run_id = %s AND block_id_final = ANY(%s)
            """,
            (RUN_ID, block_ids),
        )
        h_blocks_deleted = cur.rowcount

        cur.execute(
            """
            DELETE FROM rb20_v2_5.core_numbers
            WHERE run_id = %s AND metric_name IN ('h_block_cnt','h_member_cnt')
            """,
            (RUN_ID,),
        )
        cur.execute(
            """
            INSERT INTO rb20_v2_5.core_numbers(run_id, contract_version, metric_name, metric_value_numeric)
            SELECT %s, %s, 'h_block_cnt', COUNT(*)::numeric
            FROM rb20_v2_5.h_blocks
            WHERE run_id = %s
            """,
            (RUN_ID, contract_version, RUN_ID),
        )
        cur.execute(
            """
            INSERT INTO rb20_v2_5.core_numbers(run_id, contract_version, metric_name, metric_value_numeric)
            SELECT %s, %s, 'h_member_cnt', COUNT(*)::numeric
            FROM rb20_v2_5.h_members
            WHERE run_id = %s
            """,
            (RUN_ID, contract_version, RUN_ID),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log(
        "Deleted bad H objects: "
        f"h_blocks={h_blocks_deleted}, "
        f"h_members={h_members_deleted}, "
        f"h_block_summary={h_summary_deleted}"
    )


def phase_worker(task):
    shard_id, rerun_e, contract_version = task
    replacements = {
        "{{run_id}}": RUN_ID,
        "{{contract_version}}": contract_version,
        "{{shard_id}}": str(shard_id),
    }
    try:
        exec_sql_file(SQL_FILES["06"], replacements, f"Shard {shard_id}: 06 R1 Members")
        if rerun_e:
            exec_sql_file(SQL_FILES["07"], replacements, f"Shard {shard_id}: 07 E Atoms/Runs/Members")
        exec_sql_file(SQL_FILES["08"], replacements, f"Shard {shard_id}: 08 F Members")
        return True, shard_id, rerun_e, None
    except Exception as exc:
        return False, shard_id, rerun_e, str(exc)


def rerun_local_phase7(affected_shards, e_rebuild_shards, contract_version):
    e_shard_set = set(e_rebuild_shards)
    tasks = [(sid, sid in e_shard_set, contract_version) for sid in affected_shards]
    if not tasks:
        return

    with multiprocessing.Pool(min(PHASE_CONCURRENCY, len(tasks))) as pool:
        results = pool.map(phase_worker, tasks)

    failures = [r for r in results if not r[0]]
    if failures:
        for _, shard_id, rerun_e, err in failures:
            log(f"Shard {shard_id} failed (rerun_e={rerun_e}): {err}")
        raise RuntimeError(f"{len(failures)} shards failed during localized Phase 7 repair")


def fetch_member_flow_gap_rows(shard_ids):
    shard_ids = sorted(set(shard_ids))
    if not shard_ids:
        return []

    rows = fetch_all(
        """
        WITH target_shards AS (
          SELECT unnest(%s::smallint[]) AS shard_id
        ),
        keep_no_h AS (
          SELECT
            km.shard_id,
            COUNT(*)::bigint AS keep_no_h_cnt
          FROM rb20_v2_5.keep_members km
          LEFT JOIN rb20_v2_5.h_members hm
            ON hm.run_id = km.run_id
           AND hm.ip_long = km.ip_long
          WHERE km.run_id = %s
            AND km.shard_id = ANY(%s::smallint[])
            AND hm.ip_long IS NULL
          GROUP BY km.shard_id
        ),
        r1 AS (
          SELECT shard_id, COUNT(*)::bigint AS r1_cnt
          FROM rb20_v2_5.r1_members
          WHERE run_id = %s
            AND shard_id = ANY(%s::smallint[])
          GROUP BY shard_id
        ),
        e1 AS (
          SELECT shard_id, COUNT(*)::bigint AS e_cnt
          FROM rb20_v2_5.e_members
          WHERE run_id = %s
            AND shard_id = ANY(%s::smallint[])
          GROUP BY shard_id
        ),
        f1 AS (
          SELECT shard_id, COUNT(*)::bigint AS f_cnt
          FROM rb20_v2_5.f_members
          WHERE run_id = %s
            AND shard_id = ANY(%s::smallint[])
          GROUP BY shard_id
        )
        SELECT
          ts.shard_id,
          COALESCE(k.keep_no_h_cnt, 0)::bigint AS keep_no_h_cnt,
          COALESCE(r.r1_cnt, 0)::bigint AS r1_cnt,
          COALESCE(e1.e_cnt, 0)::bigint AS e_cnt,
          COALESCE(f1.f_cnt, 0)::bigint AS f_cnt,
          (COALESCE(k.keep_no_h_cnt, 0) - COALESCE(r.r1_cnt, 0))::bigint AS keep_minus_r1_gap,
          (COALESCE(r.r1_cnt, 0) - COALESCE(e1.e_cnt, 0) - COALESCE(f1.f_cnt, 0))::bigint AS r1_minus_ef_gap
        FROM target_shards ts
        LEFT JOIN keep_no_h k ON k.shard_id = ts.shard_id
        LEFT JOIN r1 r ON r.shard_id = ts.shard_id
        LEFT JOIN e1 ON e1.shard_id = ts.shard_id
        LEFT JOIN f1 ON f1.shard_id = ts.shard_id
        WHERE COALESCE(k.keep_no_h_cnt, 0) <> COALESCE(r.r1_cnt, 0)
           OR COALESCE(r.r1_cnt, 0) <> COALESCE(e1.e_cnt, 0) + COALESCE(f1.f_cnt, 0)
        ORDER BY ts.shard_id
        """,
        (shard_ids, RUN_ID, shard_ids, RUN_ID, shard_ids, RUN_ID, shard_ids, RUN_ID, shard_ids),
    )

    return [
        {
            "shard_id": row[0],
            "keep_no_h_cnt": row[1],
            "r1_cnt": row[2],
            "e_cnt": row[3],
            "f_cnt": row[4],
            "keep_minus_r1_gap": row[5],
            "r1_minus_ef_gap": row[6],
        }
        for row in rows
    ]


def ensure_member_flow_closed(shard_ids, contract_version):
    gap_rows = fetch_member_flow_gap_rows(shard_ids)
    if not gap_rows:
        return

    retry_shards = [row["shard_id"] for row in gap_rows]
    log(f"Detected member-flow gaps after localized rerun: {gap_rows}")
    log(f"Retrying Phase 7 on gap shards: {retry_shards}")
    rerun_local_phase7(retry_shards, retry_shards, contract_version)

    gap_rows = fetch_member_flow_gap_rows(retry_shards)
    if gap_rows:
        raise RuntimeError(f"Localized Phase 7 still has member-flow gaps: {gap_rows}")


def run_qa(contract_version):
    exec_sql_file(
        SQL_FILES["99"],
        {
            "{{run_id}}": RUN_ID,
            "{{contract_version}}": contract_version,
            "{{shard_cnt}}": str(get_shard_count()),
        },
        "QA Assert",
    )


def run_python_script(script_path, extra_env, description):
    env = os.environ.copy()
    env.update(extra_env)
    log(f"Running {description}...")
    subprocess.check_call([sys.executable, script_path], env=env)


def rebuild_local_summaries(affected_shards, e_rebuild_shards):
    shard_csv = ",".join(str(sid) for sid in affected_shards)
    if e_rebuild_shards:
        e_csv = ",".join(str(sid) for sid in e_rebuild_shards)
        run_python_script(
            E_SUMMARY_SCRIPT,
            {
                "RUN_ID": RUN_ID,
                "SHARD_FILTER": e_csv,
                "CONCURRENCY": str(min(SUMMARY_CONCURRENCY, len(e_rebuild_shards))),
            },
            "partial e_cidr_summary rebuild",
        )

    run_python_script(
        F_REBUILD_SCRIPT,
        {
            "RUN_ID": RUN_ID,
            "SHARD_FILTER": shard_csv,
            "CONCURRENCY": str(min(SUMMARY_CONCURRENCY, len(affected_shards))),
        },
        "partial f_members/f_ip_summary rebuild",
    )


def validate_repair():
    shard_rows = fetch_all(
        "SELECT shard_id FROM rb20_v2_5.shard_plan WHERE run_id = %s ORDER BY shard_id",
        (RUN_ID,),
    )
    all_shards = [row[0] for row in shard_rows]
    row = fetch_one(
        """
        SELECT
          (SELECT COUNT(*)::bigint FROM rb20_v2_5.h_blocks WHERE run_id = %s AND valid_cnt < %s) AS h_valid_lt4,
          (SELECT COUNT(*)::bigint FROM rb20_v2_5.h_block_summary WHERE run_id = %s AND ip_count < %s) AS h_summary_ip_lt4,
          (SELECT COUNT(*)::bigint FROM rb20_v2_5.h_blocks WHERE run_id = %s) AS h_blocks_cnt,
          (SELECT COUNT(*)::bigint FROM rb20_v2_5.h_block_summary WHERE run_id = %s) AS h_summary_cnt,
          (SELECT COUNT(*)::bigint FROM rb20_v2_5.qa_assert WHERE run_id = %s AND severity = 'STOP' AND pass_flag = false) AS qa_stop_fail
        """,
        (RUN_ID, MIN_H_VALID, RUN_ID, MIN_H_VALID, RUN_ID, RUN_ID, RUN_ID),
    )
    flow_gap_rows = fetch_member_flow_gap_rows(all_shards)
    return {
        "h_valid_lt4": row[0],
        "h_summary_ip_lt4": row[1],
        "h_blocks_cnt": row[2],
        "h_summary_cnt": row[3],
        "flow_gap_shard_cnt": len(flow_gap_rows),
        "qa_stop_fail": row[4],
    }


def main():
    t0 = time.time()
    contract_version = get_contract_version()
    bad_blocks = fetch_bad_blocks()
    scope = fetch_scope()

    log(f"=== Localized H valid_lt4 repair start (run={RUN_ID}) ===")
    log(f"Contract version: {contract_version}")
    log(
        "Bad H scope: "
        f"blocks={scope['bad_h_blocks']}, "
        f"member_cnt_total={scope['bad_h_member_cnt_total']}, "
        f"valid_cnt_total={scope['bad_h_valid_cnt_total']}"
    )
    log(f"Affected shards ({len(scope['affected_shards'])}): {scope['affected_shards']}")
    log(f"E rebuild shards ({len(scope['e_rebuild_shards'])}): {scope['e_rebuild_shards']}")
    log(f"F-only shards ({len(scope['f_only_shards'])}): {scope['f_only_shards']}")
    log(
        "Touched E runs: "
        f"real={scope['affected_real_e_runs']}, "
        f"short={scope['affected_short_runs']}"
    )

    if scope["missing_keep_members"] != 0:
        raise RuntimeError(f"Bad H members missing from keep_members: {scope['missing_keep_members']}")

    if not bad_blocks:
        log("No bad H blocks found. Nothing to do.")
        return

    if DRY_RUN:
        log("DRY_RUN=1, stop before applying changes.")
        return

    delete_bad_h_objects(bad_blocks, contract_version)
    rerun_local_phase7(scope["affected_shards"], scope["e_rebuild_shards"], contract_version)
    ensure_member_flow_closed(scope["affected_shards"], contract_version)
    run_qa(contract_version)
    rebuild_local_summaries(scope["affected_shards"], scope["e_rebuild_shards"])

    validation = validate_repair()
    log(f"Validation: {validation}")
    if validation["h_valid_lt4"] != 0:
        raise RuntimeError("Repair incomplete: h_blocks still contains valid_cnt < 4")
    if validation["h_summary_ip_lt4"] != 0:
        raise RuntimeError("Repair incomplete: h_block_summary still contains ip_count < 4")
    if validation["flow_gap_shard_cnt"] != 0:
        raise RuntimeError("Repair incomplete: keep -> r1 -> e/f member flow still has gap shards")
    if validation["qa_stop_fail"] != 0:
        raise RuntimeError("Repair incomplete: QA STOP assertions failed")
    if validation["h_blocks_cnt"] != validation["h_summary_cnt"]:
        raise RuntimeError("Repair incomplete: h_block_summary row count no longer matches h_blocks")

    log(f"=== Localized H valid_lt4 repair success in {time.time() - t0:.1f}s ===")


if __name__ == "__main__":
    main()
