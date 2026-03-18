"""
Run selection and summary readiness helpers shared by UI endpoints.
"""
from typing import Optional

from models.database import fetch_one

SCHEMA = "rb20_v2_5"

SUMMARY_CONFIG = {
    "h": {
        "table": "h_block_summary",
        "required_fields": ["start_ip_text", "avg_apps_per_ip"],
        "label": "H 块摘要",
    },
    "e": {
        "table": "e_cidr_summary",
        "required_fields": ["start_ip_text"],
        "label": "E CIDR 摘要",
    },
    "f": {
        "table": "f_ip_summary",
        "required_fields": ["ip_address", "total_reports", "total_devices"],
        "label": "F IP 摘要",
    },
}


def _required_clause(lib: str) -> str:
    cfg = SUMMARY_CONFIG[lib]
    return " AND ".join(f"{field} IS NOT NULL" for field in cfg["required_fields"])


async def resolve_latest_run_id() -> Optional[str]:
    row = await fetch_one(
        f"SELECT run_id FROM {SCHEMA}.shard_plan ORDER BY created_at DESC LIMIT 1"
    )
    return row["run_id"] if row else None


async def resolve_run_id(run_id: Optional[str] = None) -> Optional[str]:
    if run_id and run_id != "latest":
        return run_id
    return await resolve_latest_run_id()


async def latest_ready_summary_run(lib: str) -> Optional[str]:
    cfg = SUMMARY_CONFIG.get(lib)
    if not cfg:
        return None
    ready_clause = _required_clause(lib)
    row = await fetch_one(
        f"""
        SELECT run_id
        FROM {SCHEMA}.{cfg["table"]}
        GROUP BY run_id
        HAVING COUNT(*) > 0
           AND COUNT(*) FILTER (WHERE {ready_clause}) = COUNT(*)
        ORDER BY run_id DESC
        LIMIT 1
        """
    )
    return row["run_id"] if row else None


async def get_summary_status(run_id: str, lib: str) -> dict:
    cfg = SUMMARY_CONFIG.get(lib)
    if not cfg:
        return {
            "lib": lib,
            "state": "unknown",
            "table": None,
            "message": f"未知库类型: {lib}",
        }

    ready_clause = _required_clause(lib)
    row = await fetch_one(
        f"""
        SELECT
            COUNT(*)::bigint AS total_rows,
            COUNT(*) FILTER (WHERE {ready_clause})::bigint AS ready_rows
        FROM {SCHEMA}.{cfg["table"]}
        WHERE run_id = :run_id
        """,
        {"run_id": run_id},
    )

    total_rows = int((row or {}).get("total_rows") or 0)
    ready_rows = int((row or {}).get("ready_rows") or 0)
    fallback_run_id = None

    if total_rows == 0:
        state = "missing"
        fallback_run_id = await latest_ready_summary_run(lib)
        message = f"{cfg['label']}在当前 run 下还没有生成。"
    elif ready_rows < total_rows:
        state = "partial"
        fallback_run_id = await latest_ready_summary_run(lib)
        message = (
            f"{cfg['label']}已生成 {total_rows} 行，但只有 {ready_rows} 行关键展示字段完整。"
        )
    else:
        state = "ready"
        message = f"{cfg['label']}已就绪。"

    return {
        "lib": lib,
        "state": state,
        "table": cfg["table"],
        "required_fields": cfg["required_fields"],
        "total_rows": total_rows,
        "ready_rows": ready_rows,
        "fallback_run_id": fallback_run_id if fallback_run_id != run_id else None,
        "message": message,
    }
