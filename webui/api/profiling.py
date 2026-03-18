"""
画像标签漏斗 API
漏斗式 block 分层标记引擎。
"""
import json
import os
from pathlib import Path
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from models.database import fetch_all, fetch_one
from services.summary_state import get_summary_status, latest_ready_summary_run, resolve_run_id

router = APIRouter(prefix="/api/research/profiling", tags=["profiling"])

SCHEMA = "rb20_v2_5"
CONFIG_PATH = Path(__file__).parent.parent / "config" / "profile_tags.json"
E_CONFIG_PATH = Path(__file__).parent.parent / "config" / "e_profile_tags.json"
F_CONFIG_PATH = Path(__file__).parent.parent / "config" / "f_profile_tags.json"

TABLE_MAP = {
    "h": "h_block_summary",
    "e": "e_cidr_summary",
    "f": "f_ip_summary",
}
RUN_ID_COL_MAP = {
    "h": "run_id",
    "e": "run_id",
    "f": "run_id",
}

# ── helpers ──

def _load_tags(lib: str = "h") -> list[dict]:
    if lib == "f":
        cfg_path = F_CONFIG_PATH
    elif lib == "e":
        cfg_path = E_CONFIG_PATH
    else:
        cfg_path = CONFIG_PATH
    if not cfg_path.exists():
        return []
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg.get("tags", [])


def _save_tags(tags: list[dict], lib: str = "h"):
    if lib == "f":
        cfg_path = F_CONFIG_PATH
    elif lib == "e":
        cfg_path = E_CONFIG_PATH
    else:
        cfg_path = CONFIG_PATH
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        cfg = {}
    cfg["tags"] = tags
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def _build_where(conditions: list[dict], logic: str = "AND", tag: dict = None) -> str:
    """Build SQL WHERE clauses from tag conditions. Supports AND/OR/CUSTOM logic."""
    if logic.upper() == "CUSTOM" and tag:
        return _build_custom_where(conditions, tag)

    parts = []
    for c in conditions:
        field = c["field"]
        op = c["op"]
        val = c["value"]
        # Safety: whitelist field names (alphanumeric + underscore only)
        if not field.replace("_", "").isalnum():
            continue
        if op == "IN" and isinstance(val, list):
            quoted = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in val)
            parts.append(f"COALESCE({field},'') IN ({quoted})")
        elif op == "TIERED" and isinstance(val, dict):
            # Tiered thresholds: {tier_field, tiers: [{max: N, threshold: T}, ...]}
            # threshold=0 means skip check (always pass) for that tier
            tier_field = val.get("tier_field", "total_devices")
            tiers = val.get("tiers", [])
            if tiers and tier_field.replace("_", "").isalnum():
                case_parts = []
                for t in tiers:
                    th = t['threshold']
                    val_expr = "TRUE" if th == 0 else f"{field} >= {th}"
                    if "max" in t:
                        case_parts.append(f"WHEN {tier_field} < {t['max']} THEN {val_expr}")
                    else:
                        case_parts.append(f"ELSE {val_expr}")
                parts.append(f"(CASE {' '.join(case_parts)} END)")
        elif op in (">=", "<=", ">", "<", "=", "!="):
            # Quote string values for SQL safety
            if isinstance(val, str):
                safe_val = val.replace("'", "''")
                parts.append(f"{field} {op} '{safe_val}'")
            else:
                parts.append(f"{field} {op} {val}")
    joiner = " OR " if logic.upper() == "OR" else " AND "
    return joiner.join(parts)


def _build_custom_where(conditions: list[dict], tag: dict) -> str:
    """Build CUSTOM logic for multi-dimensional anomaly scoring."""
    # Build prerequisite conditions (ip_count >= 4, avg_devices >= 5, etc.)
    prereqs = []
    threshold = 3  # default anomaly score threshold
    for c in conditions:
        field = c["field"]
        if field.startswith("_"):
            # Meta field like _anomaly_score → extract threshold
            threshold = c.get("value", 3)
            continue
        if not field.replace("_", "").isalnum():
            continue
        op = c["op"]
        val = c["value"]
        prereqs.append(f"{field} {op} {val}")

    # Build anomaly dimension scoring
    dims = tag.get("anomaly_dimensions", [])
    score_parts = []
    for d in dims:
        expr = d["field"]
        op = d["op"]
        val = d["value"]
        # Each dimension: CASE WHEN expr op val THEN 1 ELSE 0 END
        score_parts.append(f"CASE WHEN ({expr}) {op} {val} THEN 1 ELSE 0 END")

    if not score_parts:
        return " AND ".join(prereqs) if prereqs else "FALSE"

    score_expr = " + ".join(score_parts)
    all_parts = prereqs + [f"({score_expr}) >= {threshold}"]
    return " AND ".join(all_parts)


async def _resolve_run(run_id: str, lib: str = "h") -> str:
    if run_id == "latest":
        summary_run = await latest_ready_summary_run(lib)
        if summary_run:
            return summary_run
    return await resolve_run_id(run_id)


# ── 1) GET /funnel  — 完整漏斗计算 ──

@router.get("/funnel")
async def compute_funnel(run_id: str = Query("latest"), lib: str = Query("h")):
    """
    计算漏斗：按标签顺序，对 block_summary 逐层筛选。
    lib='h' → h_block_summary, lib='e' → e_cidr_summary
    每层输出：标签信息 + 命中数 + 剩余数 + IP 统计。
    """
    lib = lib.lower()
    if lib not in TABLE_MAP:
        lib = "h"
    table = TABLE_MAP[lib]
    run_id = await _resolve_run(run_id, lib)
    summary_status = await get_summary_status(run_id, lib)
    tags = _load_tags(lib)

    if summary_status["state"] != "ready":
        return {
            "run_id": run_id,
            "lib": lib,
            "total": {"blocks": 0, "ips": 0, "devices": 0, "reports": 0},
            "tag_count": 0,
            "funnel": [],
            "summary_status": summary_status,
            "message": summary_status["message"],
        }

    if not tags:
        return {
            "run_id": run_id,
            "lib": lib,
            "total": {"blocks": 0, "ips": 0, "devices": 0, "reports": 0},
            "tag_count": 0,
            "funnel": [],
            "summary_status": summary_status,
            "message": f"{lib.upper()} 库暂无标签配置，请先创建 {'e_profile_tags' if lib == 'e' else 'profile_tags'}.json"
        }

    # Total baseline
    total_row = await fetch_one(f"""
        SELECT COUNT(*) as cnt, COALESCE(SUM(ip_count),0) as ips,
               COALESCE(SUM(total_devices),0) as devices,
               COALESCE(SUM(total_reports),0) as reports
        FROM {SCHEMA}.{table} WHERE run_id = :run_id
    """, {"run_id": run_id})

    total_blocks = total_row["cnt"]
    total_ips = total_row["ips"]
    total_devices = total_row["devices"]
    total_reports = total_row["reports"]

    funnel = []
    exclude_clauses = []  # accumulates NOT conditions for previous tags

    for tag in tags:
        tag_logic = tag.get("logic", "AND")
        where = _build_where(tag["conditions"], tag_logic, tag=tag)
        if not where:
            continue

        # Build "remaining pool" = run_id AND NOT(tag1) AND NOT(tag2) ...
        pool_clause = f"run_id = :run_id"
        for exc in exclude_clauses:
            pool_clause += f" AND NOT ({exc})"

        # Count matched in remaining pool
        match_sql = f"""
            SELECT COUNT(*) as cnt,
                   COALESCE(SUM(ip_count),0) as ips,
                   COALESCE(SUM(total_devices),0) as devices,
                   COALESCE(SUM(total_reports),0) as reports,
                   COALESCE(SUM(abnormal_ip_count),0) as abnormal_ips
            FROM {SCHEMA}.{table}
            WHERE {pool_clause} AND ({where})
        """
        match_row = await fetch_one(match_sql, {"run_id": run_id})

        # Remaining after this tag
        exclude_clauses.append(where)
        remain_clause = f"run_id = :run_id"
        for exc in exclude_clauses:
            remain_clause += f" AND NOT ({exc})"

        remain_row = await fetch_one(f"""
            SELECT COUNT(*) as cnt, COALESCE(SUM(ip_count),0) as ips,
                   COALESCE(SUM(total_devices),0) as devices
            FROM {SCHEMA}.{table}
            WHERE {remain_clause}
        """, {"run_id": run_id})

        funnel.append({
            "tag": {
                "id": tag["id"],
                "name": tag["name"],
                "emoji": tag.get("emoji", "🏷️"),
                "color": tag.get("color", "#58a6ff"),
                "description": tag.get("description", ""),
                "notes": tag.get("notes", ""),
                "logic": tag.get("logic", "AND"),
                "conditions": tag["conditions"],
            },
            "matched": {
                "blocks": match_row["cnt"],
                "ips": match_row["ips"],
                "devices": match_row["devices"],
                "reports": match_row["reports"],
                "abnormal_ips": match_row["abnormal_ips"],
            },
            "remaining": {
                "blocks": remain_row["cnt"],
                "ips": remain_row["ips"],
                "devices": remain_row["devices"],
            }
        })

    return {
        "run_id": run_id,
        "lib": lib,
        "total": {
            "blocks": total_blocks,
            "ips": total_ips,
            "devices": total_devices,
            "reports": total_reports,
        },
        "tag_count": len(funnel),
        "funnel": funnel,
        "summary_status": summary_status,
    }


# ── 2) GET /remaining-stats  — 剩余池分位数分析 ──

@router.get("/remaining-stats")
async def remaining_stats(run_id: str = Query("latest"), lib: str = Query("h")):
    """
    对当前剩余池（排除所有已标记标签后）做 P50/P85/P90/P95 分位数分析。
    """
    lib = lib.lower()
    if lib not in TABLE_MAP:
        lib = "h"
    table = TABLE_MAP[lib]
    run_id = await _resolve_run(run_id, lib)
    summary_status = await get_summary_status(run_id, lib)
    tags = _load_tags(lib)

    if summary_status["state"] != "ready":
        return {
            "run_id": run_id,
            "lib": lib,
            "remaining_blocks": 0,
            "stats": [],
            "summary_status": summary_status,
            "message": summary_status["message"],
        }

    # Build remaining pool WHERE
    pool = f"run_id = :run_id"
    for tag in tags:
        tag_logic = tag.get("logic", "AND")
        w = _build_where(tag["conditions"], tag_logic, tag=tag)
        if w:
            pool += f" AND NOT ({w})"

    metrics = [
        "mobile_device_ratio", "wifi_device_ratio",
        "ip_count", "avg_devices_per_ip", "avg_reports_per_ip",
        "daa_dna_ratio", "avg_active_days",
        "late_night_report_ratio", "abnormal_ip_ratio",
        "proxy_reports", "root_reports",
        "distinct_operators",
    ]
    # H-specific metrics
    if lib == "h":
        metrics.extend([
            "avg_apps_per_ip", "density",
            "android_device_ratio", "oaid_device_ratio",
            "android_oaid_ratio", "report_oaid_ratio",
            "avg_model_per_ip", "avg_manufacturer_per_ip",
        ])

    results = []
    for m in metrics:
        row = await fetch_one(f"""
            SELECT
                COUNT(*) as cnt,
                ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {m})::numeric, 4) as p50,
                ROUND(PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY {m})::numeric, 4) as p85,
                ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY {m})::numeric, 4) as p90,
                ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {m})::numeric, 4) as p95,
                ROUND(AVG({m})::numeric, 4) as avg,
                ROUND(MIN({m})::numeric, 4) as min,
                ROUND(MAX({m})::numeric, 4) as max
            FROM {SCHEMA}.{table}
            WHERE {pool}
        """, {"run_id": run_id})
        results.append({
            "metric": m,
            "count": row["cnt"],
            "p50": float(row["p50"]) if row["p50"] else None,
            "p85": float(row["p85"]) if row["p85"] else None,
            "p90": float(row["p90"]) if row["p90"] else None,
            "p95": float(row["p95"]) if row["p95"] else None,
            "avg": float(row["avg"]) if row["avg"] else None,
            "min": float(row["min"]) if row["min"] else None,
            "max": float(row["max"]) if row["max"] else None,
        })

    return {
        "run_id": run_id,
        "lib": lib,
        "remaining_blocks": results[0]["count"] if results else 0,
        "stats": results,
        "summary_status": summary_status,
    }


# ── 3) GET/PUT /tags  — 标签管理 ──

@router.get("/tags")
async def get_tags():
    """返回所有标签配置。"""
    return {"tags": _load_tags()}


@router.put("/tags/{tag_id}/conditions")
async def update_tag_conditions(tag_id: str, body: dict):
    """更新某个标签的 conditions（阈值调整）。"""
    tags = _load_tags()
    for t in tags:
        if t["id"] == tag_id:
            t["conditions"] = body.get("conditions", t["conditions"])
            _save_tags(tags)
            return {"status": "ok", "tag": t}
    return JSONResponse(status_code=404, content={"error": f"Tag {tag_id} not found"})
