"""
Library Lab Research API — IP 库画像研究端点

Phase 1: 库总览/对比、分布查询、流转漏斗、边界检测
"""
from fastapi import APIRouter, Query, Body
from fastapi.responses import JSONResponse
from models.database import fetch_all, fetch_one
import json, os
from datetime import datetime
from pathlib import Path
from services.summary_state import get_summary_status, resolve_run_id

router = APIRouter(prefix="/api/research", tags=["Research"])

SCHEMA = "rb20_v2_5"

# ── Helper: resolve latest run_id ──
async def _resolve_run(run_id: str = None) -> str:
    return await resolve_run_id(run_id)


# ============================================================
# 1) 三库 KPI 总览与对比
# ============================================================
@router.get("/runs/{run_id}/libraries/overview")
async def libraries_overview(run_id: str):
    """H/E/F 三库 KPI: 成员数、valid_ratio、运营商 CR3、report/device P50/P90"""
    run_id = await _resolve_run(run_id)

    # --- per-library stats via source_members_slim join ---
    results = {}
    for lib in ("h", "e", "f"):
        row = await fetch_one(f"""
            SELECT
                COUNT(*)::bigint AS members_total,
                COUNT(*) FILTER(WHERE s.is_valid)::bigint AS valid_cnt,
                COUNT(*) FILTER(WHERE NOT s.is_valid)::bigint AS invalid_cnt,
                ROUND(COUNT(*) FILTER(WHERE s.is_valid)::numeric / GREATEST(COUNT(*), 1) * 100, 2) AS valid_pct,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.reports) AS report_p50,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY s.reports) AS report_p90,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.devices) AS device_p50,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY s.devices) AS device_p90,
                ROUND(AVG(CASE WHEN s.devices > 0
                    THEN s.mobile_devices::float / s.devices ELSE 0 END)::numeric, 4) AS avg_mobile_ratio
            FROM {SCHEMA}.{lib}_members m
            JOIN {SCHEMA}.source_members_slim s
                ON s.run_id = m.run_id AND s.ip_long = m.ip_long
            WHERE m.run_id = :run_id
        """, {"run_id": run_id})
        results[lib] = dict(row) if row else {}

    # --- operator CR3 per library ---
    for lib in ("h", "e", "f"):
        ops = await fetch_all(f"""
            SELECT s.operator, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.{lib}_members m
            JOIN {SCHEMA}.source_members_slim s
                ON s.run_id = m.run_id AND s.ip_long = m.ip_long
            WHERE m.run_id = :run_id
            GROUP BY s.operator ORDER BY cnt DESC LIMIT 5
        """, {"run_id": run_id})
        total = results[lib].get("members_total", 1) or 1
        results[lib]["top_operators"] = [
            {"operator": r["operator"], "cnt": r["cnt"],
             "pct": round(r["cnt"] / total * 100, 2)}
            for r in ops
        ]
        cr3 = sum(r["cnt"] for r in ops[:3])
        results[lib]["cr3_pct"] = round(cr3 / total * 100, 2)

    # --- global funnel counts ---
    funnel = await fetch_one(f"""
        SELECT
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.source_members_slim WHERE run_id = :run_id) AS source_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.keep_members WHERE run_id = :run_id) AS keep_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.drop_members WHERE run_id = :run_id) AS drop_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.h_members WHERE run_id = :run_id) AS h_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.e_members WHERE run_id = :run_id) AS e_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.f_members WHERE run_id = :run_id) AS f_total
    """, {"run_id": run_id})

    return {
        "run_id": run_id,
        "libraries": results,
        "funnel": dict(funnel) if funnel else {},
    }


# ============================================================
# 2) 分布查询 (按 field + 分桶)
# ============================================================
@router.get("/runs/{run_id}/library/{lib}/distribution")
async def library_distribution(
    run_id: str,
    lib: str,
    field: str = Query("reports", description="reports|devices|mobile_ratio|density|valid_cnt|simple_score"),
    bins: int = Query(20, ge=5, le=100),
):
    """返回某个字段在该库中的分布直方图数据"""
    run_id = await _resolve_run(run_id)
    lib = lib.lower()

    # fields from source_members_slim (member-level)
    member_fields = {
        "reports": "s.reports",
        "devices": "s.devices",
        "mobile_ratio": "CASE WHEN s.devices > 0 THEN s.mobile_devices::float / s.devices ELSE 0 END",
    }

    # fields from profile_final (block-level, H only meaningful)
    block_fields = {
        "density": "pf.density",
        "valid_cnt": "pf.valid_cnt",
        "simple_score": "pf.simple_score",
    }

    # fields from e_atoms (E-specific)
    atom_fields = {
        "atom_valid_cnt": "ea.valid_ip_cnt",
        "atom_density": "ea.atom_density",
    }

    # fields from e_runs (E-specific)
    run_fields = {
        "run_len": "er.run_len",
    }

    if field in member_fields:
        expr = member_fields[field]
        rows = await fetch_all(f"""
            WITH vals AS (
                SELECT {expr}::float AS v
                FROM {SCHEMA}.{lib}_members m
                JOIN {SCHEMA}.source_members_slim s
                    ON s.run_id = m.run_id AND s.ip_long = m.ip_long
                WHERE m.run_id = :run_id
            ),
            stats AS (
                SELECT MIN(v) AS mn, MAX(v) AS mx FROM vals
            ),
            bucketed AS (
                SELECT width_bucket(v, s.mn, s.mx + 0.001, :bins) AS bucket, COUNT(*)::bigint AS cnt
                FROM vals, stats s
                GROUP BY bucket ORDER BY bucket
            )
            SELECT
                bucket,
                ROUND((s.mn + (bucket - 1) * (s.mx - s.mn + 0.001) / :bins)::numeric, 2) AS bin_start,
                ROUND((s.mn + bucket * (s.mx - s.mn + 0.001) / :bins)::numeric, 2) AS bin_end,
                b.cnt
            FROM bucketed b, stats s
            ORDER BY bucket
        """, {"run_id": run_id, "bins": bins})
        return {"field": field, "lib": lib, "bins": rows}

    elif field in block_fields:
        expr = block_fields[field]
        # For H: profile_final where network_tier_final = '中型网络'
        # For all: just use profile_final based on what blocks belong to this lib
        if lib == "h":
            where = "pf.network_tier_final IN ('中型网络', '大型网络', '超大网络') AND pf.network_tier_final = '中型网络'"
        else:
            where = "1=1"  # profile_final covers all blocks
        rows = await fetch_all(f"""
            WITH vals AS (
                SELECT {expr}::float AS v
                FROM {SCHEMA}.profile_final pf
                WHERE pf.run_id = :run_id AND {where}
            ),
            stats AS (
                SELECT MIN(v) AS mn, MAX(v) AS mx FROM vals
            ),
            bucketed AS (
                SELECT width_bucket(v, s.mn, s.mx + 0.001, :bins) AS bucket, COUNT(*)::bigint AS cnt
                FROM vals, stats s
                GROUP BY bucket ORDER BY bucket
            )
            SELECT
                bucket,
                ROUND((s.mn + (bucket - 1) * (s.mx - s.mn + 0.001) / :bins)::numeric, 2) AS bin_start,
                ROUND((s.mn + bucket * (s.mx - s.mn + 0.001) / :bins)::numeric, 2) AS bin_end,
                b.cnt
            FROM bucketed b, stats s
            ORDER BY bucket
        """, {"run_id": run_id, "bins": bins})
        return {"field": field, "lib": lib, "bins": rows}

    elif field in atom_fields:
        expr = atom_fields[field]
        rows = await fetch_all(f"""
            SELECT {expr}::int AS value, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.e_atoms ea
            WHERE ea.run_id = :run_id AND ea.is_e_atom = true
            GROUP BY 1 ORDER BY 1
        """, {"run_id": run_id})
        return {"field": field, "lib": lib, "bins": rows}

    elif field in run_fields:
        expr = run_fields[field]
        rows = await fetch_all(f"""
            SELECT {expr} AS value, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.e_runs er
            WHERE er.run_id = :run_id AND NOT er.short_run
            GROUP BY 1 ORDER BY 1
        """, {"run_id": run_id})
        return {"field": field, "lib": lib, "bins": rows}

    return {"error": f"Unknown field: {field}"}


# ============================================================
# 3) 运营商 TopN
# ============================================================
@router.get("/runs/{run_id}/library/{lib}/top")
async def library_top(
    run_id: str,
    lib: str,
    field: str = Query("operator"),
    limit: int = Query(15, ge=1, le=50),
):
    """某库按字段的 TopN"""
    run_id = await _resolve_run(run_id)
    lib = lib.lower()

    if field == "operator":
        rows = await fetch_all(f"""
            SELECT s.operator AS label, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.{lib}_members m
            JOIN {SCHEMA}.source_members_slim s
                ON s.run_id = m.run_id AND s.ip_long = m.ip_long
            WHERE m.run_id = :run_id AND s.operator IS NOT NULL
            GROUP BY s.operator ORDER BY cnt DESC LIMIT :limit
        """, {"run_id": run_id, "limit": limit})
    else:
        return {"error": f"Unknown field: {field}"}

    total_row = await fetch_one(f"""
        SELECT COUNT(*)::bigint AS total FROM {SCHEMA}.{lib}_members WHERE run_id = :run_id
    """, {"run_id": run_id})
    total = total_row["total"] if total_row else 1

    return {
        "field": field, "lib": lib,
        "items": [
            {**r, "pct": round(r["cnt"] / max(total, 1) * 100, 2)}
            for r in rows
        ],
        "total": total,
    }

# ============================================================
# 3.5) 画像增强 (散点图 & 箱线图)
# ============================================================
@router.get("/runs/{run_id}/h/block-scatter")
async def h_block_scatter(
    run_id: str,
    sample: int = Query(500, ge=100, le=5000)
):
    """H Block 散点图: valid_cnt vs density (气泡大小=member_cnt_total)"""
    run_id = await _resolve_run(run_id)
    
    rows = await fetch_all(f"""
        SELECT 
            block_id_final,
            ROUND(density::numeric, 4) AS density,
            valid_cnt,
            member_cnt_total,
            simple_score,
            network_tier_final
        FROM {SCHEMA}.profile_final
        WHERE run_id = :run_id 
          AND network_tier_final = '中型网络'
          AND member_cnt_total > 0
        ORDER BY RANDOM()
        LIMIT :limit
    """, {"run_id": run_id, "limit": sample})
    
    return {
        "run_id": run_id,
        "sample_size": len(rows),
        "data": rows
    }

@router.get("/runs/{run_id}/h/operator-boxplot")
async def h_operator_boxplot(
    run_id: str,
    field: str = Query("density"),
    top: int = Query(5, ge=3, le=10)
):
    """按 Top 运营商分组的 density 箱线图"""
    run_id = await _resolve_run(run_id)
    
    if field != "density":
         return {"error": "Only density is supported for now"}

    # 1. Get Top Operators for H blocks
    # We join h_members with source_members_slim to get operator stats
    top_ops = await fetch_all(f"""
        SELECT s.operator, COUNT(*)::bigint AS cnt
        FROM {SCHEMA}.h_members m
        JOIN {SCHEMA}.source_members_slim s
            ON s.run_id = m.run_id AND s.ip_long = m.ip_long
        WHERE m.run_id = :run_id AND s.operator IS NOT NULL AND s.operator != ''
        GROUP BY s.operator
        ORDER BY cnt DESC
        LIMIT :top
    """, {"run_id": run_id, "top": top})
    
    if not top_ops:
        return {"run_id": run_id, "data": []}
        
    ops_list = [r["operator"] for r in top_ops]
    
    # 2. Get 5-number summary (min, Q1, median, Q3, max) for each top operator
    # Note: We group by the MOST COMMON operator in mapping a block to an operator
    # A simpler approximation: Join profile_final to the most frequent operator of that block
    # Since exact 5 quantiles might be slow, we use PERCENTILE_CONT
    
    # We use map_member_block_final to bridge source_members_slim and profile_final
    boxplots = []
    for op in ops_list:
        row = await fetch_one(f"""
            WITH op_blocks AS (
                SELECT pf.block_id_final, pf.density
                FROM {SCHEMA}.profile_final pf
                JOIN {SCHEMA}.map_member_block_final mb
                    ON mb.run_id = pf.run_id AND mb.block_id_final = pf.block_id_final
                JOIN {SCHEMA}.source_members_slim s
                    ON s.run_id = mb.run_id AND s.ip_long = mb.ip_long
                WHERE pf.run_id = :run_id 
                  AND pf.network_tier_final = '中型网络'
                  AND s.operator = :op
                GROUP BY pf.block_id_final, pf.density
            )
            SELECT
                MIN(density) AS min_val,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY density) AS q1,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY density) AS median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY density) AS q3,
                MAX(density) AS max_val
            FROM op_blocks
        """, {"run_id": run_id, "op": op})
        
        if row and row["min_val"] is not None:
            boxplots.append({
                "operator": op,
                "min": round(float(row["min_val"]), 4),
                "q1": round(float(row["q1"]), 4),
                "median": round(float(row["median"]), 4),
                "q3": round(float(row["q3"]), 4),
                "max": round(float(row["max_val"]), 4),
            })
            
    return {
        "run_id": run_id,
        "field": field,
        "data": boxplots
    }

@router.get("/runs/{run_id}/e/suspicious-runs")
async def e_suspicious_runs(
    run_id: str,
    limit: int = Query(50, ge=10, le=200)
):
    """E 库可疑连续段：run_len 大于 3 但中间含有大量低 valid_cnt 的原子"""
    run_id = await _resolve_run(run_id)
    
    # Simple heuristic: runs with length >= 3, with avg valid_ip_cnt per atom being low (e.g. < 4)
    rows = await fetch_all(f"""
        WITH run_atoms AS (
            SELECT 
                r.run_id, r.run_len, r.block_id_final, r.member_cnt,
                AVG(a.valid_ip_cnt) AS avg_valid_cnt
            FROM {SCHEMA}.e_runs r
            JOIN {SCHEMA}.map_run_atom m ON m.run_id = r.run_id AND m.e_run_id = r.id
            JOIN {SCHEMA}.e_atoms a ON a.run_id = m.run_id AND a.id = m.e_atom_id
            WHERE r.run_id = :run_id AND NOT r.short_run
            GROUP BY 1, 2, 3, 4
        )
        SELECT block_id_final, run_len, member_cnt, ROUND(avg_valid_cnt, 2) AS avg_valid_cnt
        FROM run_atoms
        WHERE avg_valid_cnt < 4 AND run_len >= 3
        ORDER BY run_len DESC, member_cnt DESC
        LIMIT :limit
    """, {"run_id": run_id, "limit": limit})
    
    return {
        "run_id": run_id,
        "limit": limit,
        "suspicious_runs": rows
    }

@router.get("/runs/{run_id}/f/opportunity-zone")
async def f_opportunity_zone(
    run_id: str,
    limit: int = Query(50, ge=10, le=200)
):
    """F 库机会带：差点进入 E 的成员 (e.g. valid_cnt=6 或 run_len=2)"""
    run_id = await _resolve_run(run_id)
    
    # Find F members that belong to atoms with valid_cnt = 6 or small runs (run_len = 2)
    # We query f_members and see if they have associated e_atoms with high valid_cnt
    # Since map_member_atom exists, we can use it
    
    rows = await fetch_all(f"""
        SELECT 
            f.ip_long, f.block_id_final, 
            a.valid_ip_cnt, a.atom_size
        FROM {SCHEMA}.f_members f
        JOIN {SCHEMA}.map_member_atom ma ON ma.run_id = f.run_id AND ma.ip_long = f.ip_long
        JOIN {SCHEMA}.e_atoms a ON a.run_id = ma.run_id AND a.id = ma.e_atom_id
        WHERE f.run_id = :run_id
          AND a.valid_ip_cnt >= 5  -- Nearly 7
        ORDER BY a.valid_ip_cnt DESC
        LIMIT :limit
    """, {"run_id": run_id, "limit": limit})
    
    return {
        "run_id": run_id,
        "limit": limit,
        "opportunities": rows
    }

# ============================================================
# 4) 全局漏斗
# ============================================================
@router.get("/runs/{run_id}/flow/global-funnel")
async def global_funnel(run_id: str):
    """Source → Keep/Drop → H/E/F 全局漏斗"""
    run_id = await _resolve_run(run_id)
    row = await fetch_one(f"""
        SELECT
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.source_members_slim WHERE run_id = :run_id) AS source_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.keep_members WHERE run_id = :run_id) AS keep_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.drop_members WHERE run_id = :run_id) AS drop_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.h_members WHERE run_id = :run_id) AS h_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.r1_members WHERE run_id = :run_id) AS r1_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.e_members WHERE run_id = :run_id) AS e_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.f_members WHERE run_id = :run_id) AS f_total
    """, {"run_id": run_id})
    return dict(row) if row else {}


# ============================================================
# 5) 库专属 Sankey / 流转
# ============================================================
@router.get("/runs/{run_id}/library/{lib}/flow/sankey")
async def library_sankey(run_id: str, lib: str):
    """
    H: network_tier_pre → split? → network_tier_final → H
    E/F: R1 atoms → density pass? → run pass? → E/F
    """
    run_id = await _resolve_run(run_id)
    lib = lib.lower()

    if lib == "h":
        # H Sankey: profile_pre tier → (split events?) → profile_final tier → H
        rows = await fetch_all(f"""
            WITH h_finals AS (
                SELECT pf.block_id_final, pf.block_id_parent, pf.network_tier_final,
                       pf.member_cnt_total
                FROM {SCHEMA}.profile_final pf
                WHERE pf.run_id = :run_id AND pf.network_tier_final = '中型网络'
            ),
            h_with_pre AS (
                SELECT
                    hf.block_id_final,
                    hf.network_tier_final,
                    hf.member_cnt_total,
                    pp.network_tier_pre,
                    CASE WHEN hf.block_id_final != hf.block_id_parent
                         THEN 'split' ELSE 'no_split' END AS split_status
                FROM h_finals hf
                LEFT JOIN {SCHEMA}.profile_pre pp
                    ON pp.run_id = :run_id AND pp.block_id_natural = hf.block_id_parent
            )
            SELECT
                COALESCE(network_tier_pre, '未知') AS tier_pre,
                split_status,
                COUNT(*)::bigint AS block_cnt,
                SUM(member_cnt_total)::bigint AS member_cnt
            FROM h_with_pre
            GROUP BY 1, 2
            ORDER BY member_cnt DESC
        """, {"run_id": run_id})
        return {"lib": "h", "type": "tier_drift", "data": rows}

    elif lib in ("e", "f"):
        # E/F Sankey: atoms breakdown by density_pass and run_pass
        rows = await fetch_all(f"""
            WITH atom_stats AS (
                SELECT
                    ea.atom27_id,
                    ea.valid_ip_cnt,
                    ea.is_e_atom,
                    CASE WHEN ea.atom_density >= 0.2 THEN true ELSE false END AS density_pass,
                    er.run_len,
                    CASE WHEN er.run_len IS NOT NULL AND er.run_len >= 3 THEN true ELSE false END AS run_pass
                FROM {SCHEMA}.e_atoms ea
                LEFT JOIN {SCHEMA}.e_members em
                    ON em.run_id = ea.run_id AND em.atom27_id = ea.atom27_id
                LEFT JOIN {SCHEMA}.e_runs er
                    ON er.run_id = em.run_id AND er.e_run_id = em.e_run_id
                WHERE ea.run_id = :run_id
            )
            SELECT
                density_pass,
                run_pass,
                COUNT(*)::bigint AS atom_cnt,
                SUM(valid_ip_cnt)::bigint AS member_estimate
            FROM atom_stats
            GROUP BY 1, 2
            ORDER BY atom_cnt DESC
        """, {"run_id": run_id})
        return {"lib": lib, "type": "density_run_funnel", "data": rows}

    return {"error": f"Unknown library: {lib}"}


# ============================================================
# 6) 边界带问题检测
# ============================================================
@router.get("/runs/{run_id}/library/{lib}/issues")
async def library_issues(run_id: str, lib: str):
    """检测边界带不稳定问题"""
    run_id = await _resolve_run(run_id)
    lib = lib.lower()
    issues = []

    if lib == "h":
        # Issue A: wD 跳变边界 (density ≈ 30)
        boundary = await fetch_all(f"""
            SELECT
                CASE
                    WHEN density BETWEEN 28 AND 30 THEN '28-30 (wD=4)'
                    WHEN density > 30 AND density <= 32 THEN '30-32 (wD=16)'
                END AS density_band,
                COUNT(*)::bigint AS block_cnt,
                SUM(member_cnt_total)::bigint AS member_cnt,
                ROUND(AVG(simple_score)::numeric, 1) AS avg_score
            FROM {SCHEMA}.profile_final
            WHERE run_id = :run_id
              AND density BETWEEN 28 AND 32
            GROUP BY 1
            ORDER BY 1
        """, {"run_id": run_id})
        if boundary:
            issues.append({
                "id": "ISS-H-001",
                "type": "threshold_boundary",
                "severity": "high",
                "title": "wD 阶梯跳变 (density≈30)",
                "description": "density 从 30 以下到 30 以上时，wD 从 4 跳到 16，score 增加 12",
                "data": boundary,
            })

        # Issue B: simple_score 在 H 准入阈值 (20) 附近
        score_boundary = await fetch_all(f"""
            SELECT simple_score, network_tier_final,
                   COUNT(*)::bigint AS block_cnt,
                   SUM(member_cnt_total)::bigint AS member_cnt
            FROM {SCHEMA}.profile_final
            WHERE run_id = :run_id AND simple_score BETWEEN 18 AND 22
            GROUP BY 1, 2 ORDER BY 1
        """, {"run_id": run_id})
        if score_boundary:
            issues.append({
                "id": "ISS-H-002",
                "type": "threshold_boundary",
                "severity": "medium",
                "title": "score ≈ 20 准入边界",
                "description": "simple_score=19 为小型网络, =20 为中型网络(H准入), 边界附近块数",
                "data": score_boundary,
            })

        # Issue C: 切分触发器统计
        split_stats = await fetch_all(f"""
            SELECT
                trigger_report, trigger_mobile, trigger_operator,
                COUNT(*)::bigint AS cut_cnt
            FROM {SCHEMA}.split_events_64
            WHERE run_id = :run_id AND is_cut = true
            GROUP BY 1, 2, 3
            ORDER BY cut_cnt DESC
        """, {"run_id": run_id})
        if split_stats:
            issues.append({
                "id": "ISS-H-003",
                "type": "split_analysis",
                "severity": "info",
                "title": "切分触发器占比",
                "description": "Phase 04 三触发器各自命中的切分数量",
                "data": split_stats,
            })
            
        # Issue D: 异常/无效成员渗透率高的 Block
        high_invalid_blocks = await fetch_all(f"""
            SELECT
                block_id_final,
                member_cnt_total,
                (member_cnt_total - valid_cnt) AS invalid_cnt,
                ROUND((member_cnt_total - valid_cnt)::numeric / member_cnt_total, 3) AS invalid_ratio
            FROM {SCHEMA}.profile_final
            WHERE run_id = :run_id
              AND network_tier_final IN ('中型网络', '大型网络', '超大网络')
              AND member_cnt_total >= 100
              AND (member_cnt_total - valid_cnt)::float / member_cnt_total > 0.5
            ORDER BY invalid_ratio DESC
            LIMIT 10
        """, {"run_id": run_id})
        if high_invalid_blocks:
            issues.append({
                "id": "ISS-H-004",
                "type": "invalid_penetration",
                "severity": "high",
                "title": "异常成员高渗透块 (Top 10)",
                "description": "成员数≥100, 且无效/异常成员占比 > 50% 的核心网段",
                "data": [{
                    "block_id_final": r["block_id_final"],
                    "member_cnt": r["member_cnt_total"],
                    "invalid_ratio": f'{r["invalid_ratio"] * 100}%'
                } for r in high_invalid_blocks]
            })

    elif lib == "e":
        # Issue: atom_valid_cnt 阈值边界 (6 vs 7)
        atom_boundary = await fetch_all(f"""
            SELECT valid_ip_cnt AS atom_valid_cnt,
                   COUNT(*)::bigint AS atom_cnt,
                   CASE WHEN valid_ip_cnt >= 7 THEN 'pass' ELSE 'fail' END AS density_status
            FROM {SCHEMA}.e_atoms
            WHERE run_id = :run_id AND valid_ip_cnt BETWEEN 5 AND 9
            GROUP BY 1 ORDER BY 1
        """, {"run_id": run_id})
        if atom_boundary:
            issues.append({
                "id": "ISS-E-001",
                "type": "threshold_boundary",
                "severity": "high",
                "title": "原子密度阈值边界 (valid=6 vs 7)",
                "description": "atom_density≥0.2 等价 valid_ip_cnt≥7，边界附近原子数",
                "data": atom_boundary,
            })

        # Issue: run_len 阈值边界 (2 vs 3)
        run_boundary = await fetch_all(f"""
            SELECT run_len, COUNT(*)::bigint AS run_cnt,
                   SUM(run_len)::bigint AS total_atoms
            FROM {SCHEMA}.e_runs
            WHERE run_id = :run_id AND run_len BETWEEN 1 AND 5
            GROUP BY 1 ORDER BY 1
        """, {"run_id": run_id})
        if run_boundary:
            issues.append({
                "id": "ISS-E-002",
                "type": "threshold_boundary",
                "severity": "medium",
                "title": "run 长度阈值边界 (len=2 vs 3)",
                "description": "run_len≥3 才进入 E，run_len=2 差一步就进入",
                "data": run_boundary,
            })
            
        # Issue C: 自然块碎片化 (E库中某自然块被打碎的程度)
        fragmented_blocks = await fetch_all(f"""
            SELECT 
                e_run_id,
                run_len AS e_run_cnt,
                (SELECT COUNT(*)::bigint FROM {SCHEMA}.e_members m WHERE m.run_id=er.run_id AND m.shard_id=er.shard_id AND m.e_run_id=er.e_run_id) AS total_e_members
            FROM {SCHEMA}.e_runs er
            WHERE run_id = :run_id AND NOT short_run
            ORDER BY e_run_cnt DESC
            LIMIT 10
        """, {"run_id": run_id})
        
        if fragmented_blocks:
            issues.append({
                "id": "ISS-E-003",
                "type": "block_fragmentation",
                "severity": "high",
                "title": "严重碎片化的 e_runs (Top 10)",
                "description": "列出了具有大量子级的 e_runs, 可能有碎片化的自然块。",
                "data": [
                    {
                        "block_id_final": str(r["e_run_id"]),
                        "e_run_cnt": r["e_run_cnt"],
                        "total_e_members": r["total_e_members"]
                    } for r in fragmented_blocks
                ]
            })

    elif lib == "f":
        # F: 分析差一步进 E 的构成
        f_breakdown = await fetch_one(f"""
            WITH f_atoms AS (
                SELECT DISTINCT fm.atom27_id
                FROM {SCHEMA}.f_members fm
                WHERE fm.run_id = :run_id
            ),
            f_with_density AS (
                SELECT
                    fa.atom27_id,
                    ea.valid_ip_cnt,
                    ea.atom_density,
                    CASE WHEN ea.atom_density >= 0.2 THEN true ELSE false END AS density_pass
                FROM f_atoms fa
                LEFT JOIN {SCHEMA}.e_atoms ea
                    ON ea.run_id = :run_id AND ea.atom27_id = fa.atom27_id
            )
            SELECT
                COUNT(*)::bigint AS total_f_atoms,
                COUNT(*) FILTER(WHERE density_pass = false OR density_pass IS NULL)::bigint AS density_fail_atoms,
                COUNT(*) FILTER(WHERE density_pass = true)::bigint AS density_pass_but_run_fail_atoms
            FROM f_with_density
        """, {"run_id": run_id})
        if f_breakdown:
            issues.append({
                "id": "ISS-F-001",
                "type": "composition_analysis",
                "severity": "info",
                "title": "F 库成因拆解 (F-A vs F-B)",
                "description": "F-A=密度不够型, F-B=密度够但run不够型",
                "data": dict(f_breakdown),
            })
            
        # Issue D: F-B 无解释现象
        # Find members that are F-B (density pass) but lack a clear explanation for why the run failed
        # Currently simplified as an information point
        if f_breakdown and dict(f_breakdown).get("density_pass_but_run_fail_atoms", 0) > 0:
            val = dict(f_breakdown).get("density_pass_but_run_fail_atoms", 0)
            issues.append({
                "id": "ISS-F-002",
                "type": "missing_explanation",
                "severity": "medium",
                "title": "F-B 成员需跟进",
                "description": f"有 {val} 个 F 成员其所属原子密度合格但周边连打长度不够，需检查是否因为自然块切割导致。可参考【画像-机会带】",
                "data": {"count": val}
            })

    return {"lib": lib, "issues": issues}


# ============================================================
# 7) H 库 Tier 漂移矩阵
# ============================================================
@router.get("/runs/{run_id}/h/tier-drift")
async def h_tier_drift(run_id: str):
    """pre_tier × final_tier 二维矩阵（block_cnt + member_cnt）"""
    run_id = await _resolve_run(run_id)
    rows = await fetch_all(f"""
        SELECT
            COALESCE(pp.network_tier_pre, '未知') AS tier_pre,
            pf.network_tier_final AS tier_final,
            COUNT(*)::bigint AS block_cnt,
            SUM(pf.member_cnt_total)::bigint AS member_cnt
        FROM {SCHEMA}.profile_final pf
        LEFT JOIN {SCHEMA}.profile_pre pp
            ON pp.run_id = pf.run_id
            AND pp.block_id_natural = pf.block_id_parent
            AND pp.shard_id = pf.shard_id
        WHERE pf.run_id = :run_id
        GROUP BY 1, 2
        ORDER BY member_cnt DESC
    """, {"run_id": run_id})

    # also return totals per tier
    pre_tiers = sorted(set(r["tier_pre"] for r in rows))
    final_tiers = sorted(set(r["tier_final"] for r in rows))

    return {
        "run_id": run_id,
        "matrix": rows,
        "pre_tiers": pre_tiers,
        "final_tiers": final_tiers,
    }


# ============================================================
# 8) Split 触发器分析
# ============================================================
@router.get("/runs/{run_id}/h/split-analysis")
async def h_split_analysis(run_id: str):
    """Split 触发器分类 + 切分前后影响 + 是否与 H 块相关"""
    run_id = await _resolve_run(run_id)

    # trigger breakdown
    triggers = await fetch_all(f"""
        SELECT
            trigger_report, trigger_mobile, trigger_operator,
            COUNT(*)::bigint AS event_cnt,
            SUM(cntl_valid)::bigint AS left_members,
            SUM(cntr_valid)::bigint AS right_members,
            COUNT(*) FILTER(WHERE cntl_valid <= 3 OR cntr_valid <= 3)::bigint AS low_sample_cnt
        FROM {SCHEMA}.split_events_64
        WHERE run_id = :run_id AND is_cut = true
        GROUP BY 1, 2, 3
        ORDER BY event_cnt DESC
    """, {"run_id": run_id})

    # overall stats
    overall = await fetch_one(f"""
        SELECT
            COUNT(*)::bigint AS total_events,
            COUNT(*) FILTER(WHERE is_cut)::bigint AS actual_cuts,
            COUNT(*) FILTER(WHERE NOT is_cut)::bigint AS not_cut,
            COUNT(*) FILTER(WHERE is_cut AND (cntl_valid <= 3 OR cntr_valid <= 3))::bigint AS suspicious_cuts,
            ROUND(AVG(CASE WHEN is_cut THEN ABS(cvl - cvr) END)::numeric, 4) AS avg_cv_diff,
            ROUND(AVG(CASE WHEN is_cut THEN ratio_report END)::numeric, 4) AS avg_ratio_report
        FROM {SCHEMA}.split_events_64
        WHERE run_id = :run_id
    """, {"run_id": run_id})

    # how many H blocks come from split
    h_from_split = await fetch_one(f"""
        SELECT
            COUNT(*)::bigint AS h_total_blocks,
            COUNT(*) FILTER(WHERE block_id_final != block_id_parent)::bigint AS h_from_split,
            COUNT(*) FILTER(WHERE block_id_final = block_id_parent)::bigint AS h_no_split
        FROM {SCHEMA}.profile_final
        WHERE run_id = :run_id AND network_tier_final = '中型网络'
    """, {"run_id": run_id})

    return {
        "run_id": run_id,
        "triggers": triggers,
        "overall": dict(overall) if overall else {},
        "h_origin": dict(h_from_split) if h_from_split else {},
    }


# ============================================================
# 9) 可疑 Split 列表
# ============================================================
@router.get("/runs/{run_id}/h/suspicious-splits")
async def h_suspicious_splits(
    run_id: str,
    limit: int = Query(30, ge=5, le=100),
):
    """margin 过小或样本量不足的可疑切分点"""
    run_id = await _resolve_run(run_id)
    rows = await fetch_all(f"""
        SELECT
            se.block_id_natural,
            se.bucket64,
            se.cut_ip_long,
            se.cntl_valid,
            se.cntr_valid,
            ROUND(se.ratio_report::numeric, 4) AS ratio_report,
            ROUND(se.cvl::numeric, 4) AS cvl,
            ROUND(se.cvr::numeric, 4) AS cvr,
            ROUND(ABS(se.cvl - se.cvr)::numeric, 4) AS cv_margin,
            ROUND(se.mobile_diff::numeric, 4) AS mobile_diff,
            se.opl, se.opr,
            se.trigger_report, se.trigger_mobile, se.trigger_operator,
            pp.network_tier_pre,
            pp.simple_score
        FROM {SCHEMA}.split_events_64 se
        LEFT JOIN {SCHEMA}.profile_pre pp
            ON pp.run_id = se.run_id AND pp.block_id_natural = se.block_id_natural
        WHERE se.run_id = :run_id
          AND se.is_cut = true
          AND (se.cntl_valid <= 5 OR se.cntr_valid <= 5
               OR ABS(se.cvl - se.cvr) < 0.02)
        ORDER BY (se.cntl_valid + se.cntr_valid) ASC
        LIMIT :limit
    """, {"run_id": run_id, "limit": limit})

    return {
        "run_id": run_id,
        "suspicious_splits": rows,
        "count": len(rows),
    }


# ============================================================
# 10) 样本篮 — 边界带 IP 样本
# ============================================================
@router.get("/runs/{run_id}/library/{lib}/samples")
async def library_samples(
    run_id: str,
    lib: str,
    zone: str = Query("boundary", description="boundary|random|split"),
    limit: int = Query(20, ge=5, le=100),
):
    """获取边界带/随机/切分事件的 IP 样本，可跳转 Explorer"""
    run_id = await _resolve_run(run_id)
    lib = lib.lower()

    if lib == "h" and zone == "boundary":
        # H score ≈ 20 边界 IP
        rows = await fetch_all(f"""
            SELECT m.ip_long, s.ip_address, s.operator, s.reports, s.devices,
                   pf.simple_score, pf.density, pf.network_tier_final
            FROM {SCHEMA}.h_members m
            JOIN {SCHEMA}.source_members_slim s
                ON s.run_id = m.run_id AND s.ip_long = m.ip_long
            JOIN {SCHEMA}.profile_final pf
                ON pf.run_id = m.run_id AND pf.block_id_final = m.block_id_final
            WHERE m.run_id = :run_id AND pf.simple_score BETWEEN 18 AND 22
            ORDER BY RANDOM()
            LIMIT :limit
        """, {"run_id": run_id, "limit": limit})

    elif lib == "h" and zone == "split":
        # H IPs from split blocks
        rows = await fetch_all(f"""
            SELECT m.ip_long, s.ip_address, s.operator, s.reports, s.devices,
                   pf.simple_score, pf.density, pf.network_tier_final
            FROM {SCHEMA}.h_members m
            JOIN {SCHEMA}.source_members_slim s
                ON s.run_id = m.run_id AND s.ip_long = m.ip_long
            JOIN {SCHEMA}.profile_final pf
                ON pf.run_id = m.run_id AND pf.block_id_final = m.block_id_final
            WHERE m.run_id = :run_id AND pf.block_id_final != pf.block_id_parent
            ORDER BY RANDOM()
            LIMIT :limit
        """, {"run_id": run_id, "limit": limit})

    elif lib == "e" and zone == "boundary":
        # E atoms near density boundary
        rows = await fetch_all(f"""
            SELECT m.ip_long, s.ip_address, s.operator, s.reports, s.devices,
                   m.atom27_id
            FROM {SCHEMA}.e_members m
            JOIN {SCHEMA}.source_members_slim s
                ON s.run_id = m.run_id AND s.ip_long = m.ip_long
            JOIN {SCHEMA}.e_atoms ea
                ON ea.run_id = m.run_id AND ea.atom27_id = m.atom27_id
            WHERE m.run_id = :run_id AND ea.valid_ip_cnt BETWEEN 6 AND 8
            ORDER BY RANDOM()
            LIMIT :limit
        """, {"run_id": run_id, "limit": limit})

    else:
        # random sample for any lib
        rows = await fetch_all(f"""
            SELECT m.ip_long, s.ip_address, s.operator, s.reports, s.devices
            FROM {SCHEMA}.{lib}_members m
            JOIN {SCHEMA}.source_members_slim s
                ON s.run_id = m.run_id AND s.ip_long = m.ip_long
            WHERE m.run_id = :run_id
            ORDER BY RANDOM()
            LIMIT :limit
        """, {"run_id": run_id, "limit": limit})

    return {
        "run_id": run_id,
        "lib": lib,
        "zone": zone,
        "samples": rows,
        "count": len(rows),
    }


# ============================================================
# 11) What-If: wD 分段调整 (H 库影响评估)
# ============================================================
TIER_BINS = [
    (0, 10, "微型网络"),
    (10, 20, "小型网络"),
    (20, 30, "中型网络"),
    (30, 40, "大型网络"),
    (40, 9999, "超大网络"),
]

# Current production wD mapping
DEFAULT_WD_BINS = [
    {"gt": 0, "lte": 3.5, "wd": 1},
    {"gt": 3.5, "lte": 6.5, "wd": 2},
    {"gt": 6.5, "lte": 30, "wd": 4},
    {"gt": 30, "lte": 200, "wd": 16},
    {"gt": 200, "lte": 999999, "wd": 32},
]


def _score_to_tier(score: int) -> str:
    for lo, hi, name in TIER_BINS:
        if lo <= score < hi:
            return name
    return "超大网络"


def _density_to_wd(density: float, wd_bins: list) -> int:
    for b in wd_bins:
        if density > b.get("gt", 0) and density <= b.get("lte", 999999):
            return b["wd"]
    return 1


@router.post("/runs/{run_id}/experiments/whatif-wd")
async def whatif_wd(run_id: str, body: dict):
    """
    调整 wD 权重映射 → 重算 simple_score → 预测 H/tier 变化
    body: {"wd_bins": [{"gt":0,"lte":3.5,"wd":1}, ...]}
    """
    run_id = await _resolve_run(run_id)
    new_bins = body.get("wd_bins", DEFAULT_WD_BINS)

    # Fetch all blocks with their current scores
    blocks = await fetch_all(f"""
        SELECT block_id_final, density, wa, wd, simple_score,
               network_tier_final, member_cnt_total::bigint AS members
        FROM {SCHEMA}.profile_final
        WHERE run_id = :run_id
    """, {"run_id": run_id})

    # Recalculate
    old_h_blocks, new_h_blocks = 0, 0
    old_h_members, new_h_members = 0, 0

    gained, lost = [], []  # blocks changing H status
    tier_migration = {}    # (old_tier, new_tier) -> {blocks, members}

    for b in blocks:
        density = float(b["density"]) if b["density"] else 0
        wa = b["wa"] or 0
        old_wd = b["wd"] or 0
        old_score = b["simple_score"] or 0
        old_tier = b["network_tier_final"] or ""
        members = b["members"] or 0

        new_wd = _density_to_wd(density, new_bins)
        new_score = wa + new_wd
        new_tier = _score_to_tier(new_score)

        was_h = (old_tier == "中型网络")
        is_h = (new_tier == "中型网络")

        if was_h:
            old_h_blocks += 1
            old_h_members += members
        if is_h:
            new_h_blocks += 1
            new_h_members += members

        if not was_h and is_h:
            gained.append({
                "block_id": b["block_id_final"],
                "density": density,
                "old_score": old_score, "new_score": new_score,
                "old_tier": old_tier, "new_tier": new_tier,
                "members": members,
            })
        elif was_h and not is_h:
            lost.append({
                "block_id": b["block_id_final"],
                "density": density,
                "old_score": old_score, "new_score": new_score,
                "old_tier": old_tier, "new_tier": new_tier,
                "members": members,
            })

        if old_tier != new_tier:
            key = f"{old_tier} → {new_tier}"
            if key not in tier_migration:
                tier_migration[key] = {"blocks": 0, "members": 0}
            tier_migration[key]["blocks"] += 1
            tier_migration[key]["members"] += members

    # Sort by member impact
    gained.sort(key=lambda x: -x["members"])
    lost.sort(key=lambda x: -x["members"])

    return {
        "run_id": run_id,
        "current_wd_bins": DEFAULT_WD_BINS,
        "new_wd_bins": new_bins,
        "summary": {
            "old_h_blocks": old_h_blocks,
            "new_h_blocks": new_h_blocks,
            "delta_h_blocks": new_h_blocks - old_h_blocks,
            "old_h_members": old_h_members,
            "new_h_members": new_h_members,
            "delta_h_members": new_h_members - old_h_members,
        },
        "tier_migration": [
            {"transition": k, "blocks": v["blocks"], "members": v["members"]}
            for k, v in sorted(tier_migration.items(), key=lambda x: -x[1]["members"])
        ],
        "gained_top20": gained[:20],
        "lost_top20": lost[:20],
    }


# ============================================================
# 12) What-If: E 阈值调整 (E/F 影响评估)
# ============================================================
@router.post("/runs/{run_id}/experiments/whatif-e")
async def whatif_e(run_id: str, body: dict):
    """
    调整 E 的 atom_density 和 run_len 阈值 → 预测 E/F 成员变化
    body: {"min_valid_cnt": 6, "min_run_len": 2}
    """
    run_id = await _resolve_run(run_id)
    new_min_valid = body.get("min_valid_cnt", 7)
    new_min_run = body.get("min_run_len", 3)

    # Current E stats
    current = await fetch_one(f"""
        SELECT
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.e_members WHERE run_id = :run_id) AS e_members,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.f_members WHERE run_id = :run_id) AS f_members,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.e_atoms WHERE run_id = :run_id AND is_e_atom = true) AS e_atoms,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.e_runs WHERE run_id = :run_id AND NOT short_run) AS e_runs
    """, {"run_id": run_id})

    # Atom-level: how many atoms pass the new density threshold
    atom_impact = await fetch_all(f"""
        SELECT
            valid_ip_cnt,
            COUNT(*)::bigint AS atom_cnt,
            CASE WHEN valid_ip_cnt >= :new_min THEN 'new_pass' ELSE 'new_fail' END AS new_status,
            CASE WHEN valid_ip_cnt >= 7 THEN 'old_pass' ELSE 'old_fail' END AS old_status
        FROM {SCHEMA}.e_atoms
        WHERE run_id = :run_id
        GROUP BY 1 ORDER BY 1
    """, {"run_id": run_id, "new_min": new_min_valid})

    # Run-level: how many runs pass the new length threshold
    run_impact = await fetch_all(f"""
        SELECT
            run_len,
            COUNT(*)::bigint AS run_cnt,
            SUM(run_len)::bigint AS total_atoms,
            CASE WHEN run_len >= :new_min_run THEN 'new_pass' ELSE 'new_fail' END AS new_status,
            CASE WHEN run_len >= 3 THEN 'old_pass' ELSE 'old_fail' END AS old_status
        FROM {SCHEMA}.e_runs
        WHERE run_id = :run_id
        GROUP BY 1 ORDER BY 1
    """, {"run_id": run_id, "new_min_run": new_min_run})

    # Summary calculations
    old_pass_atoms = sum(r["atom_cnt"] for r in atom_impact if r["old_status"] == "old_pass")
    new_pass_atoms = sum(r["atom_cnt"] for r in atom_impact if r["new_status"] == "new_pass")
    old_pass_runs = sum(r["run_cnt"] for r in run_impact if r["old_status"] == "old_pass")
    new_pass_runs = sum(r["run_cnt"] for r in run_impact if r["new_status"] == "new_pass")
    old_pass_run_atoms = sum(r["total_atoms"] for r in run_impact if r["old_status"] == "old_pass")
    new_pass_run_atoms = sum(r["total_atoms"] for r in run_impact if r["new_status"] == "new_pass")

    return {
        "run_id": run_id,
        "params": {
            "old_min_valid_cnt": 7, "new_min_valid_cnt": new_min_valid,
            "old_min_run_len": 3, "new_min_run_len": new_min_run,
        },
        "current": dict(current) if current else {},
        "atom_impact": atom_impact,
        "run_impact": run_impact,
        "summary": {
            "old_pass_atoms": old_pass_atoms,
            "new_pass_atoms": new_pass_atoms,
            "delta_atoms": new_pass_atoms - old_pass_atoms,
            "old_pass_runs": old_pass_runs,
            "new_pass_runs": new_pass_runs,
            "delta_runs": new_pass_runs - old_pass_runs,
            "old_pass_run_atoms": old_pass_run_atoms,
            "new_pass_run_atoms": new_pass_run_atoms,
        },
    }

# ============================================================
# 7.5) H 库 Block 摘要看板 (h_block_summary)
# ============================================================
@router.get("/runs/{run_id}/h/kpi")
async def h_library_kpi(run_id: str):
    """H库宏观 KPI：总 block 数、总 IP、总上报、平均 score 等"""
    run_id = await _resolve_run(run_id)
    summary_status = await get_summary_status(run_id, "h")
    row = await fetch_one(f"""
        SELECT
            COUNT(*)::bigint               AS total_blocks,
            SUM(ip_count)::bigint          AS total_ips,
            ROUND(AVG(ip_count), 1)        AS avg_ip_per_block,
            ROUND(AVG(simple_score), 1)    AS avg_score,
            ROUND(AVG(density::numeric), 1) AS avg_density,
            SUM(total_reports)::bigint     AS total_reports,
            SUM(total_devices)::bigint     AS total_devices,
            SUM(daa_reports)::bigint       AS total_daa,
            SUM(dna_reports)::bigint       AS total_dna,
            SUM(wifi_devices)::bigint      AS total_wifi_devices,
            SUM(mobile_devices)::bigint    AS total_mobile_devices,
            ROUND(AVG(avg_active_days::numeric), 1) AS avg_active_days,
            SUM(unstable_ip_count)::bigint AS total_unstable_ips,
            COUNT(DISTINCT top_operator)   AS distinct_operators
        FROM {SCHEMA}.h_block_summary
        WHERE run_id = :run_id
    """, {"run_id": run_id})
    return {
        **(dict(row) if row else {}),
        "run_id": run_id,
        "summary_status": summary_status,
    }


@router.get("/runs/{run_id}/h/summary-blocks")
async def get_h_summary_blocks(
    run_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    sort_by: str = Query("ip_count", description="Sort field"),
    desc: bool = Query(True, description="Descending order"),
    filter_col: str = Query("", description="Column to filter on"),
    filter_min: str = Query("", description="Min value for filter"),
    filter_max: str = Query("", description="Max value for filter"),
    exclude_tagged: bool = Query(False, description="Exclude blocks matched by known profiling tags"),
):
    """H库 Block 级摘要（分页 + 排序 + 筛选），基于 h_block_summary 表。"""
    run_id = await _resolve_run(run_id)
    summary_status = await get_summary_status(run_id, "h")

    valid_sorts = {
        "ip_count", "total_reports", "total_reports_pre_filter",
        "total_devices", "total_devices_pre_filter", "density",
        "daa_reports", "dna_reports", "worktime_reports",
        "workday_reports", "weekend_reports", "late_night_reports",
        "wifi_devices", "mobile_devices", "vpn_devices", "wired_devices",
        "abnormal_net_devices", "total_apps",
        "android_id_count", "oaid_count", "google_id_count",
        "model_count", "manufacturer_count",
        "proxy_reports", "root_reports", "adb_reports",
        "charging_reports", "max_single_device_reports",
        "avg_reports_per_ip", "avg_devices_per_ip", "avg_active_days",
        "wifi_device_ratio", "mobile_device_ratio", "vpn_device_ratio",
        "workday_report_ratio", "late_night_report_ratio", "daa_dna_ratio",
        "abnormal_ip_count", "abnormal_ip_ratio", "abnormal_rule_hits_total",
        "abnormal_r1", "abnormal_r2", "abnormal_r3", "abnormal_r5",
        "abnormal_r7", "abnormal_r18", "abnormal_r19", "abnormal_r24", "abnormal_r26",
        "start_ip_text", "avg_apps_per_ip",
        "avg_model_per_ip", "avg_manufacturer_per_ip",
        "android_device_ratio", "oaid_device_ratio",
        "android_oaid_ratio",
        "report_oaid_ratio",
    }
    order_col = sort_by if sort_by in valid_sorts else "ip_count"
    order_dir = "DESC" if desc else "ASC"

    # Build filter clause
    filter_clause = ""
    params = {"run_id": run_id}
    if filter_col and filter_col in valid_sorts and filter_col != "start_ip_text":
        if filter_min:
            filter_clause += f" AND {filter_col} >= :fmin"
            params["fmin"] = float(filter_min)
        if filter_max:
            filter_clause += f" AND {filter_col} <= :fmax"
            params["fmax"] = float(filter_max)

    # Exclude tagged blocks
    if exclude_tagged:
        try:
            import json
            from pathlib import Path
            cfg_path = Path(__file__).parent.parent / "config" / "profile_tags.json"
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            for tag in cfg.get("tags", []):
                tag_logic = tag.get("logic", "AND")
                if tag_logic.upper() == "CUSTOM":
                    continue  # Skip CUSTOM tags in simple exclude
                parts = []
                for c in tag.get("conditions", []):
                    field = c["field"]
                    op = c["op"]
                    val = c["value"]
                    if not field.replace("_", "").isalnum():
                        continue
                    if op == "IN" and isinstance(val, list):
                        quoted = ", ".join(f"'{v}'" for v in val)
                        parts.append(f"COALESCE({field},'') IN ({quoted})")
                    elif op == "TIERED" and isinstance(val, dict):
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
                        if isinstance(val, str):
                            safe_val = val.replace("'", "''")
                            parts.append(f"{field} {op} '{safe_val}'")
                        else:
                            parts.append(f"{field} {op} {val}")
                if parts:
                    joiner = " OR " if tag_logic.upper() == "OR" else " AND "
                    filter_clause += f" AND NOT ({joiner.join(parts)})"
        except Exception:
            pass  # Silently ignore config errors

    offset = (page - 1) * page_size

    count_row = await fetch_one(
        f"SELECT COUNT(*) as total FROM {SCHEMA}.h_block_summary WHERE run_id = :run_id{filter_clause}",
        params
    )
    total = count_row["total"] if count_row else 0

    sql = f"""
        SELECT *
        FROM {SCHEMA}.h_block_summary
        WHERE run_id = :run_id{filter_clause}
        ORDER BY {order_col} {order_dir} NULLS LAST
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = page_size
    params["offset"] = offset
    rows = await fetch_all(sql, params)

    return {
        "run_id": run_id,
        "total": total,
        "page": page,
        "page_size": page_size,
        "blocks": [dict(r) for r in rows],
        "summary_status": summary_status,
    }


# ============================================================
# 8) E 库 CIDR 摘要看板 (e_cidr_summary)
# ============================================================
@router.get("/runs/{run_id}/e/kpi")
async def e_library_kpi(run_id: str):
    """E库宏观 KPI：总 CIDR 块数、总 IP、总上报、总设备等"""
    run_id = await _resolve_run(run_id)
    summary_status = await get_summary_status(run_id, "e")
    row = await fetch_one(f"""
        SELECT
            COUNT(*)::bigint               AS total_runs,
            SUM(ip_count)::bigint          AS total_ips,
            ROUND(AVG(ip_count), 1)        AS avg_ip_per_run,
            ROUND(AVG(run_len), 1)         AS avg_run_len,
            SUM(total_reports)::bigint     AS total_reports,
            SUM(total_devices)::bigint     AS total_devices,
            SUM(daa_reports)::bigint       AS total_daa,
            SUM(dna_reports)::bigint       AS total_dna,
            SUM(wifi_devices)::bigint      AS total_wifi_devices,
            SUM(mobile_devices)::bigint    AS total_mobile_devices,
            SUM(vpn_devices)::bigint       AS total_vpn_devices,
            ROUND(AVG(avg_active_days), 1) AS avg_active_days,
            SUM(unstable_ip_count)::bigint AS total_unstable_ips,
            COUNT(DISTINCT top_operator)   AS distinct_operators
        FROM {SCHEMA}.e_cidr_summary
        WHERE run_id = :run_id
    """, {"run_id": run_id})
    return {
        **(dict(row) if row else {}),
        "run_id": run_id,
        "summary_status": summary_status,
    }


@router.get("/runs/{run_id}/e/summary-blocks")
async def get_e_summary_blocks(
    run_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    sort_by: str = Query("ip_count", description="Sort field"),
    desc: bool = Query(True, description="Descending order"),
    filter_col: str = Query("", description="Column to filter on"),
    filter_min: str = Query("", description="Min value for filter"),
    filter_max: str = Query("", description="Max value for filter"),
    exclude_tagged: bool = Query(False, description="Exclude blocks matched by known profiling tags"),
):
    """
    E库 CIDR 块级摘要（分页 + 排序 + 筛选），基于 e_cidr_summary 表。
    """
    run_id = await _resolve_run(run_id)
    summary_status = await get_summary_status(run_id, "e")

    valid_sorts = {
        "ip_count", "total_reports", "total_devices", "run_len",
        "daa_reports", "dna_reports", "wifi_devices", "mobile_devices",
        "avg_reports_per_ip", "avg_devices_per_ip", "avg_active_days",
        "wifi_device_ratio", "mobile_device_ratio", "vpn_device_ratio",
        "workday_report_ratio", "late_night_report_ratio",
        "unstable_ip_count", "manufacturer_count", "ssid_count",
        "proxy_reports", "root_reports", "adb_reports",
        "ip_density", "daa_dna_ratio", "total_apps",
        "abnormal_ip_count", "abnormal_ip_ratio",
        "avg_apps_per_device", "avg_apps_per_ip",
        "start_ip_text", "network_tier_final", "simple_score",
        "android_id_count", "oaid_count",
        "charging_reports", "max_single_device_reports",
    }
    order_col = sort_by if sort_by in valid_sorts else "ip_count"
    order_dir = "DESC" if desc else "ASC"

    # Build filter clause
    filter_clause = ""
    params = {"run_id": run_id}
    if filter_col and filter_col in valid_sorts and filter_col != "start_ip_text":
        if filter_min:
            filter_clause += f" AND {filter_col} >= :fmin"
            params["fmin"] = float(filter_min)
        if filter_max:
            filter_clause += f" AND {filter_col} <= :fmax"
            params["fmax"] = float(filter_max)

    # Exclude tagged blocks
    if exclude_tagged:
        try:
            import json as _json
            from pathlib import Path as _Path
            cfg_path = _Path(__file__).parent.parent / "config" / "e_profile_tags.json"
            if cfg_path.exists():
                with open(cfg_path, "r", encoding="utf-8") as f:
                    cfg = _json.load(f)
                for tag in cfg.get("tags", []):
                    tag_logic = tag.get("logic", "AND")
                    if tag_logic.upper() == "CUSTOM":
                        continue
                    parts = []
                    for c in tag.get("conditions", []):
                        field = c["field"]
                        op = c["op"]
                        val = c["value"]
                        if not field.replace("_", "").isalnum():
                            continue
                        if op == "IN" and isinstance(val, list):
                            quoted = ", ".join(f"'{v}'" for v in val)
                            parts.append(f"COALESCE({field},'') IN ({quoted})")
                        elif op in (">=", "<=", ">", "<", "=", "!="):
                            if isinstance(val, str):
                                safe_val = val.replace("'", "''")
                                parts.append(f"{field} {op} '{safe_val}'")
                            else:
                                parts.append(f"{field} {op} {val}")
                    if parts:
                        joiner = " OR " if tag_logic.upper() == "OR" else " AND "
                        filter_clause += f" AND NOT ({joiner.join(parts)})"
        except Exception:
            pass

    offset = (page - 1) * page_size

    count_row = await fetch_one(
        f"SELECT COUNT(*) as total FROM {SCHEMA}.e_cidr_summary WHERE run_id = :run_id{filter_clause}",
        params
    )
    total = count_row["total"] if count_row else 0

    sql = f"""
        SELECT *
        FROM {SCHEMA}.e_cidr_summary
        WHERE run_id = :run_id{filter_clause}
        ORDER BY {order_col} {order_dir} NULLS LAST
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = page_size
    params["offset"] = offset
    rows = await fetch_all(sql, params)

    return {
        "run_id": run_id,
        "total": total,
        "page": page,
        "page_size": page_size,
        "blocks": [dict(r) for r in rows],
        "summary_status": summary_status,
    }


# ============================================================
# 8.5) F 库 IP 摘要看板 (f_ip_summary)
# ============================================================
@router.get("/runs/{run_id}/f/kpi")
async def f_library_kpi(run_id: str):
    """F库宏观 KPI：总 IP、总上报、总设备、稳定性分布等"""
    run_id = await _resolve_run(run_id)
    summary_status = await get_summary_status(run_id, "f")
    row = await fetch_one(f"""
        SELECT
            COUNT(*)::bigint AS total_ips,
            SUM(total_reports)::bigint AS total_reports,
            SUM(total_devices)::bigint AS total_devices,
            ROUND(AVG(total_reports)::numeric, 1) AS avg_reports_per_ip,
            ROUND(AVG(total_devices)::numeric, 1) AS avg_devices_per_ip,
            ROUND(AVG(active_days)::numeric, 1) AS avg_active_days,
            COUNT(*) FILTER (WHERE ip_stability = '稳定网络')::bigint AS stable_ips,
            COUNT(*) FILTER (WHERE ip_stability = '不稳定网络')::bigint AS unstable_ips,
            COUNT(*) FILTER (WHERE ip_stability = '消失网络')::bigint AS vanished_ips,
            COUNT(DISTINCT top_operator) AS distinct_operators,
            SUM(proxy_reports)::bigint AS proxy_reports,
            SUM(root_reports)::bigint AS root_reports,
            SUM(adb_reports)::bigint AS adb_reports
        FROM {SCHEMA}.f_ip_summary
        WHERE run_id = :run_id
    """, {"run_id": run_id})
    return {
        **(dict(row) if row else {}),
        "run_id": run_id,
        "summary_status": summary_status,
    }


@router.get("/runs/{run_id}/f/summary-ips")
async def get_f_summary_ips(
    run_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    sort_by: str = Query("total_reports", description="Sort field"),
    desc: bool = Query(True, description="Descending order"),
    filter_col: str = Query("", description="Column to filter on"),
    filter_min: str = Query("", description="Min value for filter"),
    filter_max: str = Query("", description="Max value for filter"),
):
    """F库单 IP 摘要（分页 + 排序 + 筛选），基于 f_ip_summary 表。"""
    run_id = await _resolve_run(run_id)
    summary_status = await get_summary_status(run_id, "f")

    valid_sorts = {
        "ip_address", "ip_stability", "top_operator",
        "total_reports", "total_reports_pre_filter", "total_devices",
        "total_devices_pre_filter", "daa_reports", "dna_reports",
        "worktime_reports", "workday_reports", "weekend_reports", "late_night_reports",
        "wifi_devices", "mobile_devices", "vpn_devices", "wired_devices",
        "abnormal_net_devices", "total_apps", "active_days",
        "android_id_count", "oaid_count", "google_id_count", "boot_id_count",
        "model_count", "manufacturer_count", "ssid_count", "bssid_count",
        "proxy_reports", "root_reports", "adb_reports", "charging_reports",
        "max_single_device_reports", "gateway_reports", "ethernet_reports",
        "wifi_device_ratio", "mobile_device_ratio", "vpn_device_ratio",
        "workday_report_ratio", "weekend_report_ratio", "late_night_report_ratio",
        "root_report_ratio", "daa_dna_ratio",
    }
    order_col = sort_by if sort_by in valid_sorts else "total_reports"
    order_dir = "DESC" if desc else "ASC"

    filter_clause = ""
    params = {"run_id": run_id}
    text_filters = {"ip_address", "ip_stability", "top_operator"}
    if filter_col and filter_col in valid_sorts:
        if filter_col in text_filters:
            if filter_min:
                filter_clause += f" AND {filter_col} ILIKE :ftext"
                params["ftext"] = f"%{filter_min.strip()}%"
        else:
            if filter_min:
                filter_clause += f" AND {filter_col} >= :fmin"
                params["fmin"] = float(filter_min)
            if filter_max:
                filter_clause += f" AND {filter_col} <= :fmax"
                params["fmax"] = float(filter_max)

    offset = (page - 1) * page_size
    count_row = await fetch_one(
        f"SELECT COUNT(*) AS total FROM {SCHEMA}.f_ip_summary WHERE run_id = :run_id{filter_clause}",
        params,
    )
    total = count_row["total"] if count_row else 0

    sql = f"""
        SELECT *
        FROM {SCHEMA}.f_ip_summary
        WHERE run_id = :run_id{filter_clause}
        ORDER BY {order_col} {order_dir} NULLS LAST
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = page_size
    params["offset"] = offset
    rows = await fetch_all(sql, params)

    return {
        "run_id": run_id,
        "total": total,
        "page": page,
        "page_size": page_size,
        "ips": [dict(r) for r in rows],
        "summary_status": summary_status,
    }


# ============================================================
# 13) 字段管理工作台 — 保存用户的字段审阅和自定义计算列
# ============================================================
FIELD_CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "docs" / "field_configs"

@router.post("/field-config")
async def save_field_config(body: dict = Body(...)):
    """
    保存字段审阅和自定义计算列配置为 JSON 文件。
    body: {
        "library": "h" | "e",
        "fields": [{"name": "...", "keep": true/false, "comment": "..."}],
        "computed_columns": [{"name": "...", "formula": "...", "fieldA": "...", "op": "+|-|*|/", "fieldB": "..."}]
    }
    """
    FIELD_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    lib = body.get("library", "h")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{lib}_field_config_{ts}.json"
    filepath = FIELD_CONFIG_DIR / filename

    output = {
        "library": lib,
        "created_at": datetime.now().isoformat(),
        "fields": body.get("fields", []),
        "computed_columns": body.get("computed_columns", []),
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    return JSONResponse(content={
        "status": "ok",
        "file": str(filepath),
        "filename": filename,
        "field_count": len(output["fields"]),
        "computed_count": len(output["computed_columns"]),
    })


@router.get("/ip-neighborhood")
async def ip_neighborhood(
    ip: str = Query(..., description="IP address to search, e.g. 1.2.3.4"),
    radius: int = Query(8, ge=1, le=100, description="Number of IPs before and after"),
    run_id: str = Query("latest", description="Run ID")
):
    """IP 邻域查询 —— 在原始IP源表中查询目标IP前后N个IP的全部属性。"""
    import ipaddress

    run_id = await _resolve_run(run_id)

    # 将 IP 转为 bigint
    try:
        ip_long = int(ipaddress.ip_address(ip))
    except ValueError:
        return {"error": f"Invalid IP address: {ip}"}

    src_table = 'public."ip库构建项目_ip源表_20250811_20250824_v2_1"'

    # 先检查目标 IP 是否在源表中
    target_in_src = await fetch_one(f"""
        SELECT ip_long FROM {src_table} WHERE ip_long = :ip_long LIMIT 1
    """, {"ip_long": ip_long})

    search_ip_long = ip_long  # 默认以用户输入 IP 为中心
    cidr_redirect = None      # 如果重定向了，记下来

    if not target_in_src:
        # IP 不在源表中 —— 检查是否是 E 库 CIDR 段起始 IP
        e_cidr = await fetch_one(f"""
            SELECT e_run_id, start_ip_text, ip_count, run_len,
                   total_reports, total_devices,
                   wifi_device_ratio, mobile_device_ratio,
                   NULL AS network_tier_final, NULL AS simple_score,
                   top_operator, proxy_reports, root_reports
            FROM {SCHEMA}.e_cidr_summary
            WHERE run_id = :run_id AND start_ip_text = :ip
        """, {"run_id": run_id, "ip": ip})
        if e_cidr:
            # 找到 E 库段，以段成员的最小 IP 为中心搜邻域
            min_member = await fetch_one(f"""
                SELECT MIN(ip_long) AS min_ip FROM {SCHEMA}.e_members
                WHERE run_id = :run_id AND e_run_id = :e_run_id
            """, {"run_id": run_id, "e_run_id": e_cidr["e_run_id"]})
            if min_member and min_member["min_ip"]:
                search_ip_long = int(min_member["min_ip"])
                cidr_redirect = {
                    "original_ip": ip,
                    "original_ip_long": ip_long,
                    "redirected_to": str(ipaddress.ip_address(search_ip_long)),
                    "redirected_ip_long": search_ip_long,
                    "reason": f"输入 IP 是 E 库 CIDR 段起始标记，实际成员从 {str(ipaddress.ip_address(search_ip_long))} 开始"
                }

    # 查前后 radius 个 IP (按 ip_long 排序)
    sql = f"""
        (SELECT * FROM {src_table} WHERE ip_long <= :ip_long ORDER BY ip_long DESC LIMIT :before)
        UNION ALL
        (SELECT * FROM {src_table} WHERE ip_long > :ip_long ORDER BY ip_long ASC LIMIT :after)
        ORDER BY ip_long
    """
    rows = await fetch_all(sql, {
        "ip_long": search_ip_long,
        "before": radius + 1,
        "after": radius
    })

    # 查该 IP 是否在 H库
    h_block = None
    h_row = await fetch_one(f"""
        SELECT hm.block_id_final, bs.start_ip_text, bs.ip_count,
               bs.unstable_ip_count, bs.network_tier_final
        FROM {SCHEMA}.h_members hm
        LEFT JOIN {SCHEMA}.h_block_summary bs
            ON hm.block_id_final = bs.block_id_final AND bs.run_id = :run_id
        WHERE hm.ip_long = :ip_long AND hm.run_id = :run_id
    """, {"ip_long": search_ip_long, "run_id": run_id})
    if h_row:
        h_block = dict(h_row)

    # 查该 IP 是否在 E库
    e_block = None
    e_row = await fetch_one(f"""
        SELECT em.e_run_id, em.atom27_id,
               es.start_ip_text, es.ip_count, es.run_len,
               es.total_reports, es.total_devices,
               es.wifi_device_ratio, es.mobile_device_ratio,
               NULL AS network_tier_final, NULL AS simple_score,
               es.top_operator, es.proxy_reports, es.root_reports
        FROM {SCHEMA}.e_members em
        LEFT JOIN {SCHEMA}.e_cidr_summary es
            ON em.e_run_id = es.e_run_id AND es.run_id = :run_id
        WHERE em.ip_long = :ip_long AND em.run_id = :run_id
    """, {"ip_long": search_ip_long, "run_id": run_id})
    if e_row:
        e_block = dict(e_row)

    # 如果都没找到，尝试按 start_ip_text 查 e_cidr_summary
    if not h_block and not e_block:
        e_cidr_fb = await fetch_one(f"""
            SELECT e_run_id, start_ip_text, ip_count, run_len,
                   total_reports, total_devices,
                   wifi_device_ratio, mobile_device_ratio,
                   NULL AS network_tier_final, NULL AS simple_score,
                   top_operator, proxy_reports, root_reports
            FROM {SCHEMA}.e_cidr_summary
            WHERE run_id = :run_id AND start_ip_text = :ip
        """, {"run_id": run_id, "ip": ip})
        if e_cidr_fb:
            e_block = dict(e_cidr_fb)

    # 收集邻域内所有 IP 的 e_run_id，批量查询它们所属的 e_cidr
    ip_longs = [int(dict(r).get("ip_long") or dict(r).get("ipv4_bigint") or 0) for r in rows]
    e_member_map = {}
    if ip_longs:
        ip_list_str = ",".join(str(il) for il in ip_longs if il > 0)
        if ip_list_str:
            e_members_rows = await fetch_all(f"""
                SELECT ip_long, e_run_id FROM {SCHEMA}.e_members
                WHERE run_id = :run_id AND ip_long IN ({ip_list_str})
            """, {"run_id": run_id})
            for em in e_members_rows:
                e_member_map[int(em["ip_long"])] = em["e_run_id"]

    # 将 ip_long 转为点分格式
    result_rows = []
    for r in rows:
        d = dict(r)
        long_val = d.get("ip_long") or d.get("ipv4_bigint") or 0
        if long_val:
            long_val = int(long_val)
            d["ip_text"] = str(ipaddress.ip_address(long_val))
        d["is_target"] = (long_val == search_ip_long)
        # 标注所属库
        if long_val in e_member_map:
            d["lib"] = "E"
            d["e_run_id"] = e_member_map[long_val]
        result_rows.append(d)

    return {
        "ip": ip,
        "ip_long": ip_long,
        "search_ip_long": search_ip_long,
        "radius": radius,
        "h_block": h_block,
        "e_block": e_block,
        "cidr_redirect": cidr_redirect,
        "total": len(result_rows),
        "rows": result_rows
    }
