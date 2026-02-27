"""
Dashboard API — 协同调度看板相关端点
"""
from fastapi import APIRouter
from models.database import fetch_all, fetch_one

router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])

SCHEMA = "rb20_v2_5"


@router.get("/runs")
async def list_runs():
    """获取所有 run_id 列表"""
    rows = await fetch_all(f"""
        SELECT DISTINCT run_id,
               MIN(created_at) AS started_at
        FROM {SCHEMA}.shard_plan
        GROUP BY run_id
        ORDER BY started_at DESC
    """)
    return rows


@router.get("/runs/{run_id}/overview")
async def run_overview(run_id: str):
    """单次运行的总览统计"""
    stats = await fetch_one(f"""
        SELECT
            :run_id AS run_id,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.source_members WHERE run_id = :run_id) AS source_members_cnt,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.h_members WHERE run_id = :run_id) AS h_members_cnt,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.e_members WHERE run_id = :run_id) AS e_members_cnt,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.f_members WHERE run_id = :run_id) AS f_members_cnt,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.keep_members WHERE run_id = :run_id) AS keep_members_cnt,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.drop_members WHERE run_id = :run_id) AS drop_members_cnt,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.block_natural WHERE run_id = :run_id) AS block_natural_cnt,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.block_final WHERE run_id = :run_id) AS block_final_cnt,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.shard_plan WHERE run_id = :run_id) AS shard_cnt,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.qa_assert WHERE run_id = :run_id) AS qa_total,
            (SELECT COUNT(*)::bigint FROM {SCHEMA}.qa_assert WHERE run_id = :run_id AND pass_flag = true) AS qa_passed
    """, {"run_id": run_id})
    return stats


@router.get("/runs/{run_id}/shards")
async def shard_matrix(run_id: str):
    """65 个 Shard 的状态矩阵"""
    rows = await fetch_all(f"""
        WITH sp AS (
            SELECT shard_id, ip_long_start, ip_long_end, est_rows
            FROM {SCHEMA}.shard_plan WHERE run_id = :run_id
        ),
        sm AS (
            SELECT shard_id, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.source_members WHERE run_id = :run_id GROUP BY 1
        ),
        bn AS (
            SELECT shard_id, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.block_natural WHERE run_id = :run_id GROUP BY 1
        ),
        bf AS (
            SELECT shard_id, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.block_final WHERE run_id = :run_id GROUP BY 1
        ),
        pf AS (
            SELECT shard_id, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.profile_final WHERE run_id = :run_id GROUP BY 1
        ),
        km AS (
            SELECT shard_id, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.keep_members WHERE run_id = :run_id GROUP BY 1
        ),
        em AS (
            SELECT shard_id, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.e_members WHERE run_id = :run_id GROUP BY 1
        ),
        fm AS (
            SELECT shard_id, COUNT(*)::bigint AS cnt
            FROM {SCHEMA}.f_members WHERE run_id = :run_id GROUP BY 1
        )
        SELECT
            sp.shard_id,
            sp.ip_long_start,
            sp.ip_long_end,
            sp.est_rows,
            COALESCE(sm.cnt, 0) AS source_members_cnt,
            COALESCE(bn.cnt, 0) AS block_natural_cnt,
            COALESCE(bf.cnt, 0) AS block_final_cnt,
            COALESCE(pf.cnt, 0) AS profile_final_cnt,
            COALESCE(km.cnt, 0) AS keep_members_cnt,
            COALESCE(em.cnt, 0) AS e_members_cnt,
            COALESCE(fm.cnt, 0) AS f_members_cnt,
            -- phases completed: crude estimation
            (CASE WHEN COALESCE(sm.cnt,0) > 0 THEN 1 ELSE 0 END
             + CASE WHEN COALESCE(bn.cnt,0) > 0 THEN 1 ELSE 0 END
             + CASE WHEN COALESCE(bf.cnt,0) > 0 THEN 1 ELSE 0 END
             + CASE WHEN COALESCE(pf.cnt,0) > 0 THEN 1 ELSE 0 END
             + CASE WHEN COALESCE(km.cnt,0) > 0 THEN 1 ELSE 0 END
             + CASE WHEN COALESCE(em.cnt,0) > 0 THEN 1 ELSE 0 END
             + CASE WHEN COALESCE(fm.cnt,0) > 0 THEN 1 ELSE 0 END
            ) AS phases_completed
        FROM sp
        LEFT JOIN sm USING (shard_id)
        LEFT JOIN bn USING (shard_id)
        LEFT JOIN bf USING (shard_id)
        LEFT JOIN pf USING (shard_id)
        LEFT JOIN km USING (shard_id)
        LEFT JOIN em USING (shard_id)
        LEFT JOIN fm USING (shard_id)
        ORDER BY sp.shard_id
    """, {"run_id": run_id})
    return rows


@router.get("/runs/{run_id}/qa")
async def qa_results(run_id: str):
    """QA 断言结果"""
    rows = await fetch_all(f"""
        SELECT assert_name, severity, pass_flag, details, created_at
        FROM {SCHEMA}.qa_assert
        WHERE run_id = :run_id
        ORDER BY created_at
    """, {"run_id": run_id})
    return rows


@router.get("/runs/{run_id}/step-stats")
async def step_stats(run_id: str, step_id: str = None):
    """各步骤统计指标"""
    where = "WHERE run_id = :run_id"
    params = {"run_id": run_id}
    if step_id:
        where += " AND step_id = :step_id"
        params["step_id"] = step_id
    rows = await fetch_all(f"""
        SELECT step_id, shard_id, metric_name,
               metric_value_numeric, metric_value_text, created_at
        FROM {SCHEMA}.step_stats
        {where}
        ORDER BY step_id, shard_id, metric_name
    """, params)
    return rows


@router.get("/runs/{run_id}/network-tier-distribution")
async def network_tier_distribution(run_id: str):
    """最终块的网络规模分布"""
    rows = await fetch_all(f"""
        SELECT
            network_tier_final AS network_tier,
            COUNT(*)::bigint AS block_count,
            SUM(member_cnt_total)::bigint AS member_count
        FROM {SCHEMA}.profile_final
        WHERE run_id = :run_id
        GROUP BY network_tier_final
        ORDER BY block_count DESC
    """, {"run_id": run_id})
    return rows


@router.get("/runs/{run_id}/classification-summary")
async def classification_summary(run_id: str):
    """H/E/F 分类汇总"""
    rows = await fetch_all(f"""
        SELECT 'H' AS category, COUNT(*)::bigint AS member_cnt
        FROM {SCHEMA}.h_members WHERE run_id = :run_id
        UNION ALL
        SELECT 'E', COUNT(*)::bigint FROM {SCHEMA}.e_members WHERE run_id = :run_id
        UNION ALL
        SELECT 'F', COUNT(*)::bigint FROM {SCHEMA}.f_members WHERE run_id = :run_id
        UNION ALL
        SELECT 'Keep', COUNT(*)::bigint FROM {SCHEMA}.keep_members WHERE run_id = :run_id
        UNION ALL
        SELECT 'Drop', COUNT(*)::bigint FROM {SCHEMA}.drop_members WHERE run_id = :run_id
    """, {"run_id": run_id})
    return rows
