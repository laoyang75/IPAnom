"""
Data Explorer API — 数据微观探查端点
"""
import ipaddress
from fastapi import APIRouter, HTTPException, Query
from models.database import fetch_all, fetch_one

router = APIRouter(prefix="/api/explore", tags=["Explorer"])

SCHEMA = "rb20_v2_5"


def ip_to_long(ip_str: str) -> int:
    """Convert IP address string to integer."""
    return int(ipaddress.IPv4Address(ip_str))


def long_to_ip(ip_long: int) -> str:
    """Convert integer to IP address string."""
    return str(ipaddress.IPv4Address(ip_long))


@router.get("/ip/{ip_address}")
async def trace_by_ip(ip_address: str, run_id: str = Query(None)):
    """输入 IP 地址，返回完整溯源链"""
    try:
        ip_long = ip_to_long(ip_address)
    except (ipaddress.AddressValueError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid IP address: {ip_address}")
    return await _trace_ip(ip_long, run_id)


@router.get("/ip-long/{ip_long}")
async def trace_by_ip_long(ip_long: int, run_id: str = Query(None)):
    """输入 ip_long，返回完整溯源链"""
    return await _trace_ip(ip_long, run_id)


async def _trace_ip(ip_long: int, run_id: str = None):
    """Core trace logic: reconstruct the IP's journey through the pipeline."""
    # If no run_id, use the latest
    if not run_id:
        row = await fetch_one(f"""
            SELECT run_id FROM {SCHEMA}.shard_plan
            ORDER BY created_at DESC LIMIT 1
        """)
        if not row:
            raise HTTPException(status_code=404, detail="No runs found")
        run_id = row["run_id"]

    result = {
        "ip_long": ip_long,
        "ip_address": long_to_ip(ip_long),
        "run_id": run_id,
        "classification": "未找到",
        "stages": []
    }

    # Stage 1: source_members
    sm = await fetch_one(f"""
        SELECT shard_id, ip_address, "IP归属国家" AS country, "IP归属运营商" AS operator,
               "上报次数" AS report_count, "设备数量" AS device_count,
               "移动网络设备数量" AS mobile_device_count,
               is_abnormal, is_valid
        FROM {SCHEMA}.source_members
        WHERE run_id = :run_id AND ip_long = :ip_long
    """, {"run_id": run_id, "ip_long": ip_long})

    if not sm:
        result["stages"].append({
            "stage": "source_members",
            "status": "not_found",
            "detail": "该 IP 不在源成员库中（可能被非中国过滤排除）"
        })
        return result

    result["ip_address"] = sm.get("ip_address", result["ip_address"])
    result["country"] = sm.get("country")
    result["operator"] = sm.get("operator")
    result["report_count"] = sm.get("report_count")
    result["device_count"] = sm.get("device_count")
    result["mobile_device_count"] = sm.get("mobile_device_count")
    result["is_abnormal"] = sm.get("is_abnormal")
    result["is_valid"] = sm.get("is_valid")
    result["shard_id"] = sm.get("shard_id")
    result["stages"].append({
        "stage": "source_members",
        "status": "found",
        "detail": f"Shard {sm['shard_id']} | {'⚠️ 异常' if sm.get('is_abnormal') else '✅ 正常'} | 国家: {sm.get('country')} | 运营商: {sm.get('operator')}"
    })

    # Stage 2: map_member_block_natural
    nbm = await fetch_one(f"""
        SELECT block_id_natural
        FROM {SCHEMA}.map_member_block_natural
        WHERE run_id = :run_id AND ip_long = :ip_long
    """, {"run_id": run_id, "ip_long": ip_long})

    if nbm:
        result["block_id_natural"] = nbm["block_id_natural"]
        result["stages"].append({
            "stage": "natural_block_mapping",
            "status": "found",
            "detail": f"归属自然块: {nbm['block_id_natural']}"
        })

        # Stage 3: block_natural details
        bn = await fetch_one(f"""
            SELECT ip_start, ip_end, member_cnt_total
            FROM {SCHEMA}.block_natural
            WHERE run_id = :run_id AND block_id_natural = :bid
        """, {"run_id": run_id, "bid": nbm["block_id_natural"]})
        if bn:
            result["block_natural_ip_start"] = bn["ip_start"]
            result["block_natural_ip_end"] = bn["ip_end"]
            result["block_natural_member_cnt"] = bn["member_cnt_total"]

        # Stage 4: profile_pre
        pp = await fetch_one(f"""
            SELECT keep_flag, drop_reason, network_tier_pre, simple_score,
                   density, valid_cnt
            FROM {SCHEMA}.profile_pre
            WHERE run_id = :run_id AND block_id_natural = :bid
        """, {"run_id": run_id, "bid": nbm["block_id_natural"]})

        if pp:
            result["keep_flag"] = pp["keep_flag"]
            result["drop_reason"] = pp.get("drop_reason")
            result["network_tier_pre"] = pp.get("network_tier_pre")
            result["simple_score_pre"] = pp.get("simple_score")
            result["density_pre"] = float(pp["density"]) if pp.get("density") is not None else None
            result["valid_cnt_pre"] = pp.get("valid_cnt")

            if pp["keep_flag"]:
                result["stages"].append({
                    "stage": "profile_pre",
                    "status": "keep",
                    "detail": f"✅ Keep | 网络规模: {pp.get('network_tier_pre')} | 密度: {pp.get('density')} | Valid: {pp.get('valid_cnt')}"
                })
            else:
                result["stages"].append({
                    "stage": "profile_pre",
                    "status": "drop",
                    "detail": f"❌ Drop | 原因: {pp.get('drop_reason')} | 网络规模: {pp.get('network_tier_pre')}"
                })

    # Stage 5: Check final block mapping
    fbm = await fetch_one(f"""
        SELECT block_id_final, block_id_parent
        FROM {SCHEMA}.map_member_block_final
        WHERE run_id = :run_id AND ip_long = :ip_long
    """, {"run_id": run_id, "ip_long": ip_long})

    if fbm:
        result["block_id_final"] = fbm["block_id_final"]
        # Get final block detail
        bf = await fetch_one(f"""
            SELECT ip_start, ip_end
            FROM {SCHEMA}.block_final
            WHERE run_id = :run_id AND block_id_final = :bid
        """, {"run_id": run_id, "bid": fbm["block_id_final"]})
        if bf:
            result["block_final_ip_start"] = bf["ip_start"]
            result["block_final_ip_end"] = bf["ip_end"]

        # Get final profile
        pf = await fetch_one(f"""
            SELECT network_tier_final, simple_score, density, valid_cnt
            FROM {SCHEMA}.profile_final
            WHERE run_id = :run_id AND block_id_final = :bid
        """, {"run_id": run_id, "bid": fbm["block_id_final"]})
        if pf:
            result["network_tier_final"] = pf.get("network_tier_final")
            result["simple_score_final"] = pf.get("simple_score")
            result["density_final"] = float(pf["density"]) if pf.get("density") is not None else None
            result["valid_cnt_final"] = pf.get("valid_cnt")
            result["stages"].append({
                "stage": "profile_final",
                "status": "found",
                "detail": f"最终块: {fbm['block_id_final']} | 网络规模: {pf.get('network_tier_final')} | 密度: {pf.get('density')}"
            })

    # Stage 6: Classification — check H, E, F
    h = await fetch_one(f"""
        SELECT ip_long, block_id_final
        FROM {SCHEMA}.h_members WHERE run_id = :run_id AND ip_long = :ip_long
    """, {"run_id": run_id, "ip_long": ip_long})
    if h:
        result["classification"] = "H"
        result["stages"].append({
            "stage": "classification",
            "status": "H",
            "detail": f"🟠 H 类 (中型网络) | 最终块: {h.get('block_id_final')}"
        })
        return result

    e = await fetch_one(f"""
        SELECT ip_long, atom27_id, e_run_id
        FROM {SCHEMA}.e_members WHERE run_id = :run_id AND ip_long = :ip_long
    """, {"run_id": run_id, "ip_long": ip_long})
    if e:
        result["classification"] = "E"
        result["atom27_id"] = e.get("atom27_id")
        result["e_run_id"] = e.get("e_run_id")
        result["stages"].append({
            "stage": "classification",
            "status": "E",
            "detail": f"🟣 E 类 (密集原子) | atom27_id: {e.get('atom27_id')} | e_run: {e.get('e_run_id')}"
        })
        return result

    f = await fetch_one(f"""
        SELECT ip_long, atom27_id
        FROM {SCHEMA}.f_members WHERE run_id = :run_id AND ip_long = :ip_long
    """, {"run_id": run_id, "ip_long": ip_long})
    if f:
        result["classification"] = "F"
        result["atom27_id"] = f.get("atom27_id")
        result["stages"].append({
            "stage": "classification",
            "status": "F",
            "detail": f"🟢 F 类 (剩余散点) | atom27_id: {f.get('atom27_id')}"
        })
        return result

    # Check drop_members
    dm = await fetch_one(f"""
        SELECT ip_long, drop_reason
        FROM {SCHEMA}.drop_members WHERE run_id = :run_id AND ip_long = :ip_long
    """, {"run_id": run_id, "ip_long": ip_long})
    if dm:
        result["classification"] = "Drop"
        result["stages"].append({
            "stage": "classification",
            "status": "Drop",
            "detail": f"🔴 Drop | 原因: {dm.get('drop_reason')}"
        })
        return result

    result["stages"].append({
        "stage": "classification",
        "status": "unknown",
        "detail": "⚪ 未分类 — 可能 Pipeline 尚未完成"
    })
    return result


@router.get("/block/{block_id}")
async def block_detail(block_id: str, run_id: str = Query(None)):
    """查看块详情（自动判断 natural / final）"""
    if not run_id:
        row = await fetch_one(f"SELECT run_id FROM {SCHEMA}.shard_plan ORDER BY created_at DESC LIMIT 1")
        if not row:
            raise HTTPException(status_code=404, detail="No runs found")
        run_id = row["run_id"]

    # Try final block first
    bf = await fetch_one(f"""
        SELECT bf.block_id_final, bf.ip_start, bf.ip_end, bf.member_cnt_total,
               pf.valid_cnt, pf.density, pf.network_tier_final AS network_tier,
               pf.simple_score, pf.\"wA\" AS wa, pf.\"wD\" AS wd
        FROM {SCHEMA}.block_final bf
        LEFT JOIN {SCHEMA}.profile_final pf
            ON pf.run_id = bf.run_id AND pf.shard_id = bf.shard_id AND pf.block_id_final = bf.block_id_final
        WHERE bf.run_id = :run_id AND bf.block_id_final = :bid
    """, {"run_id": run_id, "bid": block_id})

    if bf:
        return {**bf, "block_type": "final"}

    # Try natural block
    bn = await fetch_one(f"""
        SELECT bn.block_id_natural AS block_id, bn.ip_start, bn.ip_end, bn.member_cnt_total,
               pp.valid_cnt, pp.density, pp.network_tier_pre AS network_tier,
               pp.simple_score, pp.\"wA\" AS wa, pp.\"wD\" AS wd,
               pp.keep_flag, pp.drop_reason
        FROM {SCHEMA}.block_natural bn
        LEFT JOIN {SCHEMA}.profile_pre pp
            ON pp.run_id = bn.run_id AND pp.shard_id = bn.shard_id AND pp.block_id_natural = bn.block_id_natural
        WHERE bn.run_id = :run_id AND bn.block_id_natural = :bid
    """, {"run_id": run_id, "bid": block_id})

    if bn:
        return {**bn, "block_type": "natural"}

    raise HTTPException(status_code=404, detail=f"Block {block_id} not found")


@router.get("/shard/{shard_id}/blocks")
async def shard_blocks(shard_id: int, run_id: str = Query(None),
                       page: int = Query(1, ge=1), page_size: int = Query(50, le=200)):
    """某 Shard 下的最终块列表（分页）"""
    if not run_id:
        row = await fetch_one(f"SELECT run_id FROM {SCHEMA}.shard_plan ORDER BY created_at DESC LIMIT 1")
        if not row:
            raise HTTPException(status_code=404, detail="No runs found")
        run_id = row["run_id"]

    offset = (page - 1) * page_size

    total = await fetch_one(f"""
        SELECT COUNT(*)::bigint AS total
        FROM {SCHEMA}.block_final
        WHERE run_id = :run_id AND shard_id = :shard_id
    """, {"run_id": run_id, "shard_id": shard_id})

    rows = await fetch_all(f"""
        SELECT bf.block_id_final, bf.ip_start, bf.ip_end, bf.member_cnt_total,
               pf.network_tier_final, pf.density, pf.valid_cnt
        FROM {SCHEMA}.block_final bf
        LEFT JOIN {SCHEMA}.profile_final pf
            ON pf.run_id = bf.run_id AND pf.shard_id = bf.shard_id AND pf.block_id_final = bf.block_id_final
        WHERE bf.run_id = :run_id AND bf.shard_id = :shard_id
        ORDER BY bf.ip_start
        LIMIT :limit OFFSET :offset
    """, {"run_id": run_id, "shard_id": shard_id, "limit": page_size, "offset": offset})

    return {
        "total": total["total"] if total else 0,
        "page": page,
        "page_size": page_size,
        "items": rows
    }
