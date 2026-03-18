"""
E库 超大段拆分脚本 (split_oversized_e_runs.py)

将 ip_count > 16384 (1/4 B类) 的 E 库 CIDR 段进行拆分：
  规则1: 在 B 类边界 (x.y.0.0, 即 ip_long 的第2字节变更处) 强制拆分
  规则2: 拆分后每段不超过 16384 IP (1/4 B类)

操作步骤:
  1. 找出所有需要拆分的超大段
  2. 对每段的成员 IP 按 B 类边界分组
  3. 对每个 B 类组，如果 IP 数仍超上限，按 16384 硬切
  4. 为每个子段生成新的 e_run_id，更新 e_members
  5. 删除旧的 e_runs 条目，插入新的子段 e_runs
  6. 重建 e_cidr_summary

用法: python3 split_oversized_e_runs.py
"""
import os
import time
import psycopg2
import logging
import sys

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

RUN_ID = "rb20v2_20260202_191900_sg_001"
SCHEMA = "rb20_v2_5"
MAX_IP_PER_SEGMENT = 16384   # 1/4 B 类

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s',
                    datefmt='%H:%M:%S', stream=sys.stdout)

def log(msg):
    logging.info(msg)
    sys.stdout.flush()

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

def ip_to_b_class(ip_long):
    """返回 B 类标识 (前16位)"""
    return ip_long >> 16

def ip_to_text(ip_long):
    """ip_long -> dotted text"""
    return f"{(ip_long>>24)&0xFF}.{(ip_long>>16)&0xFF}.{(ip_long>>8)&0xFF}.{ip_long&0xFF}"

def b_class_boundary(b_class_id):
    """B 类起始 IP (x.y.0.0)"""
    return b_class_id << 16


def find_oversized_runs(conn):
    """查找需要拆分的超大段"""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT e.e_run_id, e.shard_id, e.atom27_start, e.atom27_end,
               e.ip_start, e.ip_end, e.run_len, e.short_run,
               es.ip_count
        FROM {SCHEMA}.e_runs e
        JOIN {SCHEMA}.e_cidr_summary es 
          ON e.run_id = es.run_id AND e.e_run_id = es.e_run_id
        WHERE e.run_id = '{RUN_ID}'
          AND es.ip_count > {MAX_IP_PER_SEGMENT}
        ORDER BY es.ip_count DESC
    """)
    rows = cur.fetchall()
    log(f"Found {len(rows)} oversized runs to split.")
    return rows


def split_single_run(conn, e_run_id, shard_id, atom27_start, atom27_end,
                     ip_start, ip_end, run_len, short_run, ip_count):
    """拆分单个超大段"""
    cur = conn.cursor()
    
    # 1. 获取所有成员 IP，按 ip_long 排序
    cur.execute(f"""
        SELECT ip_long, atom27_id 
        FROM {SCHEMA}.e_members
        WHERE run_id = '{RUN_ID}' AND e_run_id = '{e_run_id}'
        ORDER BY ip_long
    """)
    members = cur.fetchall()
    actual_count = len(members)
    
    if actual_count <= MAX_IP_PER_SEGMENT:
        log(f"  {e_run_id}: actual member count {actual_count} <= {MAX_IP_PER_SEGMENT}, skip.")
        return 0
    
    # 2. 按 B 类分组
    b_class_groups = {}
    for ip_long, atom27_id in members:
        b_class = ip_to_b_class(ip_long)
        if b_class not in b_class_groups:
            b_class_groups[b_class] = []
        b_class_groups[b_class].append((ip_long, atom27_id))
    
    # 3. 对每个 B 类组，如超过上限则按 MAX_IP_PER_SEGMENT 硬切
    sub_segments = []
    for b_class in sorted(b_class_groups.keys()):
        group_ips = b_class_groups[b_class]
        # 如果该 B 类组的 IP 数仍超上限，按固定大小切
        for chunk_start in range(0, len(group_ips), MAX_IP_PER_SEGMENT):
            chunk = group_ips[chunk_start:chunk_start + MAX_IP_PER_SEGMENT]
            sub_segments.append(chunk)
    
    if len(sub_segments) <= 1:
        log(f"  {e_run_id}: only 1 sub-segment after split, skip.")
        return 0
    
    # 4. 为每个子段生成新的 e_run_id 和记录
    new_run_ids = []
    for idx, chunk in enumerate(sub_segments):
        chunk_ips = [ip for ip, _ in chunk]
        chunk_atoms = [a for _, a in chunk]
        
        min_ip = min(chunk_ips)
        max_ip = max(chunk_ips)
        min_atom = min(chunk_atoms)
        max_atom = max(chunk_atoms)
        chunk_run_len = max_atom - min_atom + 1
        
        # 新 e_run_id 格式: E{shard}_{min_atom}_{max_atom}
        new_id = f"E{shard_id}_{min_atom}_{max_atom}"
        
        # 如果同一个 shard 内 atom 范围重叠（可能因 B 类切割），加后缀
        if new_id in [x[0] for x in new_run_ids] or new_id == e_run_id:
            new_id = f"E{shard_id}_{min_atom}_{max_atom}_s{idx}"
        
        new_run_ids.append((
            new_id,
            min_atom, max_atom,
            chunk_run_len,
            min_ip, max_ip,
            len(chunk),
            chunk_ips
        ))
    
    # 5. 更新 e_members: 批量 UPDATE
    for new_id, min_atom, max_atom, chunk_run_len, min_ip, max_ip, cnt, chunk_ips in new_run_ids:
        # 因为在切割时是按 ip_long 排序的，所以 chunk 里的 ip_long 是严格连续或单调递增的
        # 直接使用 BETWEEN 极大提升更新速度
        cur.execute(f"""
            UPDATE {SCHEMA}.e_members 
            SET e_run_id = '{new_id}'
            WHERE run_id = '{RUN_ID}' 
              AND e_run_id = '{e_run_id}'
              AND ip_long BETWEEN {min_ip} AND {max_ip}
        """)
    
    # 6. 删除旧的 e_runs 记录
    cur.execute(f"""
        DELETE FROM {SCHEMA}.e_runs 
        WHERE run_id = '{RUN_ID}' AND e_run_id = '{e_run_id}'
    """)
    
    # 7. 插入新的 e_runs 记录
    for new_id, min_atom, max_atom, chunk_run_len, min_ip, max_ip, cnt, _ in new_run_ids:
        is_short = chunk_run_len < 4  # 与原始逻辑一致
        cur.execute(f"""
            INSERT INTO {SCHEMA}.e_runs 
            (run_id, contract_version, shard_id, e_run_id, atom27_start, atom27_end, 
             run_len, short_run, ip_start, ip_end)
            VALUES ('{RUN_ID}', 'contract_v1', {shard_id}, '{new_id}',
                    {min_atom}, {max_atom}, {chunk_run_len}, {is_short},
                    {min_ip}, {max_ip})
        """)
    
    conn.commit()
    
    sub_info = ', '.join([f"{ip_to_text(mi)}({cnt})" 
                          for _, _, _, _, mi, _, cnt, _ in new_run_ids])
    log(f"  {e_run_id}: {ip_to_text(ip_start)} ({actual_count} IP) -> "
        f"{len(new_run_ids)} sub-segments: {sub_info}")
    
    return len(new_run_ids)


def delete_old_summary(conn):
    """删除旧的 e_cidr_summary（将被重建）"""
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {SCHEMA}.e_cidr_summary WHERE run_id = '{RUN_ID}'")
    cnt = cur.rowcount
    conn.commit()
    log(f"Deleted {cnt} rows from e_cidr_summary.")


def main():
    start = time.time()
    log(f"=== E Library Oversized Run Splitting ===")
    log(f"    Run: {RUN_ID}")
    log(f"    Max IP/segment: {MAX_IP_PER_SEGMENT}")
    
    conn = get_db_conn()
    
    # 找超大段
    oversized = find_oversized_runs(conn)
    if not oversized:
        log("No oversized runs found. Done.")
        return
    
    total_original_ips = sum(row[8] for row in oversized)
    log(f"Total oversized: {len(oversized)} runs, {total_original_ips:,} IPs")
    
    # 逐一拆分
    total_new = 0
    for row in oversized:
        e_run_id, shard_id, atom27_start, atom27_end, ip_start, ip_end, \
            run_len, short_run, ip_count = row
        new_cnt = split_single_run(
            conn, e_run_id, shard_id, atom27_start, atom27_end,
            ip_start, ip_end, run_len, short_run, ip_count
        )
        total_new += new_cnt
    
    elapsed = time.time() - start
    log(f"=== Split complete: {len(oversized)} runs -> {total_new} sub-segments ({elapsed:.1f}s) ===")
    log(f"Now run build_e_cidr_summary.py to rebuild the summary table.")
    conn.close()


if __name__ == "__main__":
    main()
