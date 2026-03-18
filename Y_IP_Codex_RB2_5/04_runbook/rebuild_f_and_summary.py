"""
F库 重建脚本 + 单IP级摘要表构建 (rebuild_f_members + f_ip_summary)

步骤1: 重建 f_members = source_members \\ h_members \\ e_members \\ drop_members
步骤2: 构建 f_ip_summary — 从源表提取每个 F 库 IP 的完整属性

用法: python3 rebuild_f_and_summary.py
预计运行时间: 2-5 分钟
"""
import os
import time
import psycopg2
from multiprocessing import Pool
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

RUN_ID = os.environ.get("RUN_ID", "rb20v2_20260202_191900_sg_004")
CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))
SHARD_FILTER = os.environ.get("SHARD_FILTER", "").strip()
SOURCE_TABLE = 'public."ip库构建项目_ip源表_20250811_20250824_v2_1"'

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S', stream=sys.stdout)


def log(msg):
    logging.info(msg)
    sys.stdout.flush()


def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def parse_shard_filter():
    if not SHARD_FILTER:
        return None
    shards = []
    for part in SHARD_FILTER.split(","):
        part = part.strip()
        if not part:
            continue
        shards.append(int(part))
    return sorted(set(shards))


def get_shard_ids():
    """Load actual shard ids for the target run from r1_members (or fall back to f_members/source_members)."""
    filter_shards = parse_shard_filter()
    if filter_shards is not None:
        return filter_shards

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT shard_id FROM rb20_v2_5.r1_members WHERE run_id = %s ORDER BY shard_id",
        (RUN_ID,),
    )
    shard_ids = [row[0] for row in cur.fetchall()]
    if shard_ids:
        conn.close()
        return shard_ids

    # Try f_members first; fall back to source_members
    cur.execute(
        "SELECT DISTINCT shard_id FROM rb20_v2_5.f_members WHERE run_id = %s ORDER BY shard_id",
        (RUN_ID,),
    )
    shard_ids = [row[0] for row in cur.fetchall()]
    if not shard_ids:
        cur.execute(
            "SELECT DISTINCT shard_id FROM rb20_v2_5.source_members WHERE run_id = %s ORDER BY shard_id",
            (RUN_ID,),
        )
        shard_ids = [row[0] for row in cur.fetchall()]
    conn.close()
    return shard_ids


# ─────────────────────────────────────────────────
# Step 1: 重建 f_members
# ─────────────────────────────────────────────────
def rebuild_f_shard(shard_id):
    """重建单个 shard 的 f_members = r1_members \\ actual_e_coverage"""
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        sql = f"""
        BEGIN;
        DELETE FROM rb20_v2_5.f_members
        WHERE run_id = '{RUN_ID}' AND shard_id = {shard_id};

        INSERT INTO rb20_v2_5.f_members (run_id, contract_version, shard_id, ip_long, atom27_id)
        SELECT
            r1.run_id,
            r1.contract_version,
            r1.shard_id,
            r1.ip_long,
            r1.atom27_id
        FROM rb20_v2_5.r1_members r1
        LEFT JOIN (
            SELECT DISTINCT atom27_id
            FROM rb20_v2_5.e_members
            WHERE run_id = '{RUN_ID}' AND shard_id = {shard_id}
        ) ec
          ON ec.atom27_id = r1.atom27_id
        WHERE r1.run_id = '{RUN_ID}' AND r1.shard_id = {shard_id}
          AND ec.atom27_id IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM rb20_v2_5.h_members h
              WHERE h.run_id = r1.run_id AND h.ip_long = r1.ip_long
          );
        COMMIT;
        """
        cur.execute(sql)

        cur.execute(f"""
            SELECT COUNT(*) FROM rb20_v2_5.f_members
            WHERE run_id = '{RUN_ID}' AND shard_id = {shard_id}
        """)
        cnt = cur.fetchone()[0]
        log(f"  Shard {shard_id:02d}: f_members rebuilt -> {cnt:,} IPs")

        conn.close()
        return shard_id, cnt
    except Exception as e:
        log(f"  Shard {shard_id:02d}: ERROR - {e}")
        return shard_id, -1


# ─────────────────────────────────────────────────
# Step 2: 构建 f_ip_summary 表
# ─────────────────────────────────────────────────
def prep_summary_table():
    """创建 f_ip_summary 表"""
    log("Creating f_ip_summary table...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()

    ddl = """
    CREATE UNLOGGED TABLE IF NOT EXISTS rb20_v2_5.f_ip_summary (
      -- 主键
      run_id              varchar    NOT NULL,
      ip_long             bigint     NOT NULL,
      shard_id            smallint,

      -- 基础信息
      ip_address          varchar,
      top_operator        varchar,
      ip_stability        varchar,

      -- 绝对量: 上报
      total_reports           bigint,
      total_reports_pre_filter bigint,
      daa_reports             bigint,
      dna_reports             bigint,
      worktime_reports        bigint,
      workday_reports         bigint,
      weekend_reports         bigint,
      late_night_reports      bigint,

      -- 绝对量: 设备
      total_devices           bigint,
      total_devices_pre_filter bigint,
      wifi_devices            bigint,
      mobile_devices          bigint,
      vpn_devices             bigint,
      wired_devices           bigint,
      abnormal_net_devices    bigint,
      empty_net_devices       bigint,

      -- 绝对量: 其他
      total_apps              bigint,
      active_days             bigint,
      android_id_count        bigint,
      oaid_count              bigint,
      google_id_count         bigint,
      boot_id_count           bigint,
      model_count             bigint,
      manufacturer_count      bigint,
      ssid_count              bigint,
      bssid_count             bigint,

      -- 风险
      proxy_reports           bigint,
      root_reports            bigint,
      adb_reports             bigint,
      charging_reports        bigint,
      max_single_device_reports bigint,
      gateway_reports         bigint,
      ethernet_reports        bigint,

      -- 衍生比例 (与 E 库对齐)
      wifi_device_ratio       numeric(6,4),
      mobile_device_ratio     numeric(6,4),
      vpn_device_ratio        numeric(6,4),
      workday_report_ratio    numeric(6,4),
      weekend_report_ratio    numeric(6,4),
      late_night_report_ratio numeric(6,4),
      root_report_ratio       numeric(6,4),
      daa_dna_ratio           numeric(10,2),

      -- 元数据
      created_at              timestamptz DEFAULT NOW(),

      PRIMARY KEY(run_id, ip_long)
    );
    """
    cur.execute(ddl)
    filter_shards = parse_shard_filter()
    if filter_shards is None:
        cur.execute("DELETE FROM rb20_v2_5.f_ip_summary WHERE run_id = %s", (RUN_ID,))
    else:
        cur.execute(
            "DELETE FROM rb20_v2_5.f_ip_summary WHERE run_id = %s AND shard_id = ANY(%s)",
            (RUN_ID, filter_shards),
        )
    log(f"f_ip_summary table ready, cleared run_id={RUN_ID}.")
    conn.close()


def build_summary_shard(shard_id):
    """从源表提取单个 shard 的 F 库 IP 属性"""
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        sql = f"""
        BEGIN;
        SET LOCAL work_mem = '256MB';

        INSERT INTO rb20_v2_5.f_ip_summary (
            run_id, ip_long, shard_id,
            ip_address, top_operator, ip_stability,
            total_reports, total_reports_pre_filter, daa_reports, dna_reports,
            worktime_reports, workday_reports, weekend_reports, late_night_reports,
            total_devices, total_devices_pre_filter,
            wifi_devices, mobile_devices, vpn_devices, wired_devices,
            abnormal_net_devices, empty_net_devices,
            total_apps, active_days,
            android_id_count, oaid_count, google_id_count, boot_id_count,
            model_count, manufacturer_count, ssid_count, bssid_count,
            proxy_reports, root_reports, adb_reports, charging_reports,
            max_single_device_reports, gateway_reports, ethernet_reports,
            wifi_device_ratio, mobile_device_ratio, vpn_device_ratio,
            workday_report_ratio, weekend_report_ratio, late_night_report_ratio,
            root_report_ratio, daa_dna_ratio
        )
        SELECT
            '{RUN_ID}',
            fm.ip_long,
            {shard_id},
            src.ip_address,
            src."IP归属运营商",
            src."IP稳定性",
            -- 上报
            src."上报次数",
            src."过滤前上报次数",
            src."DAA业务上报次数",
            src."DNA业务上报次数",
            src."工作时上报次数",
            src."工作日上报次数",
            src."周末上报次数",
            src."深夜上报次数",
            -- 设备
            src."设备数量",
            src."过滤前设备数量",
            src."WiFi设备数量",
            src."移动网络设备数量",
            src."VPN设备数量",
            src."有线网络设备数量",
            src."异常网络设备数量",
            src."空网络状态设备数量",
            -- 其他
            src."应用数量",
            src."活跃天数",
            src."安卓ID数量",
            src."OAID数量",
            src."谷歌ID数量",
            src."启动ID数量",
            src."型号数量",
            src."制造商数量",
            src."SSID去重数",
            src."BSSID去重数",
            -- 风险
            src."代理上报次数",
            src."Root设备上报次数",
            src."ADB调试上报次数",
            src."充电状态上报次数",
            src."单设备最大上报次数",
            src."网关存在上报次数",
            src."以太网接口上报次数",
            -- 衍生比例
            CASE WHEN src."设备数量" > 0 THEN ROUND(src."WiFi设备数量"::numeric / src."设备数量", 4) ELSE 0 END,
            CASE WHEN src."设备数量" > 0 THEN ROUND(src."移动网络设备数量"::numeric / src."设备数量", 4) ELSE 0 END,
            CASE WHEN src."设备数量" > 0 THEN ROUND(src."VPN设备数量"::numeric / src."设备数量", 4) ELSE 0 END,
            CASE WHEN src."上报次数" > 0 THEN ROUND(src."工作日上报次数"::numeric / src."上报次数", 4) ELSE 0 END,
            CASE WHEN src."上报次数" > 0 THEN ROUND(src."周末上报次数"::numeric / src."上报次数", 4) ELSE 0 END,
            CASE WHEN src."上报次数" > 0 THEN ROUND(src."深夜上报次数"::numeric / src."上报次数", 4) ELSE 0 END,
            CASE WHEN src."上报次数" > 0 THEN ROUND(src."Root设备上报次数"::numeric / src."上报次数", 4) ELSE 0 END,
            CASE WHEN src."DNA业务上报次数" > 0 THEN ROUND(src."DAA业务上报次数"::numeric / src."DNA业务上报次数", 2) ELSE 0 END
        FROM rb20_v2_5.f_members fm
        JOIN {SOURCE_TABLE} src ON fm.ip_long = src.ip_long
        WHERE fm.run_id = '{RUN_ID}' AND fm.shard_id = {shard_id};

        COMMIT;
        """
        cur.execute(sql)

        cur.execute(f"""
            SELECT COUNT(*) FROM rb20_v2_5.f_ip_summary
            WHERE run_id = '{RUN_ID}' AND shard_id = {shard_id}
        """)
        cnt = cur.fetchone()[0]
        log(f"  Shard {shard_id:02d}: f_ip_summary built -> {cnt:,} IPs")

        conn.close()
        return shard_id, cnt
    except Exception as e:
        log(f"  Shard {shard_id:02d}: SUMMARY ERROR - {e}")
        return shard_id, -1


def build_indexes():
    """创建索引"""
    log("Building indexes...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_f_ip_summary_run ON rb20_v2_5.f_ip_summary(run_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_f_ip_summary_operator ON rb20_v2_5.f_ip_summary(run_id, top_operator)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_f_ip_summary_shard ON rb20_v2_5.f_ip_summary(run_id, shard_id)")
    log("Indexes created.")
    conn.close()


def main():
    t0 = time.time()

    # ── Step 1: 重建 f_members ──
    log("=" * 60)
    log("STEP 1: Rebuilding f_members (Source \\ H \\ E \\ Drop)")
    log("=" * 60)

    shards = get_shard_ids()
    if not shards:
        log("  No shards selected, nothing to do.")
        return
    log(f"  Shards: {len(shards)} ({shards[0]}-{shards[-1]})")
    with Pool(CONCURRENCY) as p:
        results = p.map(rebuild_f_shard, shards)

    total_f = sum(cnt for _, cnt in results if cnt >= 0)
    errors = [s for s, cnt in results if cnt < 0]
    log(f"Step 1 done: f_members total = {total_f:,} IPs | errors: {len(errors)}")

    if errors:
        log(f"  Error shards: {errors}")
        return

    # ── Step 2: 验证互斥性 ──
    log("\n" + "=" * 60)
    log("STEP 1.5: Verifying mutual exclusivity")
    log("=" * 60)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM rb20_v2_5.f_members f
             INNER JOIN rb20_v2_5.h_members h ON f.ip_long = h.ip_long AND f.run_id = h.run_id
             WHERE f.run_id = '{RUN_ID}') as f_h_overlap,
            (SELECT COUNT(*) FROM rb20_v2_5.f_members f
             INNER JOIN rb20_v2_5.e_members e ON f.ip_long = e.ip_long AND f.run_id = e.run_id
             WHERE f.run_id = '{RUN_ID}') as f_e_overlap,
            (SELECT COUNT(*) FROM rb20_v2_5.source_members WHERE run_id = '{RUN_ID}') as source_cnt,
            (SELECT COUNT(*) FROM rb20_v2_5.h_members WHERE run_id = '{RUN_ID}') as h_cnt,
            (SELECT COUNT(*) FROM rb20_v2_5.e_members WHERE run_id = '{RUN_ID}') as e_cnt,
            (SELECT COUNT(*) FROM rb20_v2_5.f_members WHERE run_id = '{RUN_ID}') as f_cnt,
            (SELECT COUNT(*) FROM rb20_v2_5.drop_members WHERE run_id = '{RUN_ID}') as d_cnt
    """)
    row = cur.fetchone()
    f_h, f_e, src, h, e, f, d = row
    log(f"  F∩H = {f_h} | F∩E = {f_e}")
    log(f"  Source = {src:,} | H = {h:,} | E = {e:,} | F = {f:,} | Drop = {d:,}")
    log(f"  H+E+F+D = {h+e+f+d:,} | diff from Source = {h+e+f+d - src:,}")
    conn.close()

    # ── Step 3: 构建 f_ip_summary ──
    log("\n" + "=" * 60)
    log("STEP 2: Building f_ip_summary")
    log("=" * 60)
    prep_summary_table()

    summary_shards = get_shard_ids()  # re-read after f_members rebuild
    with Pool(CONCURRENCY) as p:
        results2 = p.map(build_summary_shard, summary_shards)

    total_summary = sum(cnt for _, cnt in results2 if cnt >= 0)
    errors2 = [s for s, cnt in results2 if cnt < 0]
    log(f"Step 2 done: f_ip_summary total = {total_summary:,} | errors: {len(errors2)}")

    # ── Step 4: 索引 ──
    build_indexes()

    elapsed = time.time() - t0
    log(f"\nAll done in {elapsed:.1f}s")
    log(f"   f_members: {total_f:,} IPs")
    log(f"   f_ip_summary: {total_summary:,} IPs")


if __name__ == "__main__":
    main()
