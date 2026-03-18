"""
H∩E 交叉修复 + E/F 库重建脚本

问题: R1 中包含了 H 库成员（329万），导致 E 库中混入 H 库 IP。
修复:
  Step 1: 从 e_members 中删除属于 h_members 的 IP（按 shard 并发）
  Step 2: 重建 e_cidr_summary（调用 build_e_cidr_summary.py 的逻辑）
  Step 3: 重建 f_members = Source \ H \ E \ Drop
  Step 4: 重建 f_ip_summary
  Step 5: 完整性验证

用法: python3 fix_he_overlap.py
"""
import os
import time
import psycopg2
from multiprocessing import Pool
import logging
import sys

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
SHARD_CNT = 65
CONCURRENCY = 8
SOURCE_TABLE = 'public."ip库构建项目_ip源表_20250811_20250824_v2_1"'

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S', stream=sys.stdout)

def log(msg):
    logging.info(msg)
    sys.stdout.flush()

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


# ─────────────────────────────────────────────────
# Step 1: 从 e_members 中删除 H 库交叉 IP
# ─────────────────────────────────────────────────
def fix_e_members_shard(shard_id):
    """从 e_members 中删除属于 h_members 的 IP"""
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        sql = f"""
        DELETE FROM rb20_v2_5.e_members em
        WHERE em.run_id = '{RUN_ID}' AND em.shard_id = {shard_id}
          AND EXISTS (
              SELECT 1 FROM rb20_v2_5.h_members h
              WHERE h.run_id = em.run_id AND h.ip_long = em.ip_long
          );
        """
        cur.execute(sql)
        deleted = cur.rowcount
        conn.commit()

        log(f"  Shard {shard_id:02d}: deleted {deleted:,} H-overlap IPs from e_members")
        conn.close()
        return shard_id, deleted
    except Exception as e:
        log(f"  Shard {shard_id:02d}: ERROR - {e}")
        return shard_id, -1


# ─────────────────────────────────────────────────
# Step 2: 同样修复 r1_members
# ─────────────────────────────────────────────────
def fix_r1_members_shard(shard_id):
    """从 r1_members 中删除属于 h_members 的 IP"""
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        sql = f"""
        DELETE FROM rb20_v2_5.r1_members r1
        WHERE r1.run_id = '{RUN_ID}' AND r1.shard_id = {shard_id}
          AND EXISTS (
              SELECT 1 FROM rb20_v2_5.h_members h
              WHERE h.run_id = r1.run_id AND h.ip_long = r1.ip_long
          );
        """
        cur.execute(sql)
        deleted = cur.rowcount
        conn.commit()

        log(f"  Shard {shard_id:02d}: deleted {deleted:,} H-overlap IPs from r1_members")
        conn.close()
        return shard_id, deleted
    except Exception as e:
        log(f"  Shard {shard_id:02d}: ERROR - {e}")
        return shard_id, -1


# ─────────────────────────────────────────────────
# Step 3: 重建 f_members = Source \ H \ E \ Drop
# ─────────────────────────────────────────────────
def rebuild_f_shard(shard_id):
    """重建单个 shard 的 f_members"""
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        sql = f"""
        BEGIN;
        DELETE FROM rb20_v2_5.f_members
        WHERE run_id = '{RUN_ID}' AND shard_id = {shard_id};

        INSERT INTO rb20_v2_5.f_members (run_id, contract_version, shard_id, ip_long, atom27_id)
        SELECT
            sm.run_id, sm.contract_version, sm.shard_id, sm.ip_long,
            (sm.ip_long >> 5)::bigint AS atom27_id
        FROM rb20_v2_5.source_members sm
        WHERE sm.run_id = '{RUN_ID}' AND sm.shard_id = {shard_id}
          AND NOT EXISTS (SELECT 1 FROM rb20_v2_5.h_members h WHERE h.run_id = sm.run_id AND h.ip_long = sm.ip_long)
          AND NOT EXISTS (SELECT 1 FROM rb20_v2_5.e_members e WHERE e.run_id = sm.run_id AND e.ip_long = sm.ip_long)
          AND NOT EXISTS (SELECT 1 FROM rb20_v2_5.drop_members d WHERE d.run_id = sm.run_id AND d.ip_long = sm.ip_long);
        COMMIT;
        """
        cur.execute(sql)

        cur.execute(f"SELECT COUNT(*) FROM rb20_v2_5.f_members WHERE run_id = '{RUN_ID}' AND shard_id = {shard_id}")
        cnt = cur.fetchone()[0]
        log(f"  Shard {shard_id:02d}: f_members rebuilt → {cnt:,} IPs")
        conn.close()
        return shard_id, cnt
    except Exception as e:
        log(f"  Shard {shard_id:02d}: F ERROR - {e}")
        return shard_id, -1


# ─────────────────────────────────────────────────
# Step 4: 重建 f_ip_summary
# ─────────────────────────────────────────────────
def prep_f_summary():
    log("Creating f_ip_summary table...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS rb20_v2_5.f_ip_summary")
    ddl = """
    CREATE UNLOGGED TABLE rb20_v2_5.f_ip_summary (
      run_id varchar NOT NULL, ip_long bigint NOT NULL, shard_id smallint,
      ip_address varchar, top_operator varchar, ip_stability varchar,
      total_reports bigint, total_reports_pre_filter bigint,
      daa_reports bigint, dna_reports bigint,
      worktime_reports bigint, workday_reports bigint, weekend_reports bigint, late_night_reports bigint,
      total_devices bigint, total_devices_pre_filter bigint,
      wifi_devices bigint, mobile_devices bigint, vpn_devices bigint, wired_devices bigint,
      abnormal_net_devices bigint, empty_net_devices bigint,
      total_apps bigint, active_days bigint,
      android_id_count bigint, oaid_count bigint, google_id_count bigint, boot_id_count bigint,
      model_count bigint, manufacturer_count bigint, ssid_count bigint, bssid_count bigint,
      proxy_reports bigint, root_reports bigint, adb_reports bigint, charging_reports bigint,
      max_single_device_reports bigint, gateway_reports bigint, ethernet_reports bigint,
      wifi_device_ratio numeric(6,4), mobile_device_ratio numeric(6,4), vpn_device_ratio numeric(6,4),
      workday_report_ratio numeric(6,4), weekend_report_ratio numeric(6,4),
      late_night_report_ratio numeric(6,4), root_report_ratio numeric(6,4),
      daa_dna_ratio numeric(10,2),
      created_at timestamptz DEFAULT NOW(),
      PRIMARY KEY(run_id, ip_long)
    );
    """
    cur.execute(ddl)
    log("f_ip_summary table created.")
    conn.close()

def build_f_summary_shard(shard_id):
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        sql = f"""
        BEGIN; SET LOCAL work_mem = '256MB';
        INSERT INTO rb20_v2_5.f_ip_summary (
            run_id, ip_long, shard_id, ip_address, top_operator, ip_stability,
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
        SELECT '{RUN_ID}', fm.ip_long, {shard_id},
            src.ip_address, src."IP归属运营商", src."IP稳定性",
            src."上报次数", src."过滤前上报次数", src."DAA业务上报次数", src."DNA业务上报次数",
            src."工作时上报次数", src."工作日上报次数", src."周末上报次数", src."深夜上报次数",
            src."设备数量", src."过滤前设备数量",
            src."WiFi设备数量", src."移动网络设备数量", src."VPN设备数量", src."有线网络设备数量",
            src."异常网络设备数量", src."空网络状态设备数量",
            src."应用数量", src."活跃天数",
            src."安卓ID数量", src."OAID数量", src."谷歌ID数量", src."启动ID数量",
            src."型号数量", src."制造商数量", src."SSID去重数", src."BSSID去重数",
            src."代理上报次数", src."Root设备上报次数", src."ADB调试上报次数", src."充电状态上报次数",
            src."单设备最大上报次数", src."网关存在上报次数", src."以太网接口上报次数",
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
        cur.execute(f"SELECT COUNT(*) FROM rb20_v2_5.f_ip_summary WHERE run_id = '{RUN_ID}' AND shard_id = {shard_id}")
        cnt = cur.fetchone()[0]
        log(f"  Shard {shard_id:02d}: f_ip_summary → {cnt:,}")
        conn.close()
        return shard_id, cnt
    except Exception as e:
        log(f"  Shard {shard_id:02d}: SUMMARY ERROR - {e}")
        return shard_id, -1


def main():
    t0 = time.time()
    shards = list(range(SHARD_CNT))

    # ── Step 1: 修复 e_members ──
    log("=" * 60)
    log("STEP 1: Removing H-overlap IPs from e_members")
    log("=" * 60)
    with Pool(CONCURRENCY) as p:
        results = p.map(fix_e_members_shard, shards)
    total_deleted_e = sum(d for _, d in results if d >= 0)
    log(f"Step 1 done: removed {total_deleted_e:,} IPs from e_members")

    # ── Step 2: 修复 r1_members ──
    log("\n" + "=" * 60)
    log("STEP 2: Removing H-overlap IPs from r1_members")
    log("=" * 60)
    with Pool(CONCURRENCY) as p:
        results2 = p.map(fix_r1_members_shard, shards)
    total_deleted_r1 = sum(d for _, d in results2 if d >= 0)
    log(f"Step 2 done: removed {total_deleted_r1:,} IPs from r1_members")

    # ── Step 3: 重建 f_members ──
    log("\n" + "=" * 60)
    log("STEP 3: Rebuilding f_members (Source \\ H \\ E \\ Drop)")
    log("=" * 60)
    with Pool(CONCURRENCY) as p:
        results3 = p.map(rebuild_f_shard, shards)
    total_f = sum(cnt for _, cnt in results3 if cnt >= 0)
    log(f"Step 3 done: f_members = {total_f:,}")

    # ── Step 4: 重建 f_ip_summary ──
    log("\n" + "=" * 60)
    log("STEP 4: Rebuilding f_ip_summary")
    log("=" * 60)
    prep_f_summary()
    with Pool(CONCURRENCY) as p:
        results4 = p.map(build_f_summary_shard, shards)
    total_fs = sum(cnt for _, cnt in results4 if cnt >= 0)
    log(f"Step 4 done: f_ip_summary = {total_fs:,}")

    # ── Step 5: 完整性验证 ──
    log("\n" + "=" * 60)
    log("STEP 5: Integrity verification")
    log("=" * 60)
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM rb20_v2_5.source_members WHERE run_id = '{RUN_ID}') as src,
            (SELECT COUNT(*) FROM rb20_v2_5.h_members WHERE run_id = '{RUN_ID}') as h,
            (SELECT COUNT(*) FROM rb20_v2_5.e_members WHERE run_id = '{RUN_ID}') as e,
            (SELECT COUNT(*) FROM rb20_v2_5.f_members WHERE run_id = '{RUN_ID}') as f,
            (SELECT COUNT(*) FROM rb20_v2_5.drop_members WHERE run_id = '{RUN_ID}') as d
    """)
    src, h, e, f, d = cur.fetchone()
    log(f"  Source    = {src:>12,}")
    log(f"  H         = {h:>12,}")
    log(f"  E         = {e:>12,}")
    log(f"  F         = {f:>12,}")
    log(f"  Drop      = {d:>12,}")
    log(f"  H+E+F+D   = {h+e+f+d:>12,}")
    log(f"  Diff      = {h+e+f+d - src:>12,}")

    # 交叉验证（只查少量看趋势）
    cur.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM rb20_v2_5.e_members em
             WHERE em.run_id = '{RUN_ID}'
             AND EXISTS (SELECT 1 FROM rb20_v2_5.h_members h WHERE h.ip_long = em.ip_long AND h.run_id = em.run_id)
            ) as he_overlap,
            (SELECT COUNT(*) FROM rb20_v2_5.f_members fm
             WHERE fm.run_id = '{RUN_ID}'
             AND EXISTS (SELECT 1 FROM rb20_v2_5.h_members h WHERE h.ip_long = fm.ip_long AND h.run_id = fm.run_id)
            ) as fh_overlap,
            (SELECT COUNT(*) FROM rb20_v2_5.f_members fm
             WHERE fm.run_id = '{RUN_ID}'
             AND EXISTS (SELECT 1 FROM rb20_v2_5.e_members e WHERE e.ip_long = fm.ip_long AND e.run_id = fm.run_id)
            ) as fe_overlap
    """)
    he, fh, fe = cur.fetchone()
    log(f"  H∩E = {he:,} | F∩H = {fh:,} | F∩E = {fe:,}")
    conn.close()

    elapsed = time.time() - t0
    log(f"\n✅ All done in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
