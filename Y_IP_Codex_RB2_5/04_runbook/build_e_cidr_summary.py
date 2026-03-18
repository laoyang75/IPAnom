"""
E库 CIDR 块级属性摘要表构建脚本 (e_cidr_summary)

从原始 IP 源表 public."ip库构建项目_ip源表_20250811_20250824_v2_1" 聚合到 e_run 级。
替代旧版 e_runs_summary（仅10字段），新版包含 ~60 个属性维度。

用法: python3 build_e_cidr_summary.py
预计运行时间: 15-25 分钟（64 shard 并发 x 16 workers）
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
CONCURRENCY = int(os.environ.get("CONCURRENCY", "4"))
SHARD_FILTER = os.environ.get("SHARD_FILTER", "").strip()
SOURCE_TABLE = 'public."ip库构建项目_ip源表_20250811_20250824_v2_1"'

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S', stream=sys.stdout)


def log(msg):
    logging.info(msg)
    sys.stdout.flush()


def get_db_conn():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        log(f"DB Connection Error: {e}")
        raise


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
    """Load actual shard ids for the target run instead of assuming a fixed range."""
    filter_shards = parse_shard_filter()
    if filter_shards is not None:
        return filter_shards

    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT shard_id FROM rb20_v2_5.e_members WHERE run_id = %s ORDER BY shard_id",
        (RUN_ID,),
    )
    shard_ids = [row[0] for row in cur.fetchall()]
    conn.close()
    return shard_ids


def prep_table():
    """Ensure table exists and clear only the target run_id."""
    log(f"Prepping e_cidr_summary table for run_id={RUN_ID}...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()

    ddl = """
    CREATE UNLOGGED TABLE IF NOT EXISTS rb20_v2_5.e_cidr_summary (
      -- A. 主键与结构信息
      e_run_id          varchar    NOT NULL,
      run_id            varchar    NOT NULL,
      shard_id          smallint   NOT NULL,
      atom27_start      bigint,
      atom27_end        bigint,
      run_len           int,
      short_run         boolean,
      ip_range_start    bigint,
      ip_range_end      bigint,

      -- B. 规模与质量指标
      ip_count          int,
      ip_density        numeric(6,4),
      abnormal_ip_count int        DEFAULT 0,
      abnormal_ip_ratio numeric(6,4) DEFAULT 0,
      unstable_ip_count int        DEFAULT 0,
      unstable_ip_ratio numeric(6,4) DEFAULT 0,

      -- C. 上报行为（绝对量）
      total_reports           bigint,
      total_reports_pre_filter bigint,
      daa_reports             bigint,
      dna_reports             bigint,
      worktime_reports        bigint,
      workday_reports         bigint,
      weekend_reports         bigint,
      late_night_reports      bigint,

      -- D. 设备行为（绝对量）
      total_devices           bigint,
      total_devices_pre_filter bigint,
      wifi_devices            bigint,
      mobile_devices          bigint,
      vpn_devices             bigint,
      wired_devices           bigint,
      abnormal_net_devices    bigint,
      empty_net_devices       bigint,
      total_apps              bigint,

      -- E. ID 标识维度
      android_id_count  bigint,
      oaid_count        bigint,
      google_id_count   bigint,
      boot_id_count     bigint,
      model_count       bigint,
      manufacturer_count bigint,

      -- F. WiFi & 网络特征
      ssid_count              bigint,
      bssid_count             bigint,
      gateway_reports         bigint,
      ethernet_reports        bigint,
      wifi_comparable_reports bigint,

      -- G. 风险与异常特征
      proxy_reports           bigint,
      root_reports            bigint,
      adb_reports             bigint,
      charging_reports        bigint,
      max_single_device_reports bigint,

      -- H. 衍生画像指标
      avg_reports_per_ip      numeric(10,2),
      avg_devices_per_ip      numeric(10,2),
      avg_active_days         numeric(6,2),
      avg_apps_per_ip         numeric(10,2),
      avg_apps_per_device     numeric(10,2),
      avg_manufacturer_per_ip numeric(10,2),
      avg_model_per_ip        numeric(10,2),
      wifi_device_ratio       numeric(6,4),
      mobile_device_ratio     numeric(6,4),
      vpn_device_ratio        numeric(6,4),
      android_device_ratio    numeric(6,4),
      oaid_device_ratio       numeric(6,4),
      android_oaid_ratio      numeric(6,4),
      report_oaid_ratio       numeric(6,4),
      workday_report_ratio    numeric(6,4),
      weekend_report_ratio    numeric(6,4),
      late_night_report_ratio numeric(6,4),
      root_report_ratio       numeric(6,4),
      daa_dna_ratio           numeric(10,2),
      top_operator            varchar,
      distinct_operators      int,
      start_ip_text           varchar,
      abnormal_rule_hits_total bigint DEFAULT 0,

      -- I. 元数据
      created_at              timestamptz DEFAULT NOW(),

      PRIMARY KEY(run_id, shard_id, e_run_id)
    );
    """
    cur.execute(ddl)
    filter_shards = parse_shard_filter()
    if filter_shards is None:
        cur.execute("DELETE FROM rb20_v2_5.e_cidr_summary WHERE run_id = %s", (RUN_ID,))
    else:
        cur.execute(
            "DELETE FROM rb20_v2_5.e_cidr_summary WHERE run_id = %s AND shard_id = ANY(%s)",
            (RUN_ID, filter_shards),
        )
    log("e_cidr_summary table ready and target run cleared.")
    conn.close()


def process_shard(shard_id):
    """处理单个 shard 的聚合"""
    try:
        conn = get_db_conn()
        cur = conn.cursor()

        log(f"Shard {shard_id:02d}: Starting aggregation from original source table...")

        sql = f"""
        BEGIN;
        SET LOCAL enable_nestloop = off;
        SET LOCAL work_mem = '512MB';
        SET LOCAL statement_timeout = '15min';

        WITH shard_ips AS (
            -- 获取该 shard 的 E 库成员及其 e_run_id
            SELECT em.ip_long, em.e_run_id
            FROM rb20_v2_5.e_members em
            WHERE em.run_id = '{RUN_ID}' AND em.shard_id = {shard_id}
        ),
        member_flags AS (
            SELECT
                si.e_run_id,
                si.ip_long,
                COALESCE(sm.is_abnormal, false) AS is_abnormal
            FROM shard_ips si
            LEFT JOIN rb20_v2_5.source_members sm
              ON sm.run_id = '{RUN_ID}'
             AND sm.shard_id = {shard_id}
             AND sm.ip_long = si.ip_long
        ),
        abnormal_stats AS (
            SELECT
                e_run_id,
                COUNT(*) FILTER (WHERE is_abnormal) AS abnormal_ip_count,
                COUNT(*) AS total_ip_count
            FROM member_flags
            GROUP BY e_run_id
        ),
        joined AS (
            -- JOIN 原始 IP 源表获取完整属性
            SELECT
                mf.e_run_id,
                mf.ip_long,
                src."上报次数"                   AS reports,
                src."过滤前上报次数"              AS reports_pre,
                src."设备数量"                   AS devices,
                src."过滤前设备数量"              AS devices_pre,
                src."应用数量"                   AS apps,
                src."活跃天数"                   AS active_days,
                src."安卓ID数量"                 AS android_ids,
                src."OAID数量"                  AS oaids,
                src."谷歌ID数量"                 AS google_ids,
                src."启动ID数量"                 AS boot_ids,
                src."型号数量"                   AS models,
                src."制造商数量"                  AS manufacturers,
                src."深夜上报次数"                AS late_night,
                src."工作时上报次数"              AS worktime,
                src."工作日上报次数"              AS workday,
                src."周末上报次数"                AS weekend,
                src."以太网接口上报次数"           AS ethernet,
                src."代理上报次数"                AS proxy,
                src."Root设备上报次数"            AS root_rpt,
                src."ADB调试上报次数"             AS adb_rpt,
                src."充电状态上报次数"             AS charging,
                src."单设备最大上报次数"           AS max_dev_rpt,
                src."DAA业务上报次数"             AS daa,
                src."DNA业务上报次数"             AS dna,
                src."WiFi可比上报次数"            AS wifi_comp,
                src."SSID去重数"                AS ssids,
                src."BSSID去重数"               AS bssids,
                src."网关存在上报次数"             AS gateway,
                src."WiFi设备数量"               AS wifi_dev,
                src."移动网络设备数量"             AS mobile_dev,
                src."VPN设备数量"                AS vpn_dev,
                src."有线网络设备数量"             AS wired_dev,
                src."空网络状态设备数量"           AS empty_dev,
                src."异常网络设备数量"             AS abnormal_dev,
                src."IP归属运营商"               AS operator,
                src."IP稳定性"                  AS ip_stability
            FROM member_flags mf
            JOIN {SOURCE_TABLE} src ON mf.ip_long = src.ip_long
            WHERE mf.is_abnormal = false
        ),
        -- 计算主要运营商（MODE）
        op_counts AS (
            SELECT e_run_id, operator, COUNT(*) as cnt,
                   ROW_NUMBER() OVER(PARTITION BY e_run_id ORDER BY COUNT(*) DESC) as rn
            FROM joined
            WHERE operator IS NOT NULL AND operator != ''
            GROUP BY e_run_id, operator
        ),
        top_ops AS (
            SELECT e_run_id, operator as top_operator
            FROM op_counts WHERE rn = 1
        ),
        -- 运营商多样性
        op_distinct AS (
            SELECT e_run_id, COUNT(DISTINCT operator) as distinct_ops
            FROM joined
            WHERE operator IS NOT NULL AND operator != ''
            GROUP BY e_run_id
        ),
        -- 主聚合
        agg AS (
            SELECT
                j.e_run_id,
                COUNT(j.ip_long)                AS ip_count,
                SUM(j.reports)                  AS total_reports,
                SUM(j.reports_pre)              AS total_reports_pre_filter,
                SUM(j.daa)                      AS daa_reports,
                SUM(j.dna)                      AS dna_reports,
                SUM(j.worktime)                 AS worktime_reports,
                SUM(j.workday)                  AS workday_reports,
                SUM(j.weekend)                  AS weekend_reports,
                SUM(j.late_night)               AS late_night_reports,
                SUM(j.devices)                  AS total_devices,
                SUM(j.devices_pre)              AS total_devices_pre_filter,
                SUM(j.wifi_dev)                 AS wifi_devices,
                SUM(j.mobile_dev)               AS mobile_devices,
                SUM(j.vpn_dev)                  AS vpn_devices,
                SUM(j.wired_dev)                AS wired_devices,
                SUM(j.abnormal_dev)             AS abnormal_net_devices,
                SUM(j.empty_dev)                AS empty_net_devices,
                SUM(j.apps)                     AS total_apps,
                SUM(j.android_ids)              AS android_id_count,
                SUM(j.oaids)                    AS oaid_count,
                SUM(j.google_ids)               AS google_id_count,
                SUM(j.boot_ids)                 AS boot_id_count,
                SUM(j.models)                   AS model_count,
                SUM(j.manufacturers)            AS manufacturer_count,
                SUM(j.ssids)                    AS ssid_count,
                SUM(j.bssids)                   AS bssid_count,
                SUM(j.gateway)                  AS gateway_reports,
                SUM(j.ethernet)                 AS ethernet_reports,
                SUM(j.wifi_comp)                AS wifi_comparable_reports,
                SUM(j.proxy)                    AS proxy_reports,
                SUM(j.root_rpt)                 AS root_reports,
                SUM(j.adb_rpt)                  AS adb_reports,
                SUM(j.charging)                 AS charging_reports,
                MAX(j.max_dev_rpt)              AS max_single_device_reports,
                ROUND(AVG(j.active_days), 2)    AS avg_active_days,
                SUM(CASE WHEN j.ip_stability = '不稳定网络' THEN 1 ELSE 0 END) AS unstable_ip_count
            FROM joined j
            GROUP BY j.e_run_id
        )
        INSERT INTO rb20_v2_5.e_cidr_summary (
            e_run_id, run_id, shard_id,
            atom27_start, atom27_end, run_len, short_run, ip_range_start, ip_range_end,
            ip_count, ip_density,
            abnormal_ip_count, abnormal_ip_ratio,
            unstable_ip_count, unstable_ip_ratio,
            total_reports, total_reports_pre_filter, daa_reports, dna_reports,
            worktime_reports, workday_reports, weekend_reports, late_night_reports,
            total_devices, total_devices_pre_filter,
            wifi_devices, mobile_devices, vpn_devices, wired_devices,
            abnormal_net_devices, empty_net_devices, total_apps,
            android_id_count, oaid_count, google_id_count, boot_id_count,
            model_count, manufacturer_count,
            ssid_count, bssid_count, gateway_reports, ethernet_reports, wifi_comparable_reports,
            proxy_reports, root_reports, adb_reports, charging_reports, max_single_device_reports,
            avg_reports_per_ip, avg_devices_per_ip, avg_active_days,
            avg_apps_per_ip, avg_apps_per_device,
            avg_manufacturer_per_ip, avg_model_per_ip,
            wifi_device_ratio, mobile_device_ratio, vpn_device_ratio,
            android_device_ratio, oaid_device_ratio, android_oaid_ratio, report_oaid_ratio,
            workday_report_ratio, weekend_report_ratio, late_night_report_ratio, root_report_ratio,
            daa_dna_ratio, top_operator, distinct_operators,
            start_ip_text, abnormal_rule_hits_total
        )
        SELECT
            a.e_run_id,
            '{RUN_ID}',
            {shard_id},
            -- 结构信息来自 e_runs
            er.atom27_start, er.atom27_end, er.run_len, er.short_run, er.ip_start, er.ip_end,
            -- 规模
            a.ip_count,
            CASE WHEN er.run_len > 0 THEN ROUND(a.ip_count::numeric / (er.run_len * 32), 4) ELSE 0 END,
            -- 异常（来自 source_members）
            COALESCE(ab.abnormal_ip_count, 0),
            CASE WHEN COALESCE(ab.total_ip_count, 0) > 0
                 THEN ROUND(ab.abnormal_ip_count::numeric / ab.total_ip_count, 4)
                 ELSE 0 END,
            -- 不稳定
            a.unstable_ip_count,
            CASE WHEN a.ip_count > 0 THEN ROUND(a.unstable_ip_count::numeric / a.ip_count, 4) ELSE 0 END,
            -- 上报
            a.total_reports, a.total_reports_pre_filter, a.daa_reports, a.dna_reports,
            a.worktime_reports, a.workday_reports, a.weekend_reports, a.late_night_reports,
            -- 设备
            a.total_devices, a.total_devices_pre_filter,
            a.wifi_devices, a.mobile_devices, a.vpn_devices, a.wired_devices,
            a.abnormal_net_devices, a.empty_net_devices, a.total_apps,
            -- ID
            a.android_id_count, a.oaid_count, a.google_id_count, a.boot_id_count,
            a.model_count, a.manufacturer_count,
            -- WiFi & 网络
            a.ssid_count, a.bssid_count, a.gateway_reports, a.ethernet_reports, a.wifi_comparable_reports,
            -- 风险
            a.proxy_reports, a.root_reports, a.adb_reports, a.charging_reports, a.max_single_device_reports,
            -- 衍生: 均值
            CASE WHEN a.ip_count > 0 THEN ROUND(a.total_reports::numeric / a.ip_count, 2) ELSE 0 END,
            CASE WHEN a.ip_count > 0 THEN ROUND(a.total_devices::numeric / a.ip_count, 2) ELSE 0 END,
            a.avg_active_days,
            CASE WHEN a.ip_count > 0 THEN ROUND(a.total_apps::numeric / a.ip_count, 2) ELSE 0 END,
            CASE WHEN a.total_devices > 0 THEN ROUND(a.total_apps::numeric / a.total_devices, 2) ELSE 0 END,
            CASE WHEN a.ip_count > 0 THEN ROUND(a.manufacturer_count::numeric / a.ip_count, 2) ELSE 0 END,
            CASE WHEN a.ip_count > 0 THEN ROUND(a.model_count::numeric / a.ip_count, 2) ELSE 0 END,
            -- 衍生: 设备占比
            CASE WHEN a.total_devices > 0 THEN ROUND(a.wifi_devices::numeric / a.total_devices, 4) ELSE 0 END,
            CASE WHEN a.total_devices > 0 THEN ROUND(a.mobile_devices::numeric / a.total_devices, 4) ELSE 0 END,
            CASE WHEN a.total_devices > 0 THEN ROUND(a.vpn_devices::numeric / a.total_devices, 4) ELSE 0 END,
            CASE WHEN a.total_devices > 0 THEN ROUND(a.android_id_count::numeric / a.total_devices, 4) ELSE 0 END,
            CASE WHEN a.total_devices > 0 THEN ROUND(a.oaid_count::numeric / a.total_devices, 4) ELSE 0 END,
            CASE WHEN a.android_id_count > 0 THEN ROUND(a.oaid_count::numeric / a.android_id_count, 4) ELSE 0 END,
            CASE WHEN a.total_reports > 0 THEN ROUND(a.oaid_count::numeric / a.total_reports, 4) ELSE 0 END,
            -- 衍生: 时间占比
            CASE WHEN a.total_reports > 0 THEN ROUND(a.workday_reports::numeric / a.total_reports, 4) ELSE 0 END,
            CASE WHEN a.total_reports > 0 THEN ROUND(a.weekend_reports::numeric / a.total_reports, 4) ELSE 0 END,
            CASE WHEN a.total_reports > 0 THEN ROUND(a.late_night_reports::numeric / a.total_reports, 4) ELSE 0 END,
            CASE WHEN a.total_reports > 0 THEN ROUND(a.root_reports::numeric / a.total_reports, 4) ELSE 0 END,
            -- 衍生: DAA/DNA
            CASE WHEN a.dna_reports > 0 THEN ROUND(a.daa_reports::numeric / a.dna_reports, 2) ELSE NULL END,
            t.top_operator,
            COALESCE(od.distinct_ops, 0),
            -- start_ip_text: bigint -> dotted text
            CONCAT(
                (er.ip_start >> 24) & 255, '.',
                (er.ip_start >> 16) & 255, '.',
                (er.ip_start >> 8) & 255, '.',
                er.ip_start & 255
            ),
            0  -- abnormal_rule_hits_total (暂设0)
        FROM agg a
        JOIN rb20_v2_5.e_runs er ON er.e_run_id = a.e_run_id AND er.run_id = '{RUN_ID}' AND er.shard_id = {shard_id}
        LEFT JOIN top_ops t ON a.e_run_id = t.e_run_id
        LEFT JOIN op_distinct od ON a.e_run_id = od.e_run_id
        LEFT JOIN abnormal_stats ab ON a.e_run_id = ab.e_run_id;

        COMMIT;
        """
        cur.execute(sql)
        
        # 统计该 shard 写入行数
        cur.execute(
            "SELECT COUNT(*) FROM rb20_v2_5.e_cidr_summary WHERE run_id = %s AND shard_id = %s",
            (RUN_ID, shard_id),
        )
        cnt = cur.fetchone()[0]
        log(f"Shard {shard_id:02d}: Done - {cnt} runs inserted.")
        conn.close()
        return True
    except Exception as e:
        log(f"Shard {shard_id:02d} FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def build_indexes():
    log("Building indexes on e_cidr_summary...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ecidr_run ON rb20_v2_5.e_cidr_summary(run_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ecidr_shard ON rb20_v2_5.e_cidr_summary(shard_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ecidr_ip_count ON rb20_v2_5.e_cidr_summary(ip_count DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ecidr_total_reports ON rb20_v2_5.e_cidr_summary(total_reports DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ecidr_run_len ON rb20_v2_5.e_cidr_summary(run_len DESC);")
    cur.execute("ANALYZE rb20_v2_5.e_cidr_summary;")
    conn.close()
    log("Indexes built and ANALYZE done.")


def main():
    start = time.time()
    log(f"=== Building E CIDR Summary Table (e_cidr_summary) ===")
    log(f"    Run: {RUN_ID}")
    log(f"    Source: {SOURCE_TABLE}")

    shard_ids = get_shard_ids()
    if not shard_ids:
        log("No shards selected, nothing to do.")
        return
    log(f"    Shards: {len(shard_ids)} ({shard_ids[0]}-{shard_ids[-1]}), Concurrency: {CONCURRENCY}")

    prep_table()

    shards = shard_ids
    log(f"Processing {len(shards)} shards...")

    pool = Pool(CONCURRENCY)
    results = pool.map(process_shard, shards)
    pool.close()
    pool.join()

    success = sum(1 for r in results if r)
    failed = len(shards) - success
    log(f"Shard results: {success} success, {failed} failed")

    if failed > 0:
        log("WARNING: Some shards failed! Check logs above.")

    build_indexes()

    # 最终统计
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM rb20_v2_5.e_cidr_summary WHERE run_id = %s", (RUN_ID,))
    total = cur.fetchone()[0]
    conn.close()

    elapsed = time.time() - start
    log(f"=== DONE: {total} CIDR blocks in e_cidr_summary ({elapsed:.1f}s) ===")


if __name__ == "__main__":
    main()
