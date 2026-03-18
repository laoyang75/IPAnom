"""
H库 Block 级属性摘要表构建脚本 (h_block_summary)

粒度：每个 block_id_final 一行（约 16,217 行）
数据源：
  1. rb20_v2_5.profile_final  —— block 级评分/结构字段 (38列)
  2. rb20_v2_5.h_members      —— block -> ip_long 关系
  3. public."ip库构建项目_ip源表..." —— IP 级原始属性 (63列)

用法: python3 build_h_block_summary.py
估计运行: 3-5 分钟（仅 13M IP，单线程即可）
"""
import os
import time
import psycopg2
import logging
import sys

# --------------- Configuration ---------------
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
SOURCE_TABLE = 'public."ip库构建项目_ip源表_20250811_20250824_v2_1"'
SCHEMA = "rb20_v2_5"

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s',
                    datefmt='%H:%M:%S', stream=sys.stdout)


def log(msg):
    logging.info(msg)
    sys.stdout.flush()


def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def prep_table():
    """CREATE h_block_summary"""
    log("Preparing h_block_summary table...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()

    ddl = f"""
    CREATE UNLOGGED TABLE IF NOT EXISTS {SCHEMA}.h_block_summary (
      -- A. 主键与结构信息 (来自 profile_final)
      block_id_final       varchar   NOT NULL,
      run_id               varchar   NOT NULL,
      shard_id             smallint,
      block_id_parent      varchar,
      network_tier_final   varchar,
      simple_score         int,
      wa                   int,
      wd                   int,
      density              numeric(10,4),
      member_cnt_total     bigint,
      valid_cnt            bigint,

      -- B. profile_final 已有的设备/上报汇总 (valid 口径)
      reports_sum_valid         bigint,
      devices_sum_valid         bigint,
      wifi_devices_sum_valid    bigint,
      mobile_devices_sum_valid  bigint,
      vpn_devices_sum_valid     bigint,
      wired_devices_sum_valid   bigint,
      report_density_valid      numeric(10,4),

      -- C. 从原始IP源表聚合的规模指标
      ip_count             int,          -- 实际 JOIN 到原始表的 IP 数
      total_reports        bigint,
      total_reports_pre_filter bigint,
      daa_reports          bigint,
      dna_reports          bigint,
      worktime_reports     bigint,
      workday_reports      bigint,
      weekend_reports      bigint,
      late_night_reports   bigint,

      -- D. 设备分类 (原始表口径)
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
      android_id_count   bigint,
      oaid_count         bigint,
      google_id_count    bigint,
      boot_id_count      bigint,
      model_count        bigint,
      manufacturer_count bigint,

      -- F. WiFi & 网络特征
      ssid_count              bigint,
      bssid_count             bigint,
      gateway_reports         bigint,
      ethernet_reports        bigint,
      wifi_comparable_reports bigint,

      -- G. 风险与异常特征
      proxy_reports            bigint,
      root_reports             bigint,
      adb_reports              bigint,
      charging_reports         bigint,
      max_single_device_reports bigint,

      -- H. 衍生画像指标
      avg_reports_per_ip       numeric(10,2),
      avg_devices_per_ip       numeric(10,2),
      avg_active_days          numeric(6,2),
      wifi_device_ratio        numeric(6,4),
      mobile_device_ratio      numeric(6,4),
      vpn_device_ratio         numeric(6,4),
      workday_report_ratio     numeric(6,4),
      late_night_report_ratio  numeric(6,4),
      daa_dna_ratio            numeric(10,2),
      top_operator             varchar,
      distinct_operators       int,
      unstable_ip_count        int      DEFAULT 0,
      unstable_ip_ratio        numeric(6,4) DEFAULT 0,

      -- I. 派生画像列（重建时自动填充）
      start_ip_text            varchar,
      avg_apps_per_ip          numeric(10,2),
      avg_apps_per_device      numeric(10,4),
      android_device_ratio     numeric(10,4),
      android_oaid_ratio       numeric(10,4),
      report_oaid_ratio        numeric(10,4),
      avg_manufacturer_per_ip  numeric(10,2),
      avg_model_per_ip         numeric(10,2),
      oaid_device_ratio        numeric(10,4),
      abnormal_ip_count        int      DEFAULT 0,
      abnormal_ip_ratio        numeric(6,4) DEFAULT 0,
      abnormal_rule_hits_total bigint   DEFAULT 0,

      -- J. 元数据
      created_at               timestamptz DEFAULT NOW(),

      PRIMARY KEY(run_id, block_id_final)
    );
    """
    cur.execute(ddl)
    cur.execute(f"DELETE FROM {SCHEMA}.h_block_summary WHERE run_id = %s", (RUN_ID,))
    log(f"h_block_summary table ready, cleared run_id={RUN_ID}.")
    conn.close()


def aggregate():
    """单次大 SQL 聚合 — H库只有 16K 块，不需要分 shard"""
    log("Starting aggregation: h_members JOIN original IP source table...")
    conn = get_db_conn()
    cur = conn.cursor()

    sql = f"""
    BEGIN;
    SET LOCAL work_mem = '1GB';
    SET LOCAL statement_timeout = '30min';

    WITH all_members AS (
        -- First: get abnormal flag for each h_member
        SELECT hm.block_id_final, hm.ip_long,
               COALESCE(sm.is_abnormal, false) AS is_abnormal
        FROM {SCHEMA}.h_members hm
        LEFT JOIN {SCHEMA}.source_members sm
          ON hm.run_id = sm.run_id AND hm.ip_long = sm.ip_long
        WHERE hm.run_id = '{RUN_ID}'
    ),
    -- Abnormal IP counts per block (keep for reference)
    abnormal_stats AS (
        SELECT block_id_final,
               COUNT(*) FILTER(WHERE is_abnormal) AS abnormal_ip_count,
               COUNT(*) AS total_ip_count
        FROM all_members
        GROUP BY block_id_final
    ),
    joined AS (
        SELECT
            am.block_id_final,
            am.ip_long,
            src."上报次数"               AS reports,
            src."过滤前上报次数"          AS reports_pre,
            src."设备数量"               AS devices,
            src."过滤前设备数量"          AS devices_pre,
            src."应用数量"               AS apps,
            src."活跃天数"               AS active_days,
            src."安卓ID数量"             AS android_ids,
            src."OAID数量"              AS oaids,
            src."谷歌ID数量"             AS google_ids,
            src."启动ID数量"             AS boot_ids,
            src."型号数量"               AS models,
            src."制造商数量"              AS manufacturers,
            src."深夜上报次数"            AS late_night,
            src."工作时上报次数"          AS worktime,
            src."工作日上报次数"          AS workday,
            src."周末上报次数"            AS weekend,
            src."以太网接口上报次数"       AS ethernet,
            src."代理上报次数"            AS proxy,
            src."Root设备上报次数"        AS root_rpt,
            src."ADB调试上报次数"         AS adb_rpt,
            src."充电状态上报次数"         AS charging,
            src."单设备最大上报次数"       AS max_dev_rpt,
            src."DAA业务上报次数"         AS daa,
            src."DNA业务上报次数"         AS dna,
            src."WiFi可比上报次数"        AS wifi_comp,
            src."SSID去重数"            AS ssids,
            src."BSSID去重数"           AS bssids,
            src."网关存在上报次数"         AS gateway,
            src."WiFi设备数量"           AS wifi_dev,
            src."移动网络设备数量"         AS mobile_dev,
            src."VPN设备数量"            AS vpn_dev,
            src."有线网络设备数量"         AS wired_dev,
            src."空网络状态设备数量"       AS empty_dev,
            src."异常网络设备数量"         AS abnormal_dev,
            src."IP归属运营商"           AS operator,
            src."IP稳定性"              AS ip_stability
        FROM all_members am
        JOIN {SOURCE_TABLE} src ON am.ip_long = src.ip_long
        WHERE am.is_abnormal = false  -- 排除异常IP
    ),
    -- 主要运营商
    op_counts AS (
        SELECT block_id_final, operator, COUNT(*) as cnt,
               ROW_NUMBER() OVER(PARTITION BY block_id_final ORDER BY COUNT(*) DESC) as rn
        FROM joined
        WHERE operator IS NOT NULL AND operator != ''
        GROUP BY block_id_final, operator
    ),
    top_ops AS (
        SELECT block_id_final, operator as top_operator
        FROM op_counts WHERE rn = 1
    ),
    op_distinct AS (
        SELECT block_id_final, COUNT(DISTINCT operator) as distinct_ops
        FROM joined
        WHERE operator IS NOT NULL AND operator != ''
        GROUP BY block_id_final
    ),
    -- 主聚合
    agg AS (
        SELECT
            j.block_id_final,
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
        GROUP BY j.block_id_final
    )
    INSERT INTO {SCHEMA}.h_block_summary (
        block_id_final, run_id, shard_id, block_id_parent,
        network_tier_final, simple_score, wa, wd, density,
        member_cnt_total, valid_cnt,
        reports_sum_valid, devices_sum_valid,
        wifi_devices_sum_valid, mobile_devices_sum_valid,
        vpn_devices_sum_valid, wired_devices_sum_valid,
        report_density_valid,
        -- 原始表聚合
        ip_count,
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
        wifi_device_ratio, mobile_device_ratio, vpn_device_ratio,
        workday_report_ratio, late_night_report_ratio,
        daa_dna_ratio, top_operator, distinct_operators,
        unstable_ip_count, unstable_ip_ratio,
        abnormal_ip_count, abnormal_ip_ratio
    )
    SELECT
        a.block_id_final,
        '{RUN_ID}',
        pf.shard_id, pf.block_id_parent,
        pf.network_tier_final, pf.simple_score, pf.wa, pf.wd, pf.density,
        pf.member_cnt_total, pf.valid_cnt,
        pf.reports_sum_valid, pf.devices_sum_valid,
        pf.wifi_devices_sum_valid, pf.mobile_devices_sum_valid,
        pf.vpn_devices_sum_valid, pf.wired_devices_sum_valid,
        pf.report_density_valid,
        -- 原始表聚合结果
        a.ip_count,
        a.total_reports, a.total_reports_pre_filter, a.daa_reports, a.dna_reports,
        a.worktime_reports, a.workday_reports, a.weekend_reports, a.late_night_reports,
        a.total_devices, a.total_devices_pre_filter,
        a.wifi_devices, a.mobile_devices, a.vpn_devices, a.wired_devices,
        a.abnormal_net_devices, a.empty_net_devices, a.total_apps,
        a.android_id_count, a.oaid_count, a.google_id_count, a.boot_id_count,
        a.model_count, a.manufacturer_count,
        a.ssid_count, a.bssid_count, a.gateway_reports, a.ethernet_reports, a.wifi_comparable_reports,
        a.proxy_reports, a.root_reports, a.adb_reports, a.charging_reports, a.max_single_device_reports,
        -- 衍生
        CASE WHEN a.ip_count > 0 THEN ROUND(a.total_reports::numeric / a.ip_count, 2) ELSE 0 END,
        CASE WHEN a.ip_count > 0 THEN ROUND(a.total_devices::numeric / a.ip_count, 2) ELSE 0 END,
        a.avg_active_days,
        CASE WHEN a.total_devices > 0 THEN ROUND(a.wifi_devices::numeric / a.total_devices, 4) ELSE 0 END,
        CASE WHEN a.total_devices > 0 THEN ROUND(a.mobile_devices::numeric / a.total_devices, 4) ELSE 0 END,
        CASE WHEN a.total_devices > 0 THEN ROUND(a.vpn_devices::numeric / a.total_devices, 4) ELSE 0 END,
        CASE WHEN a.total_reports > 0 THEN ROUND(a.workday_reports::numeric / a.total_reports, 4) ELSE 0 END,
        CASE WHEN a.total_reports > 0 THEN ROUND(a.late_night_reports::numeric / a.total_reports, 4) ELSE 0 END,
        CASE WHEN a.dna_reports > 0 THEN ROUND(a.daa_reports::numeric / a.dna_reports, 2) ELSE NULL END,
        t.top_operator,
        COALESCE(od.distinct_ops, 0),
        a.unstable_ip_count,
        CASE WHEN a.ip_count > 0 THEN ROUND(a.unstable_ip_count::numeric / a.ip_count, 4) ELSE 0 END,
        COALESCE(ab.abnormal_ip_count, 0),
        CASE WHEN ab.total_ip_count > 0 THEN ROUND(ab.abnormal_ip_count::numeric / ab.total_ip_count, 4) ELSE 0 END
    FROM agg a
    JOIN {SCHEMA}.profile_final pf
        ON pf.run_id = '{RUN_ID}' AND pf.block_id_final = a.block_id_final
    LEFT JOIN top_ops t ON a.block_id_final = t.block_id_final
    LEFT JOIN op_distinct od ON a.block_id_final = od.block_id_final
    LEFT JOIN abnormal_stats ab ON a.block_id_final = ab.block_id_final;

    COMMIT;
    """
    cur.execute(sql)
    conn.close()
    log("Aggregation complete.")


def fill_derived_columns():
    """填充派生列：先做表内派生，再按 h_members 回填块起始IP。"""
    log("Filling derived columns...")
    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute("""
    BEGIN;
    SET LOCAL work_mem = '1GB';
    SET LOCAL statement_timeout = '30min';
    """)

    cur.execute(f"""
    UPDATE {SCHEMA}.h_block_summary hs SET
      avg_apps_per_ip = CASE WHEN ip_count > 0 THEN ROUND(total_apps::numeric / ip_count, 2) ELSE NULL END,
      avg_devices_per_ip = CASE WHEN ip_count > 0 THEN ROUND(total_devices::numeric / ip_count, 2) ELSE NULL END,
      android_device_ratio = CASE WHEN total_devices > 0 THEN LEAST(ROUND(android_id_count::numeric / total_devices, 4), 99.9999) ELSE NULL END,
      android_oaid_ratio = CASE WHEN total_devices > 0 THEN LEAST(ROUND(oaid_count::numeric / total_devices, 4), 99.9999) ELSE NULL END,
      report_oaid_ratio = CASE WHEN total_reports > 0 THEN LEAST(ROUND(oaid_count::numeric / total_reports * 100, 4), 99.9999) ELSE NULL END,
      avg_manufacturer_per_ip = CASE WHEN ip_count > 0 THEN ROUND(manufacturer_count::numeric / ip_count, 2) ELSE NULL END,
      avg_model_per_ip = CASE WHEN ip_count > 0 THEN ROUND(model_count::numeric / ip_count, 2) ELSE NULL END,
      oaid_device_ratio = CASE WHEN total_devices > 0 THEN LEAST(ROUND(oaid_count::numeric / total_devices, 4), 99.9999) ELSE NULL END,
      avg_apps_per_device = CASE WHEN total_devices > 0 THEN ROUND(total_apps::numeric / total_devices, 4) ELSE NULL END
    WHERE hs.run_id = %s;
    """, (RUN_ID,))

    # block_final 在大 run 下会达到千万行。这里直接从 H 成员表取每块最小 IP，
    # 既能保持与历史 start_ip_text 一致，也能避免相关子查询反复回扫 block_final。
    cur.execute(f"""
    CREATE TEMP TABLE tmp_h_block_starts ON COMMIT DROP AS
    SELECT
      hm.run_id,
      hm.block_id_final,
      MIN(hm.ip_long) AS start_ip_long
    FROM {SCHEMA}.h_members hm
    WHERE hm.run_id = %s
    GROUP BY hm.run_id, hm.block_id_final;
    """, (RUN_ID,))
    cur.execute("""
    CREATE INDEX tmp_h_block_starts_idx
    ON tmp_h_block_starts(run_id, block_id_final);
    """)
    cur.execute("ANALYZE tmp_h_block_starts;")

    cur.execute(f"""
    UPDATE {SCHEMA}.h_block_summary hs
    SET start_ip_text = host(('0.0.0.0'::inet + t.start_ip_long))
    FROM tmp_h_block_starts t
    WHERE hs.run_id = %s
      AND t.run_id = hs.run_id
      AND t.block_id_final = hs.block_id_final;
    """, (RUN_ID,))

    conn.commit()
    log("Derived columns filled.")
    conn.close()


def build_indexes():
    log("Building indexes...")
    conn = get_db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_hbs_run ON {SCHEMA}.h_block_summary(run_id);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_hbs_score ON {SCHEMA}.h_block_summary(simple_score DESC);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_hbs_ipcnt ON {SCHEMA}.h_block_summary(ip_count DESC);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_hbs_reports ON {SCHEMA}.h_block_summary(total_reports DESC);")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_hbs_tier ON {SCHEMA}.h_block_summary(network_tier_final);")
    cur.execute(f"ANALYZE {SCHEMA}.h_block_summary;")
    conn.close()
    log("Indexes built and ANALYZE done.")


def main():
    start = time.time()
    log("=== Building H Block Summary Table (h_block_summary) ===")
    log(f"    Run: {RUN_ID}")
    log(f"    Source: {SOURCE_TABLE}")
    log(f"    Expected: ~16K blocks")

    prep_table()
    aggregate()
    fill_derived_columns()
    build_indexes()

    # 最终统计
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.h_block_summary WHERE run_id = %s", (RUN_ID,))
    total = cur.fetchone()[0]
    conn.close()

    elapsed = time.time() - start
    log(f"=== DONE: {total} blocks in h_block_summary ({elapsed:.1f}s) ===")


if __name__ == "__main__":
    main()
