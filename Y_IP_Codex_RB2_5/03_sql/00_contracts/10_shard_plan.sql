-- RB20 v2.0 / Contracts / ShardPlan (Executable, Legacy)
-- 说明：
-- - 本文件为“plpgsql 可执行版”的历史实现（固定 64 shard：初始等分 + 最多 3 轮 5% 调整）。
-- - DBHub 的 execute_sql 对 $$ plpgsql 支持不稳定；且在“成员稀疏且成簇”时，范围等分容易产生空 shard。
-- - 现主流程推荐使用 SQL-only：`03_sql/00_contracts/10_shard_plan_generate_sql_only.sql`（基于 /16 直方图的近似分位切分；避免全量排序 NTILE）。
--
-- 依赖：
-- - 输出 schema：rb20_v2_5（DP-006 已确认）
-- - 表：rb20_v2_5.shard_plan（见 schema_contract_draft_v1.md，确认后会固化 DDL）
--
-- 使用方式（示例）：
--   CALL rb20_v2_5.generate_shard_plan('rb20v2_YYYYMMDD_HHMMSS_sg_001', 'contract_v1', 0.10, 3);

CREATE SCHEMA IF NOT EXISTS rb20_v2_5;

CREATE OR REPLACE PROCEDURE rb20_v2_5.generate_shard_plan(
  p_run_id text,
  p_contract_version text,
  p_eps float8 DEFAULT 0.10,
  p_max_rounds int DEFAULT 3
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_min bigint;
  v_max bigint;
  v_span bigint; -- exclusive span: (v_max+1) - v_min
  v_base bigint;
  v_rem bigint;
  v_round int := 0;
  v_any_adjust boolean;

  a_start bigint[];
  a_end bigint[];
  a_cnt bigint[];
  a_big boolean[];
  a_small boolean[];

  v_avg_cnt float8;
  v_total_cnt bigint;
  v_cuts float8[];

  i int;
  l_len bigint;
  r_len bigint;
  base_shift bigint;
  score int;
  raw_shift bigint;
  shift bigint;
  cut bigint;
BEGIN
  IF p_max_rounds < 0 OR p_max_rounds > 3 THEN
    RAISE EXCEPTION 'p_max_rounds must be between 0 and 3, got %', p_max_rounds;
  END IF;

  -- 1) 输入范围（唯一锚点）
  SELECT MIN(ip_long), MAX(ip_long)
  INTO v_min, v_max
  FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1";

  IF v_min IS NULL OR v_max IS NULL THEN
    RAISE EXCEPTION 'source table is empty: cannot build shard plan';
  END IF;

  v_span := (v_max + 1) - v_min;
  IF v_span <= 0 THEN
    RAISE EXCEPTION 'invalid span: min=% max=% span=%', v_min, v_max, v_span;
  END IF;

  -- 2) 初始按范围 64 等分（精确整数拆分，保证无空 shard）
  v_base := v_span / 64;
  v_rem := v_span % 64;

  a_start := ARRAY[]::bigint[];
  a_end := ARRAY[]::bigint[];

  FOR i IN 0..63 LOOP
    -- 前 v_rem 个 shard 多 1 个 ip_long
    a_start := a_start || (v_min + i*v_base + LEAST(i::bigint, v_rem));
    a_end := a_end || (v_min + (i+1)*v_base + LEAST((i+1)::bigint, v_rem));
  END LOOP;

  -- 3) 迭代：统计 cnt → 判定 big/small → 顺序调整边界（最多 3 轮）
  FOR v_round IN 0..p_max_rounds LOOP
    -- 3.1 生成 cuts（shard0..62 的 last_ip=end-1，float8 足够精确覆盖 IPv4）
    v_cuts := ARRAY[]::float8[];
    FOR i IN 1..63 LOOP
      v_cuts := v_cuts || ((a_end[i] - 1)::float8);
    END LOOP;

    -- 3.2 单次扫描计算每 shard 行数（width_bucket 使用自定义阈值数组）
    SELECT
      ARRAY_AGG(cnt ORDER BY shard_id),
      SUM(cnt)
    INTO a_cnt, v_total_cnt
    FROM (
      SELECT
        (width_bucket(ip_long::float8, v_cuts) - 1)::int AS shard_id,
        COUNT(*)::bigint AS cnt
      FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
      GROUP BY 1
    ) t;

    IF a_cnt IS NULL OR array_length(a_cnt,1) <> 64 THEN
      -- width_bucket 在极端情况下可能出现缺失 shard（例如某 shard 计数为 0 不会出现在 GROUP BY 结果）
      -- 这里补齐缺失为 0
      a_cnt := ARRAY(SELECT COALESCE(x.cnt,0)::bigint
                    FROM generate_series(0,63) gs(shard_id)
                    LEFT JOIN (
                      SELECT (width_bucket(ip_long::float8, v_cuts) - 1)::int AS shard_id, COUNT(*)::bigint AS cnt
                      FROM public."ip库构建项目_ip源表_20250811_20250824_v2_1"
                      GROUP BY 1
                    ) x USING (shard_id)
                    ORDER BY shard_id);
      v_total_cnt := (SELECT SUM(x) FROM unnest(a_cnt) u(x));
    END IF;

    -- 空 shard 直接 STOP（主版本失败策略）
    IF EXISTS (SELECT 1 FROM unnest(a_cnt) u(x) WHERE x = 0) THEN
      RAISE EXCEPTION 'ShardPlan invalid: empty shard detected (round=%). Regenerate required.', v_round;
    END IF;

    v_avg_cnt := v_total_cnt::float8 / 64.0;
    a_big := ARRAY[]::boolean[];
    a_small := ARRAY[]::boolean[];
    FOR i IN 1..64 LOOP
      a_big := a_big || ((a_cnt[i]::float8) > (v_avg_cnt * (1.0 + p_eps)));
      a_small := a_small || ((a_cnt[i]::float8) < (v_avg_cnt * (1.0 - p_eps)));
    END LOOP;

    -- 最后一轮或无需调整：退出
    IF v_round = p_max_rounds THEN
      EXIT;
    END IF;

    v_any_adjust := EXISTS (SELECT 1 FROM unnest(a_big) b(x) WHERE x)
                    OR EXISTS (SELECT 1 FROM unnest(a_small) s(x) WHERE x);
    IF NOT v_any_adjust THEN
      EXIT;
    END IF;

    -- 3.3 顺序调整 63 个边界，避免相互穿越
    FOR i IN 1..63 LOOP
      -- shard i-1 与 shard i 的边界是 a_end[i]
      l_len := a_end[i] - a_start[i];
      r_len := a_end[i+1] - a_start[i+1];
      IF l_len <= 1 OR r_len <= 1 THEN
        CONTINUE;
      END IF;

      base_shift := FLOOR(LEAST(l_len, r_len)::float8 * 0.05)::bigint;
      IF base_shift <= 0 THEN
        CONTINUE;
      END IF;

      -- direction score:
      -- left_expand (left small)  => +1
      -- right_shrink (right big) => +1
      -- left_shrink (left big)   => -1
      -- right_expand (right small)=> -1
      score :=
        (CASE WHEN a_small[i] THEN 1 ELSE 0 END) +
        (CASE WHEN a_big[i+1] THEN 1 ELSE 0 END) -
        (CASE WHEN a_big[i] THEN 1 ELSE 0 END) -
        (CASE WHEN a_small[i+1] THEN 1 ELSE 0 END);

      IF score = 0 THEN
        CONTINUE;
      END IF;

      raw_shift := (CASE WHEN score > 0 THEN 1 ELSE -1 END) * base_shift;

      -- clamp to keep both shards non-empty
      shift := GREATEST(-(l_len - 1), LEAST(raw_shift, (r_len - 1)));
      IF shift = 0 THEN
        CONTINUE;
      END IF;

      cut := a_end[i] + shift;
      -- apply
      a_end[i] := cut;
      a_start[i+1] := cut;
    END LOOP;
  END LOOP;

  -- 4) 写入最终 shard_plan（幂等：先清理 run_id）
  DELETE FROM rb20_v2_5.shard_plan WHERE run_id = p_run_id;

  FOR i IN 0..63 LOOP
    INSERT INTO rb20_v2_5.shard_plan(
      run_id, contract_version, shard_id,
      ip_long_start, ip_long_end,
      est_rows, plan_round, created_at
    )
    VALUES (
      p_run_id, p_contract_version, i::smallint,
      a_start[i+1], a_end[i+1],
      a_cnt[i+1], v_round::smallint, now()
    );
  END LOOP;
END;
$$;
