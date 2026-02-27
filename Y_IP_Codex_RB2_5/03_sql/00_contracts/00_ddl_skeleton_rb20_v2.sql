-- RB20 v2.0 / Contracts / DDL Skeleton
-- 注意：执行前必须完成合同确认（主版本硬规则：先合同再执行）。
-- 本脚本默认输出 schema 为 rb20_v2_5（需 DP-006 确认）；如选择其它方案，请在执行前调整 schema。

CREATE SCHEMA IF NOT EXISTS rb20_v2_5;

-- 关键实体表（DDL 仅骨架；字段以 schema_contract_draft_v1.md 为准，确认后再固化）
-- run_meta / config_kv / shard_plan
CREATE TABLE IF NOT EXISTS rb20_v2_5.run_meta (
  run_id text PRIMARY KEY,
  contract_version text NOT NULL,
  status text NOT NULL,
  started_at timestamptz,
  finished_at timestamptz,
  note text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.config_kv (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  key text NOT NULL,
  value_text text,
  value_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, key)
);

CREATE TABLE IF NOT EXISTS rb20_v2_5.shard_plan (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  shard_id smallint NOT NULL,
  ip_long_start bigint NOT NULL,
  ip_long_end bigint NOT NULL,
  est_rows bigint,
  plan_round smallint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, shard_id)
);

-- abnormal_dedup（全局）
CREATE TABLE IF NOT EXISTS rb20_v2_5.abnormal_dedup (
  run_id text NOT NULL,
  contract_version text NOT NULL,
  ip_long bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, ip_long)
);

-- NOTE: per-shard 大表（source_members / maps / profiles / H/E/F）在合同最终确认后再补齐完整 DDL 与索引策略。
-- 完整 DDL：见 `01_ddl_rb20_v2_full.sql` 与 `02_indexes_rb20_v2.sql`
