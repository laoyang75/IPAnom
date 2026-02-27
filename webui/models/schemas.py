"""
Pydantic response schemas for API endpoints.
"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


# ── Dashboard ────────────────────────────────────────────
class RunInfo(BaseModel):
    run_id: str
    contract_version: Optional[str] = None
    status: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class OverviewStats(BaseModel):
    run_id: str
    source_members_cnt: int
    h_members_cnt: int
    e_members_cnt: int
    f_members_cnt: int
    keep_members_cnt: int
    drop_members_cnt: int
    block_natural_cnt: int
    block_final_cnt: int
    shard_cnt: int
    qa_total: int
    qa_passed: int


class ShardStatus(BaseModel):
    shard_id: int
    ip_long_start: Optional[int] = None
    ip_long_end: Optional[int] = None
    est_rows: Optional[int] = None
    source_members_cnt: int = 0
    block_natural_cnt: int = 0
    block_final_cnt: int = 0
    profile_final_cnt: int = 0
    keep_members_cnt: int = 0
    h_members_cnt: int = 0
    e_members_cnt: int = 0
    f_members_cnt: int = 0
    phases_completed: int = 0
    total_phases: int = 8


class QAAssert(BaseModel):
    assert_name: str
    severity: str
    pass_flag: bool
    details: Optional[str] = None
    created_at: Optional[datetime] = None


class StepStat(BaseModel):
    step_id: str
    shard_id: int
    metric_name: str
    metric_value_numeric: Optional[float] = None
    metric_value_text: Optional[str] = None


# ── Data Explorer ────────────────────────────────────────
class IPTraceResult(BaseModel):
    """Complete trace of an IP through the pipeline."""
    ip_long: int
    ip_address: Optional[str] = None
    country: Optional[str] = None
    operator: Optional[str] = None
    report_count: Optional[int] = None
    device_count: Optional[int] = None
    mobile_device_count: Optional[int] = None
    is_abnormal: Optional[bool] = None
    is_valid: Optional[bool] = None
    shard_id: Optional[int] = None
    # Natural block
    block_id_natural: Optional[str] = None
    block_natural_ip_start: Optional[int] = None
    block_natural_ip_end: Optional[int] = None
    block_natural_member_cnt: Optional[int] = None
    # Pre profile
    keep_flag: Optional[bool] = None
    drop_reason: Optional[str] = None
    network_tier_pre: Optional[str] = None
    simple_score_pre: Optional[int] = None
    density_pre: Optional[float] = None
    valid_cnt_pre: Optional[int] = None
    # Final block
    block_id_final: Optional[str] = None
    block_final_ip_start: Optional[int] = None
    block_final_ip_end: Optional[int] = None
    # Final profile
    network_tier_final: Optional[str] = None
    simple_score_final: Optional[int] = None
    density_final: Optional[float] = None
    valid_cnt_final: Optional[int] = None
    # Classification result
    classification: Optional[str] = None  # H / E / F / Drop / Unknown
    # E-specific
    atom27_id: Optional[int] = None
    e_run_id: Optional[str] = None


class BlockDetail(BaseModel):
    block_id: str
    block_type: str  # natural / final
    ip_start: Optional[int] = None
    ip_end: Optional[int] = None
    member_cnt_total: Optional[int] = None
    valid_cnt: Optional[int] = None
    density: Optional[float] = None
    network_tier: Optional[str] = None
    simple_score: Optional[int] = None
    wA: Optional[int] = None
    wD: Optional[int] = None


class NetworkTierDistribution(BaseModel):
    network_tier: str
    block_count: int
    member_count: int
