"""bfinance-datalake: Automated Indian Market Historical & Daily Data Lake."""

from datalake.engine import LakeTicker, SecurityIdentity, UnifiedLakeEngine
from datalake.finengine_bridge import (
    BHAV_REQUIRED_FIELDS,
    INSTITUTIONAL_SCREENS,
    LIQUIDITY_ADV_LOOKBACK_SESSIONS,
    build_ai_context_india,
    build_research_profile,
    calculate_graham_number,
    compute_amihud_illiquidity,
    compute_days_to_liquidate,
    compute_delivery_anomalies_from_frames,
    compute_forensic_ratios,
    compute_piotroski_score,
    compute_portfolio_liquidity_limits,
    format_bhavcopy_records,
    format_shareholding_for_finengine,
    transpose_financials_to_statement,
)
from datalake.remote import RemoteLakeClient, RemoteLakeEngine

__version__ = "0.2.0"
__all__ = [
    "UnifiedLakeEngine",
    "LakeTicker",
    "SecurityIdentity",
    "RemoteLakeClient",
    "RemoteLakeEngine",
    "BHAV_REQUIRED_FIELDS",
    "LIQUIDITY_ADV_LOOKBACK_SESSIONS",
    "INSTITUTIONAL_SCREENS",
    "compute_amihud_illiquidity",
    "compute_days_to_liquidate",
    "calculate_graham_number",
    "compute_piotroski_score",
    "compute_forensic_ratios",
    "transpose_financials_to_statement",
    "format_shareholding_for_finengine",
    "format_bhavcopy_records",
    "compute_delivery_anomalies_from_frames",
    "compute_portfolio_liquidity_limits",
    "build_ai_context_india",
    "build_research_profile",
    "__version__",
]
