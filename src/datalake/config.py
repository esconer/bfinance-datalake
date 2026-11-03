from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def today_ist() -> date:
    """Return current date in Asia/Kolkata (IST)."""
    return datetime.now(IST).date()


def now_ist() -> datetime:
    """Return current timestamp in Asia/Kolkata (IST)."""
    return datetime.now(IST)


@dataclass(frozen=True)
class LakeConfig:
    root_dir: Path = field(
        default_factory=lambda: Path(os.environ.get("BFINANCE_LAKE_DIR", "./data")).resolve()
    )
    archive_concurrency: int = 4
    api_concurrency: int = 1
    polite_delay_sec: float = 0.25
    holiday_freeze_days: int = 5

    @property
    def state_db_path(self) -> Path:
        return self.root_dir / "state.db"

    def ensure_dirs(self) -> None:
        for sub in (
            "universe",
            "equities",
            "derivatives",
            "indices",
            "actions",
            "fundamentals",
            "shareholding",
            "mutual_funds",
            "flows",
            "deals",
            "documents",
            "snapshots",
        ):
            (self.root_dir / sub).mkdir(parents=True, exist_ok=True)
