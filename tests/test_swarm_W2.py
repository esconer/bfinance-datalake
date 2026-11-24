"""Deterministic offline regression tests for Workstream W2.

W2A: zero-false-positive NSE/BSE debt filter for cash-market equity frames.
W2B: participant-volume CSV sync (dataset/URL/target parquet parameterized).

No live network: all HTTP is mocked with SimpleHTTPResponse.
"""

import asyncio
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from datalake.config import LakeConfig
from datalake.http_client import SimpleHTTPResponse
from datalake.scrapers.equity_eod import (
    EquityEODScraper,
    filter_bse_equity_only,
    filter_equity_only,
    is_debt_instrument,
)
from datalake.scrapers.funds_flows import FundsAndFlowsScraper
from datalake.state import StateStore
from datalake.storage import read_table_safe

# --------------------------------------------------------------------------- W2A

# Verified equity / ETF identities from the verification spec. BONDADA is the
# critical case: a naive substring "BOND" name rule would misclassify it.
EQUITY_CASES = [
    pytest.param("RELIANCE", "EQ", "INE002A01018", "RELIANCE INDUSTRIES LTD", id="reliance"),
    pytest.param("BONDADA", "EQ", "INE0Q8P01011", "BONDADA", id="bondada-eq"),
    pytest.param("BONDADA", "SM", "INE0Q8P01011", "BONDADA", id="bondada-sm"),
    pytest.param("GOLDBEES", "EQ", "INF204KB17I5", "GOLDBEES", id="goldbees"),
    pytest.param("LIQUIDBEES", "EQ", "INF732E01037", "LIQUIDBEES", id="liquidbees"),
    pytest.param("GSFC", "EQ", "INE026A01025", "GSFC", id="gfc"),
]

DEBT_CASES = [
    pytest.param("726GS2033", "GS", "IN0020220151", None, "NSE", id="gsec-726gs2033"),
    pytest.param("364D150126", "TB", "IN002024Z441", None, "NSE", id="tbill-364d"),
    pytest.param("SGBFEB32IV", "GB", "IN0020230184", None, "NSE", id="sgb"),
    pytest.param("NCD", "N1", "INE001A07RU9", None, "NSE", id="corp-ncd-ine07"),
    pytest.param("GOVT", "G", None, None, "BSE", id="bse-group-g"),
    pytest.param("CORP", "F", "INE001A07RU9", None, "BSE", id="bse-group-f-ine"),
    pytest.param(None, "B", None, "8.50% NHAI TAX FREE BOND 2029", "NSE", id="unnamed-coupon-bond"),
]


@pytest.mark.parametrize("symbol,series,isin,name", EQUITY_CASES)
def test_is_debt_instrument_zero_false_positives_on_equities_and_etfs(symbol, series, isin, name):
    assert is_debt_instrument(symbol=symbol, series=series, isin=isin, name=name) is False


@pytest.mark.parametrize("symbol,series,isin,name,exchange", DEBT_CASES)
def test_is_debt_instrument_detects_debt(symbol, series, isin, name, exchange):
    assert is_debt_instrument(symbol=symbol, series=series, isin=isin, name=name, exchange=exchange) is True


def test_isin_patterns_match_real_12_char_indian_isins():
    # p3.txt's {5} quantifier is off by one; these real ISINs must match the
    # 4-issuer-char + 2-type-digit structure or ISIN-level protection is inert.
    from datalake.scrapers.equity_eod import _DEBT_ISIN_RE, _EQUITY_OR_ETF_ISIN_RE

    assert _DEBT_ISIN_RE.match("INE001A07RU9")
    assert _EQUITY_OR_ETF_ISIN_RE.match("INE0Q8P01011")
    assert _EQUITY_OR_ETF_ISIN_RE.match("INE002A01018")


def test_verified_equity_isin_protection_overrides_debt_style_name():
    # Critical zero-false-positive guard: even a debt-looking instrument name
    # must not flag a verified equity ISIN when no explicit debt series is set.
    assert is_debt_instrument(
        symbol="BONDADA", series="B", isin="INE0Q8P01011",
        name="8.50% NHAI TAX FREE BOND 2029",
    ) is False
    # The same name with no protected ISIN is still detected as debt.
    assert is_debt_instrument(
        symbol="NHAI", series="B", isin=None,
        name="8.50% NHAI TAX FREE BOND 2029",
    ) is True


def test_explicit_debt_series_wins_over_equity_isin_protection():
    # Ordering contract: step 2 (explicit debt series/group) precedes step 3
    # (equity ISIN protection); an explicit debt tag must never be protected.
    assert is_debt_instrument(symbol="X", series="GS", isin="INE002A01018") is True
    assert is_debt_instrument(symbol="X", series="F", isin="INE002A01018", exchange="BSE") is True
    assert is_debt_instrument(symbol="X", series="G", isin="INE002A01018", exchange="BSE") is True


def test_equity_name_with_bond_substring_and_generic_series_is_protected():
    # When ISIN is unavailable EQUITY_CASES-style protection relies on the cash
    # equity series; verify the name fallback cannot override a verified EQ series.
    assert is_debt_instrument(symbol="BONDADA", series="EQ", isin=None, name="BONDADA") is False


def _mixed_frame() -> pd.DataFrame:
    """Mixed NSE-style cash frame: 4 equities/ETFs + 3 debt instruments."""
    return pd.DataFrame({
        "Symbol": ["RELIANCE", "BONDADA", "GOLDBEES", "LIQUIDBEES", "726GS2033", "SGBFEB32IV", "NCD"],
        "Series": ["EQ", "SM", "EQ", "EQ", "GS", "GB", "N1"],
        "ISIN": [
            "INE002A01018", "INE0Q8P01011", "INF204KB17I5", "INF732E01037",
            "IN0020220151", "IN0020230184", "INE001A07RU9",
        ],
        "Close": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
    })


def test_filter_equity_only_nse_keeps_equities_etfs_drops_debt():
    df = _mixed_frame()
    out = filter_equity_only(df, exchange="NSE")
    assert out["Symbol"].tolist() == ["RELIANCE", "BONDADA", "GOLDBEES", "LIQUIDBEES"]
    assert out["Close"].tolist() == [1.0, 2.0, 3.0, 4.0]
    assert out.index.is_monotonic_increasing and out.index[0] == 0


def test_filter_bse_equity_only_drops_debt_leaks_and_keeps_equities():
    # The legacy implementation leaked G-Sec/SDL/NCD rows identified by ISIN or
    # instrument name rather than by BSE Group 'G'/'F' alone.
    df = pd.DataFrame({
        "Symbol": ["RELIANCE", "GOVTSEC", "NHAI BOND", "CORP", "TBILL91"],
        "Series": ["A", "B", "B", "F", "B"],
        "ISIN": ["INE002A01018", "IN0012345678", None, "INE001A07RU9", "IN0020250001"],
    })
    out = filter_bse_equity_only(df)
    # GOVTSEC (IN00), TBILL91 (IN00) and CORP (BSE Group F / INE) are dropped;
    # NHAI BOND has no ISIN/name_series so only its name could identify it.
    assert out["Symbol"].tolist() == ["RELIANCE", "NHAI BOND"]

    # With a real coupon-bond name supplied (as UDIFF does), the name rule fires.
    # A bare "BOND" token is deliberately NOT a rule: it would flag BONDADA.
    names = df["Symbol"].where(df["Symbol"] != "NHAI BOND", "8.50% NHAI TAX FREE BOND 2029")
    named = filter_equity_only(df, exchange="BSE", name_series=names)
    assert named["Symbol"].tolist() == ["RELIANCE"]


def test_filter_equity_only_empty_frame_is_passthrough():
    empty = pd.DataFrame(columns=["Symbol", "Series", "ISIN"])
    assert filter_equity_only(empty).empty
    assert filter_bse_equity_only(empty).empty


def test_normalize_udiff_filters_debt_using_fininstrmnm():
    # UDIFF has no usable debt ISIN for some instruments; FinInstrmNm must be
    # checked before projection so coupon-bond names are dropped.
    day = date(2026, 1, 5)
    raw = pd.DataFrame({
        "Sgmt": ["CM", "CM", "CM"],
        "TckrSymb": ["RELIANCE", "BONDADA", "NHAI"],
        "FinInstrmId": ["500325", "543123", "500999"],
        "SctySrs": ["EQ", "EQ", "B"],
        "ISIN": ["INE002A01018", "INE0Q8P01011", "IN0012345678"],
        "FinInstrmNm": ["RELIANCE INDUSTRIES LTD", "BONDADA", "8.50% NHAI TAX FREE BOND 2029"],
        "OpnPric": ["1", "2", "3"],
        "HghPric": ["1", "2", "3"],
        "LwPric": ["1", "2", "3"],
        "ClsPric": ["1", "2", "3"],
        "LastPric": ["1", "2", "3"],
        "PrvsClsgPric": ["1", "2", "3"],
        "TtlTradgVol": ["10", "20", "30"],
        "TtlTrfVal": ["100", "200", "300"],
        "TtlNbOfTxsExctd": ["2", "3", "4"],
    })
    out = EquityEODScraper._normalize_udiff(raw.copy(), day, "NSE")
    assert out["Symbol"].tolist() == ["RELIANCE", "BONDADA"]


def test_fetch_nse_sec_bhavdata_full_filters_debt():
    day = date(2026, 1, 5)
    csv = (
        "SYMBOL,SERIES,DATE1,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,LAST_PRICE,"
        "PREV_CLOSE,TTL_TRD_QNTY,TURNOVER_LACS,NO_OF_TRADES,DELIV_QTY,DELIV_PER\n"
        "RELIANCE,EQ,05-Jan-2026,100,101,99,100,100,99,1000,100.0,50,500,50.0\n"
        "BONDADA,EQ,05-Jan-2026,10,11,9,10,10,9,2000,20.0,60,1000,50.0\n"
        "726GS2033,GS,05-Jan-2026,1000,1001,999,1000,1000,999,10,1.0,2,5,50.0\n"
    ).encode()

    class _Http:
        async def get(self, url, **kwargs):
            return SimpleHTTPResponse(status_code=200, content=csv)

    scraper = EquityEODScraper(_Http(), None, None)
    out, _, _ = asyncio.run(scraper._fetch_nse_sec_bhavdata_full(day))
    assert out is not None
    assert out["Symbol"].tolist() == ["RELIANCE", "BONDADA"]


# --------------------------------------------------------------------------- W2B

_PARTICIPANT_CSV = (
    "NSE Participant Volume Data (fao_participant_vol)\n"
    "Client Type,Long Clients %,Long Clients (Contracts),Short Clients %,Short Clients (Contracts)\n"
    "Client,10,100,20,200\n"
    "FII,30,300,40,400\n"
    "DII,5,50,6,60\n"
    "Pro,7,70,8,80\n"
    "Other,1,1,1,1\n"
).encode()


class _ParticipantHttp:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.urls: list[str] = []

    async def get(self, url, **kwargs):
        self.urls.append(url)
        return SimpleHTTPResponse(status_code=200, content=self.payload)


def _scraper_with(tmp: str, http: _ParticipantHttp):
    cfg = LakeConfig(root_dir=Path(tmp), polite_delay_sec=0.0)
    cfg.ensure_dirs()
    state = StateStore(cfg.state_db_path)
    return FundsAndFlowsScraper(http, state, cfg), cfg, state


def test_sync_participant_vol_writes_parquet_and_watermark():
    day = date(2026, 1, 5)  # Monday
    http = _ParticipantHttp(_PARTICIPANT_CSV)
    with tempfile.TemporaryDirectory() as tmp:
        scraper, cfg, state = _scraper_with(tmp, http)
        asyncio.run(scraper.sync_participant_vol(day, day))

        assert http.urls and all("fao_participant_vol_" in u for u in http.urls)
        assert any(f"{day:%d%m%Y}.csv" in u for u in http.urls)

        target = cfg.root_dir / "flows" / "fao_participant_vol_2026.parquet"
        assert target.exists(), f"missing {target}"
        out = read_table_safe(target)
        assert set(out["Client Type"]) == {"Client", "FII", "DII", "Pro"}
        assert "Other" not in set(out["Client Type"])
        assert out["Date"].iloc[0].date() == day

        assert state.is_completed("nse_participant_vol", day.isoformat()) is True
        assert state.completed_keys("nse_participant_oi") == set()


def test_sync_participant_oi_behavior_preserved():
    """W2B refactor must not change the existing OI dataset, URL or target file."""
    day = date(2026, 1, 5)
    http = _ParticipantHttp(_PARTICIPANT_CSV)
    with tempfile.TemporaryDirectory() as tmp:
        scraper, cfg, state = _scraper_with(tmp, http)
        asyncio.run(scraper.sync_participant_oi(day, day))

        assert http.urls and all("fao_participant_oi_" in u for u in http.urls)
        assert (cfg.root_dir / "flows" / "fao_participant_oi_2026.parquet").exists()
        assert state.is_completed("nse_participant_oi", day.isoformat()) is True
        assert state.completed_keys("nse_participant_vol") == set()
