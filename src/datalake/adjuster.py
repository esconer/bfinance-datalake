from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

from datalake.storage import read_table_safe
from datalake.symbols import SymbolRenameResolver


class OHLCVAdjuster:
    """
    Merges parsed NSE & BSE Corporate Actions into raw (`as_traded`) equity OHLCV frames
    (across symbol rename chains) and computes `Dividends`, `Stock Splits`, and backward `Adj Close`.
    """

    def __init__(self, lake_dir: Path | str = "./data"):
        self.lake_dir = Path(lake_dir).resolve()
        self._nse_actions: pd.DataFrame | None = None
        self._bse_actions: pd.DataFrame | None = None
        self._resolver: SymbolRenameResolver | None = None

    @property
    def resolver(self) -> SymbolRenameResolver:
        if self._resolver is None:
            self._resolver = SymbolRenameResolver.from_lake(self.lake_dir)
        return self._resolver

    def _load_actions(self, exchange: str) -> pd.DataFrame:
        exch = exchange.upper()
        if exch == "NSE":
            if self._nse_actions is None:
                p = self.lake_dir / "actions" / "nse_corporate_actions.parquet"
                self._nse_actions = read_table_safe(p) if p.exists() else pd.DataFrame()
            return self._nse_actions
        else:
            if self._bse_actions is None:
                p = self.lake_dir / "actions" / "bse_corporate_actions.parquet"
                self._bse_actions = read_table_safe(p) if p.exists() else pd.DataFrame()
            return self._bse_actions

    def get_symbol_actions(self, symbol_or_scrip: str, exchange: str = "NSE") -> pd.DataFrame:
        act_df = self._load_actions(exchange)
        if act_df.empty:
            return pd.DataFrame(columns=["ExDate", "DividendPerShareRs", "ShareMultiplier"])

        exch = exchange.upper()
        key = str(symbol_or_scrip).strip().upper()
        if key.endswith(".0"):
            key = key[:-2]

        if exch == "NSE" and "symbol" in act_df.columns:
            aliases = set(self.resolver.all_aliases(key))
            sub = act_df[act_df["symbol"].astype(str).str.strip().str.upper().isin(aliases)].copy()
        elif "scrip_code" in act_df.columns:
            sc_series = act_df["scrip_code"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
            mask = sc_series == key
            if "short_name" in act_df.columns:
                mask = mask | (act_df["short_name"].astype(str).str.strip().str.upper() == key)
            sub = act_df[mask].copy()
        else:
            return pd.DataFrame(columns=["ExDate", "DividendPerShareRs", "ShareMultiplier"])

        if sub.empty:
            return pd.DataFrame(columns=["ExDate", "DividendPerShareRs", "ShareMultiplier"])

        if "ActionType" in sub.columns:
            sub = sub[sub["ActionType"] != "SKIP_NON_EQUITY"].copy()

        sub["ExDate"] = pd.to_datetime(sub["ExDate"], errors="coerce").dt.normalize()
        sub["DividendPerShareRs"] = pd.to_numeric(sub["DividendPerShareRs"], errors="coerce").fillna(0.0)
        sub["ShareMultiplier"] = pd.to_numeric(sub["ShareMultiplier"], errors="coerce").fillna(1.0)
        sub = sub.dropna(subset=["ExDate"])
        sub = sub[(sub["DividendPerShareRs"] > 0.0) | (sub["ShareMultiplier"] != 1.0)]

        return (
            sub.groupby("ExDate", as_index=False)
            .agg({"DividendPerShareRs": "sum", "ShareMultiplier": "prod"})
            .sort_values("ExDate")
            .reset_index(drop=True)
        )

    def apply_adjustments(
        self,
        df: pd.DataFrame,
        symbol_or_scrip: str,
        exchange: str = "NSE",
        auto_adjust: bool = False,
        include_future_splits: bool = False,
    ) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        out = df.sort_values("Date").reset_index(drop=True).copy()
        out["Date"] = pd.to_datetime(out["Date"]).dt.normalize()
        out["Dividends"] = 0.0
        out["Stock Splits"] = 0.0

        post_window_split_factor = 1.0
        actions = self.get_symbol_actions(symbol_or_scrip, exchange=exchange)
        if not actions.empty:
            trading_dates = out["Date"].to_numpy()
            min_dt, max_dt = trading_dates[0], trading_dates[-1]

            in_win = actions[(actions["ExDate"] >= min_dt) & (actions["ExDate"] <= max_dt)]
            if not in_win.empty:
                ex_arr = in_win["ExDate"].to_numpy(dtype=trading_dates.dtype)
                indices = np.searchsorted(trading_dates, ex_arr, side="left")
                for idx_val, (_, row) in zip(indices, in_win.iterrows()):
                    idx = int(idx_val)
                    if 0 <= idx < len(out):
                        if row["DividendPerShareRs"] > 0:
                            out.loc[idx, "Dividends"] += float(row["DividendPerShareRs"])
                        if row["ShareMultiplier"] != 1.0:
                            cur_split = out.loc[idx, "Stock Splits"]
                            out.loc[idx, "Stock Splits"] = (
                                float(row["ShareMultiplier"])
                                if cur_split == 0.0
                                else cur_split * float(row["ShareMultiplier"])
                            )
            if include_future_splits:
                future_splits = actions[(actions["ExDate"] > max_dt) & (actions["ShareMultiplier"] != 1.0)]
                if not future_splits.empty:
                    post_window_split_factor = 1.0 / float(future_splits["ShareMultiplier"].prod())

        split_mult = out["Stock Splits"].replace(0.0, 1.0).to_numpy(dtype=float)
        rev_cum_splits = np.cumprod(split_mult[::-1])[::-1]
        cum_split_factor = np.ones(len(out), dtype=float)
        if len(out) > 1:
            cum_split_factor[:-1] = 1.0 / rev_cum_splits[1:]
        cum_split_factor *= post_window_split_factor

        # Detect Upstox-seeded historical rows (PrevClose and TurnoverValue are NaN)
        # which are already split-adjusted at the exchange source
        if "PrevClose" in out.columns and "TurnoverValue" in out.columns:
            is_already_split_adj = out["PrevClose"].isna() & out["TurnoverValue"].isna()
            cum_split_factor = np.where(is_already_split_adj.to_numpy(), 1.0, cum_split_factor)

        close_arr = out["Close"].to_numpy(dtype=float)
        div_arr = out["Dividends"].to_numpy(dtype=float)
        div_step = np.ones(len(out), dtype=float)
        if len(out) > 1:
            prev_close = close_arr[:-1]
            ex_divs = div_arr[1:]
            valid = (ex_divs > 0.0) & (prev_close > 0.0) & (ex_divs < prev_close)
            step = np.ones(len(out) - 1, dtype=float)
            step[valid] = 1.0 - (ex_divs[valid] / prev_close[valid])
            div_step[:-1] = np.cumprod(step[::-1])[::-1]

            # Detect unparsed demergers or special corporate actions where exchange updated PrevClose
            if "PrevClose" in out.columns:
                prev_c_col = pd.to_numeric(out["PrevClose"], errors="coerce").to_numpy(dtype=float)[1:]
                prior_close = close_arr[:-1]
                unexplained_gap = (
                    (split_mult[1:] == 1.0)
                    & (div_arr[1:] == 0.0)
                    & (prev_c_col > 0.0)
                    & (prior_close > 0.0)
                    & (np.abs(prev_c_col - prior_close) / prior_close > 0.015)
                )
                if unexplained_gap.any():
                    demerger_step = np.ones(len(out) - 1, dtype=float)
                    demerger_step[unexplained_gap] = prev_c_col[unexplained_gap] / prior_close[unexplained_gap]
                    div_step[:-1] *= np.cumprod(demerger_step[::-1])[::-1]

        total_price_factor = cum_split_factor * div_step
        out["Adj Close"] = np.round(out["Close"] * total_price_factor, 4)

        if auto_adjust:
            for col in ("Open", "High", "Low", "Close"):
                if col in out.columns:
                    out[col] = np.round(out[col] * total_price_factor, 4)
            for vcol in ("Volume", "DeliverableQty"):
                if vcol in out.columns:
                    out[vcol] = (out[vcol] / cum_split_factor).round()

        return out
