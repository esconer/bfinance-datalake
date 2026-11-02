from __future__ import annotations

from datetime import date
from pathlib import Path
import pandas as pd

from datalake.storage import read_table_safe


class SymbolRenameResolver:
    """
    Resolves historical NSE ticker renames (e.g., ETERNAL -> ZOMATO, LTIM -> MINDTREE, INFY -> INFOSYSTCH)
    using `nse_symbol_changes.parquet` (`symbolchange.csv`).
    """

    def __init__(self, df: pd.DataFrame | None = None):
        self._back_edges: dict[str, list[tuple[date, str]]] = {}
        self._fwd_edges: dict[str, list[tuple[date, str]]] = {}
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                old_s = str(row.get("OldSymbol", "")).strip().upper()
                new_s = str(row.get("NewSymbol", "")).strip().upper()
                dt_val = row.get("ChangeDate")
                if not old_s or not new_s or old_s == new_s or pd.isna(dt_val):
                    continue
                chg_dt = pd.Timestamp(dt_val).date()
                self._back_edges.setdefault(new_s, []).append((chg_dt, old_s))
                self._fwd_edges.setdefault(old_s, []).append((chg_dt, new_s))

        for k in self._back_edges:
            self._back_edges[k].sort(key=lambda x: x[0], reverse=True)
        for k in self._fwd_edges:
            self._fwd_edges[k].sort(key=lambda x: x[0])

    @classmethod
    def from_lake(cls, lake_dir: Path | str = "./data") -> "SymbolRenameResolver":
        p = Path(lake_dir).resolve() / "universe" / "nse_symbol_changes.parquet"
        if p.exists():
            return cls(read_table_safe(p))
        return cls()

    def traded_as_on(self, current_symbol: str, target_date: date) -> str:
        """Return the NSE symbol that `current_symbol` actually traded under on `target_date`."""
        sym = current_symbol.strip().upper()
        upper_bound = date.max
        steps = 0
        while sym in self._back_edges and steps < 32:
            stepped = False
            for chg_dt, old_sym in self._back_edges[sym]:
                if target_date < chg_dt < upper_bound:
                    sym = old_sym
                    upper_bound = chg_dt
                    stepped = True
                    steps += 1
                    break
            if not stepped:
                break
        return sym

    def all_aliases(self, symbol: str) -> list[str]:
        """Return `[current_symbol, ...all historical predecessor symbols...]`."""
        sym = symbol.strip().upper()
        visited_fwd = {sym}
        upper_dt = date.min
        while sym in self._fwd_edges:
            stepped = False
            for chg_dt, nxt in reversed(self._fwd_edges[sym]):
                if chg_dt >= upper_dt and nxt not in visited_fwd:
                    visited_fwd.add(nxt)
                    upper_dt = chg_dt
                    sym = nxt
                    stepped = True
                    break
            if not stepped:
                break

        chain = [sym]
        queue = [sym]
        while queue:
            cur = queue.pop(0)
            for _, old_sym in self._back_edges.get(cur, ()):
                if old_sym not in chain:
                    chain.append(old_sym)
                    queue.append(old_sym)
        return chain
