from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import gc
import hashlib
import io
import json
import logging
import os
import shutil
import time
from pathlib import Path
import pandas as pd

from datalake.config import now_ist, today_ist
from datalake.engine import UnifiedLakeEngine
from datalake.http_client import ARCHIVE_HEADERS, ResilientHTTPClient
from datalake.storage import read_table_safe, sanitize_for_parquet, write_parquet_atomic
from datalake.symbols import SymbolRenameResolver

logger = logging.getLogger("datalake.sync")


def compute_sha256(path: Path) -> str:
    """Compute SHA-256 hash of a file efficiently in 1MB chunks."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_manifest(data_dir: Path) -> dict:
    """
    Build `data/manifest.json` mapping each relative path (e.g. 'equities/nse_cm_2026.parquet')
    to its release asset filename ('equities__nse_cm_2026.parquet'), size_bytes, and sha256.
    """
    files_meta: dict[str, dict] = {}
    for pq in sorted(data_dir.rglob("*.parquet")):
        rel = pq.relative_to(data_dir).as_posix()
        asset_name = rel.replace("/", "__")
        files_meta[rel] = {
            "asset_name": asset_name,
            "size_bytes": pq.stat().st_size,
            "sha256": compute_sha256(pq),
        }
    manifest = {
        "version": 1,
        "generated_at": now_ist().isoformat(),
        "file_count": len(files_meta),
        "files": files_meta,
    }
    (data_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def build_compacted_equity_bundles(
    data_dir: Path,
    staging_dir: Path,
    manifest: dict,
    old_manifest: dict | None = None,
    remote_sizes: dict[str, int] | None = None,
) -> dict[str, dict]:
    """
    Compacts all completed historical years (< current_year) into a single
    `(Symbol, Date)`-sorted Parquet bundle per exchange (`compacted__nse_cm_hist.parquet`
    and `compacted__bse_cm_hist.parquet`) so multi-decade RemoteLakeClient queries
    only read 2 files (hist + current_year) instead of 32 files.
    """
    eq_dir = data_dir / "equities"
    if not eq_dir.exists():
        return {}

    current_year = today_ist().year
    compacted_meta: dict[str, dict] = {}
    old_files = (old_manifest or {}).get("files", {})
    old_compacted = (old_manifest or {}).get("compacted_files", {})

    for exch in ("nse", "bse"):
        hist_files = []
        for pq in sorted(eq_dir.glob(f"{exch}_cm_*.parquet")):
            yr_str = pq.stem.split("_")[-1]
            if yr_str.isdigit() and int(yr_str) < current_year:
                hist_files.append((int(yr_str), pq))

        if not hist_files:
            continue

        asset_name = f"compacted__{exch}_cm_hist.parquet"
        min_yr, max_yr = hist_files[0][0], hist_files[-1][0]

        # Check if all underlying historical yearly files are unchanged since previous manifest
        hist_unchanged = bool(old_compacted.get(asset_name)) and all(
            old_files.get(p.relative_to(data_dir).as_posix(), {}).get("sha256")
            == manifest["files"].get(p.relative_to(data_dir).as_posix(), {}).get("sha256")
            for _, p in hist_files
        )
        prev_meta = old_compacted.get(asset_name, {})
        on_remote = (
            remote_sizes is not None
            and prev_meta.get("size_bytes") is not None
            and remote_sizes.get(asset_name) == prev_meta.get("size_bytes")
        )

        if hist_unchanged and on_remote:
            compacted_meta[asset_name] = prev_meta
            logger.info("[Release Stage] %s unchanged on remote (%d..%d); skipping rebuild.", asset_name, min_yr, max_yr)
            continue

        logger.info("[Release Stage] Building %s (%d..%d across %d files)...", asset_name, min_yr, max_yr, len(hist_files))
        frames = [read_table_safe(p) for _, p in hist_files]
        frames = [f for f in frames if not f.empty]
        if not frames:
            continue

        combined = pd.concat(frames, ignore_index=True)
        del frames
        sort_cols = ["Symbol", "Date"] if exch == "nse" else ["InstrumentId", "Date"]
        valid_sort = [c for c in sort_cols if c in combined.columns]
        if valid_sort:
            combined = combined.sort_values(valid_sort).reset_index(drop=True)

        dst = staging_dir / asset_name
        write_parquet_atomic(combined, dst, row_group_size=5_000, already_sanitized=True)
        del combined
        gc.collect()

        sz = dst.stat().st_size
        if sz >= 1_950_000_000:
            dst.unlink(missing_ok=True)
            continue

        compacted_meta[asset_name] = {
            "asset_name": asset_name,
            "min_year": min_yr,
            "max_year": max_yr,
            "size_bytes": sz,
            "sha256": compute_sha256(dst),
        }

    return compacted_meta


def _serialize_ticker_parquet(df: pd.DataFrame) -> bytes:
    """Serialize a single ticker's DataFrame into a compact Zstd Parquet byte slice."""
    buf = io.BytesIO()
    df.to_parquet(buf, compression="zstd", index=False)
    return buf.getvalue()


def _pack_frames_by_symbol(
    frames: list[pd.DataFrame],
    alias_to_canon: dict[str, str],
    target_pack_path: Path,
) -> dict[str, list[int]]:
    """
    Concatenates `frames`, maps historical renamed symbols to their canonical symbol,
    serializes each symbol into an independent Parquet byte slice in parallel,
    writes `target_pack_path`, and returns `{symbol: [byte_offset, byte_length]}`.
    """
    if not frames:
        return {}

    combined = sanitize_for_parquet(pd.concat(frames, ignore_index=True))
    del frames
    if combined.empty or "Symbol" not in combined.columns:
        return {}

    raw_sym = combined["Symbol"].astype(str).str.strip().str.upper()
    combined["_CanonSym"] = raw_sym.map(lambda s: alias_to_canon.get(s, s))

    if "Series" in combined.columns:
        series_clean = combined["Series"].astype(str).str.strip().str.upper()
        priority_map = {"EQ": 0, "BE": 1, "BZ": 1, "SM": 2}
        combined["_SeriesPriority"] = series_clean.map(lambda s: priority_map.get(s, 9))
        combined = (
            combined.sort_values(["_CanonSym", "Date", "_SeriesPriority"])
            .drop_duplicates(subset=["_CanonSym", "Date"], keep="first")
            .drop(columns=["_SeriesPriority"])
            .reset_index(drop=True)
        )
    else:
        combined = (
            combined.sort_values(["_CanonSym", "Date"])
            .drop_duplicates(subset=["_CanonSym", "Date"], keep="last")
            .reset_index(drop=True)
        )

    grouped: list[tuple[str, pd.DataFrame]] = [
        (str(sym), grp.drop(columns=["_CanonSym"]).reset_index(drop=True))
        for sym, grp in combined.groupby("_CanonSym", sort=True)
    ]
    del combined

    with ThreadPoolExecutor(max_workers=min(8, (os.cpu_count() or 4))) as pool:
        blobs = list(pool.map(lambda item: _serialize_ticker_parquet(item[1]), grouped))

    offsets: dict[str, list[int]] = {}
    cursor = 0
    target_pack_path.parent.mkdir(parents=True, exist_ok=True)
    with target_pack_path.open("wb") as f:
        for (sym, _), blob in zip(grouped, blobs):
            size = len(blob)
            f.write(blob)
            offsets[sym] = [cursor, size]
            cursor += size

    return offsets


def build_company_dossier_pack(
    data_dir: Path,
    staging_dir: Path,
    alias_to_canon: dict[str, str],
) -> dict[str, list[int]]:
    """
    Packs all 8 non-price datasets (Financials, Shareholding, >1% Holders, Corporate Actions,
    Bulk/Block Deals, Insider Trades, Concalls, Annual Reports) per canonical symbol into
    `by_ticker__nse_dossier.pack` so all 8 tables can be fetched in a single ~25 KB HTTP Range request.
    """
    import zlib

    tables = {
        "financials": ("fundamentals/xbrl_financials.parquet", "Symbol"),
        "shareholding": ("shareholding/nse_shareholding_summary.parquet", "Symbol"),
        "named_holders": ("shareholding/named_shareholders.parquet", "Symbol"),
        "actions": ("actions/nse_corporate_actions.parquet", "symbol"),
        "deals": ("deals/bulk_block_deals.parquet", "Symbol"),
        "insider_trades": ("deals/insider_trading_pit.parquet", "symbol"),
        "annual_reports": ("documents/annual_reports.parquet", "Symbol"),
        "concalls": ("documents/concalls_and_filings.parquet", "CompanyName"),
    }

    by_sym: dict[str, dict[str, list[dict]]] = {}
    for key, (rel_path, sym_col) in tables.items():
        p = data_dir / rel_path
        if not p.exists():
            continue
        df = read_table_safe(p)
        if df.empty or sym_col not in df.columns:
            continue
        for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
            df[col] = df[col].dt.strftime("%Y-%m-%d")
        raw_s = df[sym_col].astype(str).str.strip().str.upper()
        df["_CanonSym"] = raw_s.map(lambda s: alias_to_canon.get(s, s))
        for sym, grp in df.groupby("_CanonSym"):
            sym_str = str(sym).strip().upper()
            if not sym_str or sym_str in ("", "NAN", "NONE"):
                continue
            by_sym.setdefault(sym_str, {})[key] = grp.drop(columns=["_CanonSym"]).to_dict("records")

    if not by_sym:
        return {}

    offsets: dict[str, list[int]] = {}
    cursor = 0
    pack_path = staging_dir / "by_ticker__nse_dossier.pack"
    with pack_path.open("wb") as f:
        for sym in sorted(by_sym.keys()):
            raw_json = json.dumps(by_sym[sym], separators=(",", ":"), default=str).encode("utf-8")
            compressed = zlib.compress(raw_json, level=6)
            f.write(compressed)
            offsets[sym] = [cursor, len(compressed)]
            cursor += len(compressed)
    return offsets


def build_market_screener_snapshot(data_dir: Path, staging_dir: Path) -> dict | None:
    """
    Pre-joins latest EOD technicals with latest Quarterly Financials and Shareholding
    into a single ~400 KB Parquet file.
    """
    eng = UnifiedLakeEngine(data_dir)
    try:
        df = eng.sql("""
            WITH recent_eq AS (
                SELECT
                    Symbol, Date, Close, Volume, TurnoverValue, DeliveryPct,
                    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS SMA_50,
                    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS SMA_200,
                    MAX(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS High_52W,
                    MIN(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS Low_52W,
                    ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY Date DESC) AS rn
                FROM equities_nse
                WHERE Series = 'EQ' AND Date >= CURRENT_DATE - INTERVAL 400 DAY
            ),
            latest_eq AS (
                SELECT * EXCLUDE (rn) FROM recent_eq WHERE rn = 1
            ),
            latest_fin AS (
                SELECT Symbol, PeriodEnd AS FinQuarter, Sales_Cr, PBT_Cr, NetProfit_Cr, BasicEPS_Rs
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY PeriodEnd DESC) AS rn
                    FROM financials WHERE PeriodType = 'Quarterly'
                ) WHERE rn = 1
            ),
            latest_shp AS (
                SELECT Symbol, QuarterEnd AS ShpQuarter, PromoterPct, PromoterPledgedPct, MutualFundsPct, FPI_Pct, RetailPct
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY QuarterEnd DESC) AS rn
                    FROM shareholding
                ) WHERE rn = 1
            )
            SELECT
                e.*,
                f.FinQuarter, f.Sales_Cr, f.PBT_Cr, f.NetProfit_Cr, f.BasicEPS_Rs,
                s.ShpQuarter, s.PromoterPct, s.PromoterPledgedPct, s.MutualFundsPct, s.FPI_Pct, s.RetailPct
            FROM latest_eq e
            LEFT JOIN latest_fin f USING (Symbol)
            LEFT JOIN latest_shp s USING (Symbol)
            ORDER BY e.TurnoverValue DESC NULLS LAST
        """)
    except Exception as exc:
        logger.debug("[Screener Snapshot] Note: %s", exc)
        return None

    if df.empty:
        return None

    asset_name = "snapshots__market_screener_latest.parquet"
    dst = staging_dir / asset_name
    write_parquet_atomic(df, dst, row_group_size=5_000)
    return {
        "asset_name": asset_name,
        "size_bytes": dst.stat().st_size,
        "sha256": compute_sha256(dst),
    }


def build_by_ticker_packs(
    data_dir: Path,
    staging_dir: Path,
    manifest: dict,
    old_manifest: dict | None = None,
    remote_sizes: dict[str, int] | None = None,
) -> dict[str, dict]:
    """
    Builds Two-Tier Per-Ticker Packfiles (`by_ticker__nse_hist.pack`, `by_ticker__nse_curr.pack`,
    and `by_ticker__nse_index.json`) at release staging time.
    - `hist.pack` (< current_year) is only rebuilt if historical yearly partitions changed.
    - `curr.pack` (== current_year) is rebuilt daily in ~1.5s.
    """
    eq_dir = data_dir / "equities"
    if not eq_dir.exists():
        return {}

    current_year = today_ist().year
    resolver = SymbolRenameResolver.from_lake(data_dir)

    alias_to_canon: dict[str, str] = {}
    for sym in list(resolver._back_edges.keys()) + list(resolver._fwd_edges.keys()):
        chain = resolver.all_aliases(sym)
        for al in chain:
            alias_to_canon[al] = chain[0]

    hist_files: list[Path] = []
    curr_files: list[Path] = []
    for pq in sorted(eq_dir.glob("nse_cm_*.parquet")):
        yr_str = pq.stem.split("_")[-1]
        if not yr_str.isdigit():
            continue
        if int(yr_str) < current_year:
            hist_files.append(pq)
        else:
            curr_files.append(pq)

    if not hist_files and not curr_files:
        return {}

    old_files = (old_manifest or {}).get("files", {})
    old_packs = (old_manifest or {}).get("by_ticker_packs", {})

    hist_pack_name = "by_ticker__nse_hist.pack"
    curr_pack_name = "by_ticker__nse_curr.pack"
    index_name = "by_ticker__nse_index.json"

    hist_unchanged = (
        bool(old_packs.get(hist_pack_name))
        and bool(old_packs.get("hist_offsets"))
        and all(
            old_files.get(p.relative_to(data_dir).as_posix(), {}).get("sha256")
            == manifest["files"].get(p.relative_to(data_dir).as_posix(), {}).get("sha256")
            for p in hist_files
        )
    )
    prev_hist_meta = old_packs.get(hist_pack_name, {})
    hist_on_remote = (
        remote_sizes is not None
        and prev_hist_meta.get("size_bytes") is not None
        and remote_sizes.get(hist_pack_name) == prev_hist_meta.get("size_bytes")
    )

    packs_meta: dict[str, dict] = {}
    if hist_unchanged and hist_on_remote:
        logger.info("[By-Ticker Pack] %s unchanged on remote; reusing cached offsets.", hist_pack_name)
        hist_offsets: dict[str, list[int]] = old_packs["hist_offsets"]
        packs_meta[hist_pack_name] = prev_hist_meta
    elif hist_files:
        logger.info("[By-Ticker Pack] Building %s across %d historical years...", hist_pack_name, len(hist_files))
        hist_dst = staging_dir / hist_pack_name
        hist_offsets = _pack_frames_by_symbol(
            [read_table_safe(p) for p in hist_files], alias_to_canon, hist_dst
        )
        if hist_dst.exists():
            packs_meta[hist_pack_name] = {
                "asset_name": hist_pack_name,
                "size_bytes": hist_dst.stat().st_size,
                "sha256": compute_sha256(hist_dst),
            }
    else:
        hist_offsets = {}

    curr_offsets: dict[str, list[int]] = {}
    if curr_files:
        logger.info("[By-Ticker Pack] Building %s for current year %d...", curr_pack_name, current_year)
        curr_dst = staging_dir / curr_pack_name
        curr_offsets = _pack_frames_by_symbol(
            [read_table_safe(p) for p in curr_files], alias_to_canon, curr_dst
        )
        if curr_dst.exists():
            packs_meta[curr_pack_name] = {
                "asset_name": curr_pack_name,
                "size_bytes": curr_dst.stat().st_size,
                "sha256": compute_sha256(curr_dst),
            }

    all_syms = sorted(set(hist_offsets.keys()) | set(curr_offsets.keys()))
    dossier_offsets = build_company_dossier_pack(data_dir, staging_dir, alias_to_canon)
    dossier_pack_name = "by_ticker__nse_dossier.pack"
    if dossier_offsets and (staging_dir / dossier_pack_name).exists():
        dst_dossier = staging_dir / dossier_pack_name
        packs_meta[dossier_pack_name] = {
            "asset_name": dossier_pack_name,
            "size_bytes": dst_dossier.stat().st_size,
            "sha256": compute_sha256(dst_dossier),
        }

    all_syms = sorted(set(all_syms) | set(dossier_offsets.keys()))
    tickers_index: dict[str, dict[str, list[int]]] = {}
    for sym in all_syms:
        entry: dict[str, list[int]] = {}
        if sym in hist_offsets:
            entry["hist"] = hist_offsets[sym]
        if sym in curr_offsets:
            entry["curr"] = curr_offsets[sym]
        if sym in dossier_offsets:
            entry["dossier"] = dossier_offsets[sym]
        tickers_index[sym] = entry

    index_payload = {
        "version": 1,
        "current_year": current_year,
        "aliases": alias_to_canon,
        "tickers": tickers_index,
    }
    index_dst = staging_dir / index_name
    index_dst.write_text(json.dumps(index_payload, separators=(",", ":")), encoding="utf-8")
    packs_meta[index_name] = {
        "asset_name": index_name,
        "size_bytes": index_dst.stat().st_size,
        "sha256": compute_sha256(index_dst),
    }
    packs_meta["hist_offsets"] = hist_offsets
    return packs_meta


def prepare_release_staging(
    data_dir: Path,
    staging_dir: Path,
    previous_manifest_path: Path | None = None,
    tag: str = "datalake-latest",
) -> dict:
    """
    Run by GitHub Actions before `gh release upload`:
      1. Clusters Equity Parquet files by `(Symbol, Date)` for sub-5ms predicate pushdown.
      2. Generates `manifest.json`.
      3. Copies `manifest.json`, `state.db`, and all `folder__file.parquet` assets into `staging_dir`
         (staging only new/modified/missing-on-release files if previous_manifest_path is provided).
    """
    import subprocess

    data_dir = data_dir.resolve()
    staging_dir = staging_dir.resolve()

    old_m: dict = {}
    old_shas: dict[str, str] = {}
    remote_sizes: dict[str, int] | None = None
    if previous_manifest_path and previous_manifest_path.exists():
        try:
            old_m = json.loads(previous_manifest_path.read_text(encoding="utf-8"))
            old_shas = {k: v.get("sha256", "") for k, v in old_m.get("files", {}).items()}
        except Exception:
            pass
        if os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN"):
            try:
                out = subprocess.check_output(
                    ["gh", "release", "view", tag, "--json", "assets"],
                    text=True,
                    stderr=subprocess.DEVNULL,
                )
                assets = json.loads(out).get("assets", [])
                remote_sizes = {
                    a["name"]: int(a.get("size", -1))
                    for a in assets
                    if isinstance(a, dict) and a.get("name")
                }
            except Exception:
                remote_sizes = None

    UnifiedLakeEngine(data_dir).optimize_layout()
    manifest = build_manifest(data_dir)

    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    compacted_meta = build_compacted_equity_bundles(
        data_dir, staging_dir, manifest, old_manifest=old_m, remote_sizes=remote_sizes
    )
    if compacted_meta:
        manifest["compacted_files"] = compacted_meta

    packs_meta = build_by_ticker_packs(
        data_dir, staging_dir, manifest, old_manifest=old_m, remote_sizes=remote_sizes
    )
    if packs_meta:
        manifest["by_ticker_packs"] = packs_meta

    screener_meta = build_market_screener_snapshot(data_dir, staging_dir)
    if screener_meta:
        manifest["snapshots"] = {"market_screener_latest.parquet": screener_meta}

    if compacted_meta or packs_meta or screener_meta:
        (data_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    shutil.copy2(data_dir / "manifest.json", staging_dir / "manifest.json")
    if (data_dir / "state.db").exists():
        shutil.copy2(data_dir / "state.db", staging_dir / "state.db")

    staged_count = 0
    skipped_count = 0
    for rel_path, meta in manifest["files"].items():
        asset_name = meta["asset_name"]
        same_sha = old_shas.get(rel_path) == meta["sha256"]
        on_remote = remote_sizes is None or remote_sizes.get(asset_name) == meta["size_bytes"]
        if same_sha and on_remote:
            skipped_count += 1
            continue
        src = data_dir / rel_path
        dst = staging_dir / asset_name
        if src.exists() and meta["size_bytes"] < 1_950_000_000:
            shutil.copy2(src, dst)
            staged_count += 1

    logger.info(
        "[Release Stage] Staged %d changed/new Parquet assets (%d unchanged skipped, %d total) in %s",
        staged_count, skipped_count, len(manifest["files"]), staging_dir,
    )
    return manifest


def prune_retired_release_assets(
    data_dir: Path,
    tag: str = "datalake-latest",
    staging_dir: Path | None = None,
) -> list[str]:
    """
    Prune retired Parquet assets and stale archive parts (`datalake_store.tar.zst.part-*`
    when single `datalake_store.tar.zst` is used, or vice versa) from a GitHub Release.
    """
    import subprocess

    manifest_path = Path(data_dir).resolve() / "manifest.json"
    if not manifest_path.exists():
        return []

    deleted: list[str] = []
    try:
        manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
        valid = {m["asset_name"] for m in manifest_data.get("files", {}).values() if "asset_name" in m}
        valid.update(
            {m["asset_name"] for m in manifest_data.get("compacted_files", {}).values() if "asset_name" in m}
        )
        valid.update(
            {
                v["asset_name"]
                for v in manifest_data.get("by_ticker_packs", {}).values()
                if isinstance(v, dict) and "asset_name" in v
            }
        )
        valid.update(
            {
                v["asset_name"]
                for v in manifest_data.get("snapshots", {}).values()
                if isinstance(v, dict) and "asset_name" in v
            }
        )
        valid.update({"manifest.json", "state.db"})

        if staging_dir and Path(staging_dir).exists():
            stg = Path(staging_dir)
            staged_parts = {p.name for p in stg.glob("datalake_store.tar.zst.part-*")}
            if staged_parts:
                valid.update(staged_parts)
            else:
                valid.add("datalake_store.tar.zst")
        else:
            valid.add("datalake_store.tar.zst")

        out = subprocess.check_output(["gh", "release", "view", tag, "--json", "assets"], text=True)
        assets = json.loads(out).get("assets", [])
        has_single_archive = "datalake_store.tar.zst" in valid

        for a in assets:
            name = a.get("name", "")
            if not name:
                continue
            if name in valid:
                continue
            if name.startswith("datalake_store.tar.zst.part-") and not has_single_archive and staging_dir is None:
                continue
            logger.info("[Release Prune] Deleting retired release asset: %s", name)
            subprocess.run(["gh", "release", "delete-asset", tag, name, "-y"], check=False)
            deleted.append(name)
    except Exception as exc:
        logger.warning("[Release Prune] Asset pruning note: %s", exc)
    return deleted


async def pull_from_github_release(
    repo: str,
    data_dir: Path | str = "./data",
    tag: str = "datalake-latest",
    token: str | None = None,
    concurrency: int = 4,
    pattern: str | None = None,
) -> dict[str, int]:
    """
    Smart delta sync from `https://github.com/{repo}/releases/download/{tag}/manifest.json`.
    Only downloads `.parquet` files whose SHA-256 differs from local disk!
    Supports both public repos (zero auth) and private repos (via `GITHUB_TOKEN`).
    Optionally filters downloads by substring `pattern` (e.g. '2026' or 'equities').
    """
    target_root = Path(data_dir).resolve()
    target_root.mkdir(parents=True, exist_ok=True)

    auth_token = token or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    headers = dict(ARCHIVE_HEADERS)
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    base_url = f"https://github.com/{repo.strip('/')}/releases/download/{tag}"
    manifest_url = f"{base_url}/manifest.json"

    async with ResilientHTTPClient() as http:
        resp = await http.get(manifest_url, headers=headers)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Could not fetch manifest from {manifest_url} (HTTP {resp.status_code}). "
                "Ensure the GitHub Action has published the `datalake-latest` release."
            )
        manifest = resp.json()

        files_map: dict[str, dict] = manifest.get("files", {})
        to_download: list[tuple[str, dict]] = []
        skipped = 0

        for rel_path, meta in files_map.items():
            if pattern:
                import re
                if not re.search(pattern, rel_path, re.IGNORECASE) and not re.search(pattern, meta.get("asset_name", ""), re.IGNORECASE):
                    continue
            local_file = target_root / rel_path
            if local_file.exists() and local_file.stat().st_size == meta["size_bytes"]:
                if compute_sha256(local_file) == meta["sha256"]:
                    skipped += 1
                    continue
            to_download.append((rel_path, meta))

        if pattern is None and files_map:
            valid_rel_paths = set(files_map.keys())
            for existing_pq in list(target_root.rglob("*.parquet")):
                rel_existing = existing_pq.relative_to(target_root).as_posix()
                if rel_existing not in valid_rel_paths:
                    logger.info("[Lake Pull] Removing retired local partition: %s", rel_existing)
                    existing_pq.unlink(missing_ok=True)

        logger.info(
            "[Lake Pull] %d files up-to-date locally, %d changed/new files to download from %s...",
            skipped, len(to_download), repo,
        )

        sem = asyncio.Semaphore(concurrency)
        downloaded = 0

        async def _dl(rel_path: str, meta: dict):
            nonlocal downloaded
            asset_url = f"{base_url}/{meta['asset_name']}"
            local_path = target_root / rel_path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = local_path.with_name(f"{local_path.name}.dl.tmp")
            
            for dl_attempt in range(4):
                try:
                    async with sem:
                        if http.client is not None:
                            async with http.client.stream("GET", asset_url, headers=headers) as r:
                                r.raise_for_status()
                                h = hashlib.sha256()
                                sz = 0
                                with tmp_path.open("wb") as f:
                                    async for chunk in r.aiter_bytes(chunk_size=512 * 1024):
                                        f.write(chunk)
                                        h.update(chunk)
                                        sz += len(chunk)
                                computed_sha = h.hexdigest()
                        else:
                            r = await http.get(asset_url, headers=headers)
                            r.raise_for_status()
                            tmp_path.write_bytes(r.content)
                            computed_sha = compute_sha256(tmp_path)
                            sz = len(r.content)

                        if computed_sha != meta["sha256"]:
                            tmp_path.unlink(missing_ok=True)
                            raise RuntimeError(f"SHA-256 mismatch on downloaded asset {meta['asset_name']}")
                        
                        # Windows-safe atomic replacement with retry
                        for attempt in range(5):
                            try:
                                os.replace(tmp_path, local_path)
                                break
                            except PermissionError:
                                if attempt == 4:
                                    raise
                                gc.collect()
                                time.sleep(0.1 * (attempt + 1))
                        
                        downloaded += 1
                        logger.info("[Lake Pull] Synced %s (%d KB)", rel_path, sz // 1024)
                        return
                except Exception as exc:
                    tmp_path.unlink(missing_ok=True)
                    if dl_attempt == 3:
                        logger.error("[Lake Pull] Failed to download %s after 4 attempts: %s", rel_path, exc)
                        raise
                    logger.warning("[Lake Pull] Retry %d for %s due to: %s", dl_attempt + 1, rel_path, exc)
                    await asyncio.sleep(1.0 * (2 ** dl_attempt))

        if to_download:
            await asyncio.gather(*[_dl(rp, m) for rp, m in to_download])

        (target_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return {"up_to_date_skipped": skipped, "downloaded": downloaded, "total_files": len(files_map)}
