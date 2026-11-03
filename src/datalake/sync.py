from __future__ import annotations

import asyncio
import gc
import hashlib
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
from datalake.storage import read_table_safe, write_parquet_atomic

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
