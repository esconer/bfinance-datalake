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

from datalake.engine import UnifiedLakeEngine
from datalake.http_client import ARCHIVE_HEADERS, ResilientHTTPClient

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
        "file_count": len(files_meta),
        "files": files_meta,
    }
    (data_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def prepare_release_staging(data_dir: Path, staging_dir: Path) -> dict:
    """
    Run by GitHub Actions before `gh release upload`:
      1. Clusters Equity Parquet files by `(Symbol, Date)` for sub-5ms predicate pushdown.
      2. Generates `manifest.json`.
      3. Copies `manifest.json`, `state.db`, and all `folder__file.parquet` assets into `staging_dir`.
    """
    data_dir = data_dir.resolve()
    staging_dir = staging_dir.resolve()

    UnifiedLakeEngine(data_dir).optimize_layout()
    manifest = build_manifest(data_dir)

    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy2(data_dir / "manifest.json", staging_dir / "manifest.json")
    if (data_dir / "state.db").exists():
        shutil.copy2(data_dir / "state.db", staging_dir / "state.db")

    for rel_path, meta in manifest["files"].items():
        src = data_dir / rel_path
        dst = staging_dir / meta["asset_name"]
        if src.exists() and meta["size_bytes"] < 1_950_000_000:
            shutil.copy2(src, dst)

    logger.info("[Release Stage] Prepared %d Parquet assets in %s", len(manifest["files"]), staging_dir)
    return manifest


async def pull_from_github_release(
    repo: str,
    data_dir: Path | str = "./data",
    tag: str = "datalake-latest",
    token: str | None = None,
    concurrency: int = 4,
) -> dict[str, int]:
    """
    Smart delta sync from `https://github.com/{repo}/releases/download/{tag}/manifest.json`.
    Only downloads `.parquet` files whose SHA-256 differs from local disk!
    Supports both public repos (zero auth) and private repos (via `GITHUB_TOKEN`).
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
            local_file = target_root / rel_path
            if local_file.exists() and local_file.stat().st_size == meta["size_bytes"]:
                if compute_sha256(local_file) == meta["sha256"]:
                    skipped += 1
                    continue
            to_download.append((rel_path, meta))

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
            tmp_path = local_path.with_name(f"{local_path.name}.dl_tmp")
            async with sem:
                r = await http.get(asset_url, headers=headers)
                r.raise_for_status()
                tmp_path.write_bytes(r.content)
                if compute_sha256(tmp_path) != meta["sha256"]:
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
                logger.info("[Lake Pull] Synced %s (%d KB)", rel_path, len(r.content) // 1024)

        if to_download:
            await asyncio.gather(*[_dl(rp, m) for rp, m in to_download])

        (target_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return {"up_to_date_skipped": skipped, "downloaded": downloaded, "total_files": len(files_map)}
