import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd

from datalake.engine import UnifiedLakeEngine
from datalake.sync import (
    build_manifest,
    compute_sha256,
    prepare_release_staging,
    prune_retired_release_assets,
    pull_from_github_release,
)


class TestSyncEngine(unittest.TestCase):
    def test_compute_sha256(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"Hello BFinance Data Lake")
            f_path = Path(f.name)
        try:
            h = compute_sha256(f_path)
            # sha256 of "Hello BFinance Data Lake"
            self.assertEqual(len(h), 64)
        finally:
            f_path.unlink(missing_ok=True)

    def test_build_manifest_and_staging(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)

            eq_dir = src / "equities"
            eq_dir.mkdir(parents=True)
            df = pd.DataFrame({"Symbol": ["TCS"], "Date": pd.to_datetime(["2026-01-01"]), "Close": [3500.0]})
            pq_file = eq_dir / "nse_cm_2026.parquet"
            df.to_parquet(pq_file)

            (src / "state.db").write_text("sqlite-mock")

            manifest = build_manifest(src)
            self.assertEqual(manifest["version"], 1)
            self.assertEqual(manifest["file_count"], 1)
            rel_key = "equities/nse_cm_2026.parquet"
            self.assertIn(rel_key, manifest["files"])
            self.assertEqual(manifest["files"][rel_key]["asset_name"], "equities__nse_cm_2026.parquet")

            staged = prepare_release_staging(src, stg)
            self.assertIn("manifest.json", [p.name for p in stg.glob("*")])
            self.assertIn("state.db", [p.name for p in stg.glob("*")])
            self.assertIn("equities__nse_cm_2026.parquet", [p.name for p in stg.glob("*")])

    def test_prune_retired_release_assets(self):
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as stg_dir:
            src = Path(src_dir)
            stg = Path(stg_dir)
            eq_dir = src / "equities"
            eq_dir.mkdir(parents=True)
            df = pd.DataFrame({"Symbol": ["TCS"], "Date": pd.to_datetime(["2026-01-01"]), "Close": [3500.0]})
            df.to_parquet(eq_dir / "nse_cm_2026.parquet")
            build_manifest(src)
            (stg / "datalake_store.tar.zst").write_bytes(b"archive")

            fake_gh_assets = json.dumps({
                "assets": [
                    {"name": "manifest.json"},
                    {"name": "state.db"},
                    {"name": "datalake_store.tar.zst"},
                    {"name": "datalake_store.tar.zst.part-00"},
                    {"name": "equities__nse_cm_2026.parquet"},
                    {"name": "retired__old_file.parquet"},
                ]
            })
            with patch("subprocess.check_output", return_value=fake_gh_assets), patch("subprocess.run") as mock_run:
                deleted = prune_retired_release_assets(src, tag="datalake-latest", staging_dir=stg)
                self.assertEqual(set(deleted), {"datalake_store.tar.zst.part-00", "retired__old_file.parquet"})
                self.assertEqual(mock_run.call_count, 2)

                # Also verify CLI entry point executes prune-release and stage-release without NameError
                from datalake.cli import main as cli_main
                with patch(
                    "sys.argv",
                    ["datalake", "--data-dir", str(src), "prune-release", "--tag", "datalake-latest", "--staging-dir", str(stg)],
                ):
                    cli_main()

                prev_m = src / "prev_manifest.json"
                prev_m.write_text((src / "manifest.json").read_text(encoding="utf-8"), encoding="utf-8")
                with patch(
                    "sys.argv",
                    ["datalake", "--data-dir", str(src), "stage-release", "--staging-dir", str(stg), "--prev-manifest", str(prev_m)],
                ):
                    cli_main()

    def test_pull_delta_skipping(self):
        with tempfile.TemporaryDirectory() as target_dir:
            target = Path(target_dir)
            eq_dir = target / "equities"
            eq_dir.mkdir(parents=True)

            df = pd.DataFrame({"Symbol": ["INFY"], "Date": pd.to_datetime(["2026-01-01"]), "Close": [1800.0]})
            local_pq = eq_dir / "nse_cm_2026.parquet"
            df.to_parquet(local_pq)

            sha = compute_sha256(local_pq)
            sz = local_pq.stat().st_size

            mock_manifest = {
                "version": 1,
                "file_count": 1,
                "files": {
                    "equities/nse_cm_2026.parquet": {
                        "asset_name": "equities__nse_cm_2026.parquet",
                        "size_bytes": sz,
                        "sha256": sha,
                    }
                },
            }

            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = mock_manifest

            with patch("datalake.sync.ResilientHTTPClient") as mock_client_cls:
                mock_client = AsyncMock()
                mock_client.get.return_value = mock_resp
                mock_client_cls.return_value.__aenter__.return_value = mock_client

                stats = asyncio.run(pull_from_github_release("user/repo", data_dir=target))
                self.assertEqual(stats["up_to_date_skipped"], 1)
                self.assertEqual(stats["downloaded"], 0)

    def test_engine_from_github_factory(self):
        with tempfile.TemporaryDirectory() as target_dir:
            eng = UnifiedLakeEngine.from_github("user/repo", lake_dir=target_dir, auto_sync=False)
            self.assertIsInstance(eng, UnifiedLakeEngine)
            self.assertEqual(str(eng.root), str(Path(target_dir).resolve()))


if __name__ == "__main__":
    unittest.main()
