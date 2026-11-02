from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
TESTS_DIR = ROOT_DIR / "tests"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))


def main() -> None:
    print("=== bfinance-datalake Verification & Self-Healing Suite ===")
    import test_datalake_offline

    print("\n[1/9] Running corporate action regex parser tests...")
    test_datalake_offline.test_corporate_action_regex_parser()
    print("  -> PASSED")

    print("[2/9] Running OHLC repair and BSE debt filter tests...")
    test_datalake_offline.test_repair_ohlc_and_bse_debt_filter()
    print("  -> PASSED")

    print("[3/9] Running Ind-AS financial & SHP XBRL parser tests...")
    test_datalake_offline.test_xbrl_parsers()
    print("  -> PASSED")

    print("[4/9] Running LakeDoctor self-healing & OHLCVAdjuster tests...")
    test_datalake_offline.test_doctor_self_healing_and_adjuster()
    print("  -> PASSED")

    print("[5/9] Running Parquet sanitization & SplitFromFV float64 upsert tests...")
    test_datalake_offline.test_sanitize_for_parquet_mixed_types()
    print("  -> PASSED")

    print("[6/9] Running Bulk/Block deals frame normalization tests...")
    test_datalake_offline.test_normalize_deals_frame()
    print("  -> PASSED")

    print("[7/9] Running 2000-2001 NSE F&O int64/float64 repair & Doctor preservation tests...")
    test_datalake_offline.test_fo_2000_int64_ohlc_and_doctor_preservation()
    print("  -> PASSED")

    print("[8/9] Running BSE Concalls & Filings 28-day window chunking tests...")
    test_datalake_offline.test_bse_concalls_chunked_windows()
    print("  -> PASSED")

    print("[9/11] Running bug fix audit verification suite...")
    test_datalake_offline.test_bug_fix_2_audit_suite()
    print("  -> PASSED")

    print("[10/11] Running Pass 1 (P1-01 to P1-07) verification suite...")
    test_datalake_offline.test_pass_1_audit_suite()
    print("  -> PASSED")

    print("[11/11] Running storage layout & cloud sync unit test suites...")
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromName("test_remote"))
    suite.addTests(loader.loadTestsFromName("test_sync"))
    runner = unittest.TextTestRunner(stream=sys.stdout, verbosity=2)
    result = runner.run(suite)
    if not result.wasSuccessful():
        sys.exit(1)

    print("\nALL BFINANCE-DATALAKE VERIFICATION TESTS PASSED!")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
