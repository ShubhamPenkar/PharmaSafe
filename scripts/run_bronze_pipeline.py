"""
PharmaSafe Bronze runner: ingests official FAERS quarters registered in data/raw/manifest.json.
Every archive must pass the provenance gate (VERIFIED_OFFICIAL, no synthetic markers, SHA-256 match).

    python scripts/run_bronze_pipeline.py                     # all manifest quarters
    python scripts/run_bronze_pipeline.py --quarters 2024Q3   # pilot
    python scripts/run_bronze_pipeline.py --force             # re-ingest completed quarters
"""

import sys
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_raw_manifest
from src.ingestion.bronze_pipeline import BronzePipeline

logger = get_logger("PharmaSafe.BronzeRunner")


def verified_archives(quarters: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    """Returns manifest entries for the requested quarters after the provenance gate."""
    archives = load_raw_manifest().get("archives", {})
    selected = [q.upper() for q in quarters] if quarters else sorted(archives)
    gated = {}
    for q in selected:
        meta = archives.get(q)
        if meta is None:
            raise ValueError(f"Quarter {q} is not registered in data/raw/manifest.json")
        if meta.get("verification_status") != "VERIFIED_OFFICIAL":
            raise ValueError(f"Quarter {q} failed provenance check: {meta.get('verification_status')}")
        if meta.get("synthetic_markers_detected"):
            raise ValueError(f"Quarter {q} contains synthetic markers; ingestion halted.")
        if not meta["resolved_path"].exists():
            raise FileNotFoundError(
                f"Quarter {q} archive missing at {meta['resolved_path']}. "
                f"Run scripts/acquire_and_verify_official_data.py first."
            )
        gated[q] = meta
    return gated


def run_bronze(quarters: Optional[List[str]] = None, force: bool = False, spark=None) -> Dict[str, Any]:
    archives = verified_archives(quarters)
    pipeline = BronzePipeline(spark=spark)
    summaries = {}
    for q, meta in archives.items():
        summaries[q] = pipeline.ingest_archive(
            meta["resolved_path"], quarter_id=q, force=force, allow_synthetic=False,
            expected_sha256=meta.get("sha256_checksum"),
        )
    total = sum(s["total_valid_rows"] for s in summaries.values())
    logger.info("Bronze complete for %d quarter(s): %d rows", len(summaries), total)
    return summaries


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--quarters", nargs="*", help="Quarter IDs such as 2024Q3 (default: all in manifest)")
    parser.add_argument("--force", action="store_true", help="Re-ingest quarters that already have a summary")
    args = parser.parse_args()
    run_bronze(args.quarters, force=args.force)
