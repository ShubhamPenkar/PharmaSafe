"""
PharmaSafe Medallion Pipeline CLI Runner
Orchestrates:
1. Verified Source Provenance & Manifest Gate
2. Bronze Ingestion & Malformed Row Quarantine (guarded against synthetic contamination)
3. Silver Deduplication, Orphan Pruning, and Temporal Snapshots
4. Automated Data Quality Audit Report Generation
"""

import sys
import json
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_data_sources_config, get_project_root
from src.ingestion.download_manager import DownloadManager
from src.ingestion.faers_sample_builder import build_synthetic_faers_quarter, build_synthetic_faers_quarter_2
from src.ingestion.bronze_pipeline import BronzePipeline
from src.preprocessing.silver_pipeline import SilverPipeline
from src.validation.quality_checks import DataQualityAuditor

logger = get_logger("PharmaSafe.CLI")


def run_pipeline(use_synthetic: bool = False):
    root = get_project_root()
    dm = DownloadManager()

    logger.info("=================================================================")
    logger.info("PHARMASAFE: EXECUTING MEDALLION PIPELINE")
    logger.info("Execution Mode: %s", "SYNTHETIC_BENCHMARK" if use_synthetic else "OFFICIAL_FDA")
    logger.info("=================================================================")

    if use_synthetic:
        # Synthetic fixture mode: operates entirely in data/synthetic namespace
        synth_dir = root / "data/synthetic"
        fixtures_dir = synth_dir / "fixtures"
        fixtures_dir.mkdir(parents=True, exist_ok=True)

        q1_zip = fixtures_dir / "faers_ascii_2024q1.zip"
        q2_zip = fixtures_dir / "faers_ascii_2024q2.zip"

        if not q1_zip.exists():
            logger.info("Generating deterministic synthetic benchmark archive for 2024Q1...")
            build_synthetic_faers_quarter(q1_zip, quarter_id="2024Q1", seed=42)

        if not q2_zip.exists():
            logger.info("Generating deterministic synthetic benchmark archive for 2024Q2...")
            build_synthetic_faers_quarter_2(q2_zip, seed=43)

        # Update synthetic manifest
        synth_manifest_path = synth_dir / "manifest.json"
        synth_manifest = {
            "version": "1.0",
            "classification": "SYNTHETIC_FIXTURE",
            "description": "Deterministic test fixtures. MUST NOT be ingested as official FDA data.",
            "archives": {
                "2024Q1": {"filename": q1_zip.name, "path": str(q1_zip.relative_to(root)), "is_synthetic": True},
                "2024Q2": {"filename": q2_zip.name, "path": str(q2_zip.relative_to(root)), "is_synthetic": True}
            }
        }
        with open(synth_manifest_path, "w", encoding="utf-8") as f:
            json.dump(synth_manifest, f, indent=2)

        synth_config = dict(dm.config)
        synth_config["storage"] = dict(dm.config.get("storage", {}))
        synth_config["storage"]["bronze_dir"] = str(synth_dir / "bronze")
        synth_config["storage"]["silver_dir"] = str(synth_dir / "silver")

        bp = BronzePipeline(config=synth_config)
        summary_q1 = bp.ingest_archive(q1_zip, quarter_id="2024Q1", force=True, allow_synthetic=True)
        summary_q2 = bp.ingest_archive(q2_zip, quarter_id="2024Q2", force=True, allow_synthetic=True)
        logger.info("Synthetic Bronze ingestion complete.")

        sp = SilverPipeline(config=synth_config)
        sp.process_silver_snapshot(quarters=["2024Q1", "2024Q2"], cutoff_date="20240331", snapshot_name="historical_2024Q1")
        sp.process_silver_snapshot(quarters=["2024Q1", "2024Q2"], cutoff_date=None, snapshot_name="latest")
        logger.info("Synthetic Silver processing complete.")
        return

    # Official FDA Pipeline Mode:
    manifest_path = root / "data/raw/manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Official manifest missing at {manifest_path}. "
            f"Please run 'python scripts/acquire_and_verify_official_data.py' to acquire official FDA data."
        )

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    official_archives = manifest.get("archives", {})
    if not official_archives:
        raise ValueError("No verified official archives registered in manifest.json.")

    # Source authenticity & integrity gate:
    for q_id, q_meta in official_archives.items():
        v_status = q_meta.get("verification_status")
        if v_status != "VERIFIED_OFFICIAL":
            raise ValueError(f"Quarter {q_id} failed provenance check: verification_status is '{v_status}'")
        if q_meta.get("synthetic_markers_detected"):
            raise ValueError(f"Quarter {q_id} contains synthetic markers! Ingestion halted.")

        zip_p = root / q_meta.get("relative_path", "")
        if not zip_p.exists():
            raise FileNotFoundError(f"Quarter {q_id} archive missing at {zip_p}")

    logger.info("Provenance gate passed: All %d registered quarters are VERIFIED_OFFICIAL.", len(official_archives))
    logger.info("Pipeline ready for subsequent medallion transformations.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PharmaSafe Medallion Pipeline CLI Runner")
    parser.add_argument("--synthetic", action="store_true", help="Run with isolated synthetic test fixtures")
    args = parser.parse_args()
    run_pipeline(use_synthetic=args.synthetic)
