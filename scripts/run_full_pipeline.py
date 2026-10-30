"""
PharmaSafe end-to-end runner: Bronze -> Silver -> Gold -> temporal -> combinations -> validation -> quality report,
on one Spark session, recording measured stage timings in reports/performance_stages.json.

    python scripts/run_full_pipeline.py                      # official quarters from the manifest
    python scripts/run_full_pipeline.py --quarters 2024Q3    # pilot on one quarter
    python scripts/run_full_pipeline.py --synthetic          # isolated synthetic fixtures (data/synthetic), no reports written
    python scripts/run_full_pipeline.py --asof 2025Q2        # also build a historical Gold snapshot
"""

import sys
import json
import time
import argparse
from pathlib import Path
from typing import Callable, Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_bronze_pipeline import run_bronze
from src.utils.logger import get_logger
from src.utils.config_loader import load_data_sources_config, load_analysis_config, get_project_root
from src.utils.spark_session import get_spark_session, stop_spark_session
from src.ingestion.bronze_pipeline import BronzePipeline
from src.preprocessing.silver_pipeline import SilverPipeline
from src.analytics.gold_pipeline import GoldPipeline
from src.analytics.temporal_analytics import TemporalAnalytics
from src.analytics.interaction_analytics import InteractionAnalytics
from src.validation.benchmark_evaluator import SignalValidator
from src.validation.quality_checks import DataQualityAuditor

logger = get_logger("PharmaSafe.FullPipeline")


def synthetic_config() -> dict:
    """Redirects every layer into data/synthetic so fixtures can never reach official outputs."""
    config = load_data_sources_config()
    synth = get_project_root() / "data/synthetic"
    config["storage"] = {**config["storage"], "bronze_dir": str(synth / "bronze"),
                         "silver_dir": str(synth / "silver"), "gold_dir": str(synth / "gold")}
    return config


class StageLog:
    """Collects measured wall-clock time and row counts per pipeline stage."""

    def __init__(self) -> None:
        self.stages: List[Dict[str, Any]] = []

    def add(self, stage: str, seconds: float, input_rows=None, output_rows=None) -> None:
        self.stages.append({"stage": stage, "seconds": round(seconds, 2), "input_rows": input_rows, "output_rows": output_rows})

    def timed(self, stage: str, fn: Callable[[], Any], rows: Callable[[Any], tuple] = lambda r: (None, None)) -> Any:
        start = time.perf_counter()
        result = fn()
        i, o = rows(result)
        self.add(stage, time.perf_counter() - start, i, o)
        return result


def main(quarters=None, synthetic=False, force=False, asof=None, validate=True):
    log = StageLog()
    spark = get_spark_session(app_name="PharmaSafe.Pipeline")
    try:
        if synthetic:
            config = synthetic_config()
            fixtures = get_project_root() / "data/synthetic/fixtures"
            bronze = BronzePipeline(config=config, spark=spark)
            summaries = {}
            for zip_path in sorted(fixtures.glob("faers_ascii_*.zip")):
                q = zip_path.stem.split("_")[-1].upper()
                summaries[q] = bronze.ingest_archive(zip_path, quarter_id=q, force=True, allow_synthetic=True)
        else:
            config = None
            summaries = run_bronze(quarters, force=force, spark=spark)

        for q, s in sorted(summaries.items()):
            tables = [t for t in s["tables"].values() if t.get("status") == "INGESTED"]
            log.add(f"Bronze {q} (7 tables + deleted list)", s.get("duration_seconds", 0),
                    sum(t.get("source_records", t.get("source_nonblank_lines", 0)) for t in tables),
                    sum(t["rows_ingested"] for t in tables))

        silver = SilverPipeline(config=config, spark=spark).run(quarters if not synthetic else None)
        st = silver["tables"]
        log.add("Silver (all quarters, 7 tables)", silver["duration_seconds"], sum(m.get("raw_rows", 0) for m in st.values()),
                st["DEMO"]["retained_report_versions"] + sum(m.get("retained_rows", 0) for t, m in st.items() if t != "DEMO"))

        gold = GoldPipeline(data_config=config, spark=spark)
        analysis = load_analysis_config()
        gold_metrics = []
        for q in (asof or [None]):
            m = gold.run(asof_quarter=q)
            gold_metrics.append(m)
            log.add(f"Gold marts + signals ({m['snapshot']})", m["duration_seconds"], m["case_drug_rows"] + m["case_reaction_rows"],
                    sum(s["pairs"] for s in m["signals"].values()))

        temporal = TemporalAnalytics(config, analysis, spark)
        combos = InteractionAnalytics(config, analysis, spark)
        log.timed("Temporal analytics (latest)", lambda: temporal.run("latest"), lambda r: (None, r["pair_quarters"]))
        log.timed("Drug-combination analytics (latest)", lambda: combos.run("latest"), lambda r: (None, r["combination_signals"]))

        if not synthetic:
            if validate:
                log.timed("Validation (references, robustness, temporal)", lambda: SignalValidator(config, analysis, spark).run("latest"))
            DataQualityAuditor(config).generate_report()
            out = get_project_root() / "reports/performance_stages.json"
            out.write_text(json.dumps({"stages": log.stages}, indent=2), encoding="utf-8")

        logger.info("Pipeline complete. Stage timings: %s", json.dumps(log.stages))
        return log.stages
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--quarters", nargs="*", help="Quarter IDs (default: all in manifest)")
    parser.add_argument("--synthetic", action="store_true", help="Run on isolated synthetic fixtures")
    parser.add_argument("--force", action="store_true", help="Re-ingest Bronze quarters")
    parser.add_argument("--asof", nargs="*", help="Also build historical Gold snapshots for these quarters")
    parser.add_argument("--skip-validation", action="store_true")
    args = parser.parse_args()
    main(args.quarters, synthetic=args.synthetic, force=args.force, asof=[None] + (args.asof or []), validate=not args.skip_validation)
