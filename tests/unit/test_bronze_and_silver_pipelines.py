"""
Integration & Unit Tests for Bronze & Silver Medallion Pipelines
Validates:
- Synthetic FAERS quarterly archive generation
- Dollar-delimited parsing and malformed row quarantine
- Deterministic case deduplication, tie-breakers, and exact duplicate removal
- Orphan record quarantining
- CRITICAL TEMPORAL REQUIREMENT: Historical quarterly cutoff vs latest-version snapshot
- Idempotent execution
- Data Quality Report compilation
"""

import sys
import json
from pathlib import Path
import pytest
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.faers_sample_builder import (
    build_synthetic_faers_quarter,
    build_synthetic_faers_quarter_2
)
from src.ingestion.bronze_pipeline import BronzePipeline
from src.preprocessing.silver_pipeline import SilverPipeline
from src.validation.quality_checks import DataQualityAuditor


@pytest.fixture(scope="module")
def pipeline_env(tmp_path_factory):
    """Sets up an isolated directory structure for Bronze & Silver pipeline tests."""
    temp_dir = tmp_path_factory.mktemp("pharmasafe_pipeline")
    raw_dir = temp_dir / "data/raw"
    bronze_dir = temp_dir / "data/bronze"
    silver_dir = temp_dir / "data/silver"
    reports_dir = temp_dir / "reports"

    raw_dir.mkdir(parents=True, exist_ok=True)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "storage": {
            "base_dir": str(temp_dir / "data"),
            "raw_dir": str(raw_dir),
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
            "manifest_path": str(raw_dir / "manifest.json")
        },
        "faers_source": {
            "delimiter": "$"
        },
        "tables": {
            "DEMO": {"columns": {"primaryid": "str", "caseid": "str", "caseversion": "int", "fda_dt": "str"}},
            "DRUG": {"columns": {"primaryid": "str", "drugname": "str", "prod_ai": "str", "role_cod": "str"}},
            "REAC": {"columns": {"primaryid": "str", "pt": "str"}},
            "OUTC": {"columns": {"primaryid": "str", "outc_cod": "str"}},
            "INDI": {"columns": {"primaryid": "str", "indi_pt": "str"}},
            "THER": {"columns": {"primaryid": "str", "start_dt": "str"}},
            "RPSR": {"columns": {"primaryid": "str", "rpsr_cod": "str"}}
        }
    }

    # Generate synthetic Q1 and Q2 archives
    zip_q1 = raw_dir / "faers_ascii_2024q1.zip"
    meta_q1 = build_synthetic_faers_quarter(zip_q1, quarter_id="2024Q1", seed=42)

    zip_q2 = raw_dir / "faers_ascii_2024q2.zip"
    meta_q2 = build_synthetic_faers_quarter_2(zip_q2, seed=43)

    return {
        "root": temp_dir,
        "config": config,
        "zip_q1": zip_q1,
        "zip_q2": zip_q2,
        "meta_q1": meta_q1,
        "meta_q2": meta_q2
    }


def test_bronze_ingestion_and_quarantine(pipeline_env):
    """
    Test Bronze ingestion of dollar-delimited ASCII files:
    - Asserts schema conformance
    - Asserts malformed row with wrong column count is quarantined
    - Asserts idempotent re-execution
    """
    cfg = pipeline_env["config"]
    bp = BronzePipeline(config=cfg)

    # Ingest Q1
    summary = bp.ingest_archive(pipeline_env["zip_q1"], quarter_id="2024Q1")
    assert summary["quarter"] == "2024Q1"
    assert "DEMO" in summary["tables"]
    demo_meta = summary["tables"]["DEMO"]
    assert demo_meta["status"] == "INGESTED"
    # Exactly 1 malformed row planted in DEMO
    assert demo_meta["rows_quarantined"] == 1

    # Verify quarantined file exists
    quarantine_file = Path(cfg["storage"]["bronze_dir"]) / "2024Q1/quarantine/demo_malformed.json"
    assert quarantine_file.exists()
    with open(quarantine_file, "r") as f:
        q_records = json.load(f)
    assert len(q_records) == 1
    assert "COLUMN_COUNT_MISMATCH" in q_records[0]["reason"]

    # Test idempotency: re-running without force should return cached summary
    summary_cached = bp.ingest_archive(pipeline_env["zip_q1"], quarter_id="2024Q1", force=False)
    assert summary_cached["ingestion_timestamp"] == summary["ingestion_timestamp"]


def test_silver_deduplication_and_orphan_quarantine(pipeline_env):
    """
    Test Silver layer deduplication and orphan removal on Q1:
    - Case 1001: v2 (primaryid 10012) beats v1 (10011)
    - Case 1002: Same date, v2 (10022) beats v1 (10021)
    - Case 1003: Exact duplicate row removed
    - Orphan primaryid 99999 quarantined in DRUG, REAC, and OUTC
    """
    cfg = pipeline_env["config"]
    sp = SilverPipeline(config=cfg)

    metrics = sp.process_silver_snapshot(quarters=["2024Q1"], snapshot_name="q1_snapshot")
    assert metrics["snapshot_name"] == "q1_snapshot"

    demo_m = metrics["demo"]
    assert demo_m["canonical_case_count"] == 46
    assert demo_m["exact_duplicate_rows"] == 1
    assert demo_m["superseded_versions_count"] == 2  # 10011 and 10021

    # Read Silver canonical DEMO
    silver_demo_path = Path(cfg["storage"]["silver_dir"]) / "q1_snapshot/demo.parquet"
    df_demo = pd.read_parquet(silver_demo_path)
    assert len(df_demo) == 46

    # Verify winning versions
    c1001 = df_demo[df_demo["caseid"] == "1001"].iloc[0]
    assert c1001["primaryid"] == "10012"
    assert int(c1001["caseversion"]) == 2

    c1002 = df_demo[df_demo["caseid"] == "1002"].iloc[0]
    assert c1002["primaryid"] == "10022"
    assert int(c1002["caseversion"]) == 2

    # Verify orphan quarantine in DRUG and REAC
    quarantine_drug = Path(cfg["storage"]["silver_dir"]) / "quarantine/q1_snapshot/drug_orphans.parquet"
    assert quarantine_drug.exists()
    df_orphan_drug = pd.read_parquet(quarantine_drug)
    # 99999 is in quarantine, plus superseded primaryids 10011 and 10021
    assert "99999" in df_orphan_drug["primaryid"].values
    assert "10011" in df_orphan_drug["primaryid"].values

    quarantine_reac = Path(cfg["storage"]["silver_dir"]) / "quarantine/q1_snapshot/reac_orphans.parquet"
    assert quarantine_reac.exists()
    df_orphan_reac = pd.read_parquet(quarantine_reac)
    assert "99999" in df_orphan_reac["primaryid"].values


def test_critical_temporal_historical_cutoff(pipeline_env):
    """
    CRITICAL TEMPORAL REQUIREMENT TEST:
    Case 1006 has Version 1 received in Q1 (20240215).
    In Q2, Case 1006 receives a follow-up Version 2 (20240510).
    - When evaluated under Q1 historical cutoff (fda_dt <= 20240331):
      Case 1006 must retain Version 1 (primaryid 10061).
    - When evaluated under full period (including Q2):
      Case 1006 must retain Version 2 (primaryid 10062).
    - Asserts that historical Q1 snapshot was NOT mutated or rewritten by the later follow-up.
    """
    cfg = pipeline_env["config"]
    bp = BronzePipeline(config=cfg)
    sp = SilverPipeline(config=cfg)

    # Ingest Q2 archive into Bronze
    bp.ingest_archive(pipeline_env["zip_q2"], quarter_id="2024Q2")

    # 1. Generate Historical Q1 Snapshot (cutoff 2024-03-31)
    sp.process_silver_snapshot(
        quarters=["2024Q1", "2024Q2"],
        cutoff_date="20240331",
        snapshot_name="historical_2024Q1"
    )

    # 2. Generate Full-Period / Latest Snapshot (no cutoff)
    sp.process_silver_snapshot(
        quarters=["2024Q1", "2024Q2"],
        cutoff_date=None,
        snapshot_name="latest_full_period"
    )

    # Check Historical Q1 Snapshot
    df_hist_q1 = pd.read_parquet(Path(cfg["storage"]["silver_dir"]) / "historical_2024Q1/demo.parquet")
    case_1006_q1 = df_hist_q1[df_hist_q1["caseid"] == "1006"].iloc[0]
    # Under Q1 cutoff, only Version 1 existed!
    assert case_1006_q1["primaryid"] == "10061"
    assert int(case_1006_q1["caseversion"]) == 1
    assert case_1006_q1["fda_dt"] == "20240215"

    # Check Full Period / Latest Snapshot
    df_latest = pd.read_parquet(Path(cfg["storage"]["silver_dir"]) / "latest_full_period/demo.parquet")
    case_1006_latest = df_latest[df_latest["caseid"] == "1006"].iloc[0]
    # In full period, Version 2 is the winning canonical version!
    assert case_1006_latest["primaryid"] == "10062"
    assert int(case_1006_latest["caseversion"]) == 2
    assert case_1006_latest["fda_dt"] == "20240510"


def test_data_quality_report_generation(pipeline_env):
    """
    Test automated Data Quality Report generation:
    - Asserts reports/data_quality_report.md exists and contains expected sections
    - Asserts data/silver/data_quality_metrics.json contains machine-readable rates
    """
    auditor = DataQualityAuditor(root=pipeline_env["root"])
    metrics = auditor.generate_report(snapshot_name="latest_full_period")

    report_md = pipeline_env["root"] / "reports/data_quality_report.md"
    assert report_md.exists()
    content = report_md.read_text(encoding="utf-8")
    assert "# PharmaSafe Automated Data-Quality & Pipeline Audit Report" in content
    assert "Case Version Reduction Rate" in content
    assert "Relational Integrity & Child Table Quarantine" in content
    assert "ACCEPTANCE CRITERIA STATUS: PASSED" in content

    json_metrics = pipeline_env["root"] / "data/silver/data_quality_metrics.json"
    assert json_metrics.exists()
    assert "case_version_reduction_rate" in metrics["metric_definitions"]
