"""
PharmaSafe Integration Test — Official Bronze Pilot (2024 Q3)
Verifies:
1. Physical existence and schema conformance of official 2024Q3 Bronze Parquet files.
2. Ingestion summary metadata and provenance attribution.
3. Enforcement of synthetic fixture quarantine (official pipeline refuses synthetic files).
"""

import sys
import json
from pathlib import Path
import pytest
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config_loader import get_project_root, load_data_sources_config
from src.ingestion.bronze_pipeline import BronzePipeline

CORE_TABLES = ["DEMO", "DRUG", "REAC", "OUTC", "INDI", "THER", "RPSR"]
EXPECTED_COUNTS = {
    "DEMO": 405513,
    "DRUG": 1907293,
    "REAC": 1431718,
    "OUTC": 288275,
    "INDI": 1177133,
    "THER": 532854,
    "RPSR": 10087
}


def test_official_2024q3_bronze_artifacts():
    """Verify that 2024Q3 Bronze Parquet datasets exist and meet exact counts and schemas."""
    root = get_project_root()
    bronze_dir = root / "data/bronze/2024Q3"
    assert bronze_dir.exists(), f"Bronze directory missing at {bronze_dir}"

    summary_path = bronze_dir / "ingestion_summary.json"
    assert summary_path.exists(), f"Ingestion summary missing at {summary_path}"

    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    assert summary["quarter"] == "2024Q3"
    assert summary["is_synthetic"] is False
    assert summary["verification_status"] == "VERIFIED_OFFICIAL"
    assert summary["total_valid_rows"] == sum(EXPECTED_COUNTS.values())
    assert summary["total_quarantined_rows"] == 0

    for t_name, expected_count in EXPECTED_COUNTS.items():
        pq_file = bronze_dir / f"{t_name.lower()}.parquet"
        assert pq_file.exists(), f"Missing Parquet file for table {t_name}"
        
        # Read parquet metadata with pyarrow (instantaneous without reading entire file)
        meta = pq.read_metadata(str(pq_file))
        assert meta.num_rows == expected_count, (
            f"Row count mismatch for {t_name}: expected {expected_count}, got {meta.num_rows}"
        )

        schema = pq.read_schema(str(pq_file))
        col_names = schema.names
        assert "primaryid" in col_names
        assert "_source_quarter" in col_names
        assert "_source_file" in col_names
        assert "_ingestion_timestamp" in col_names
        assert "_is_synthetic" in col_names


def test_synthetic_isolation_in_official_runner():
    """Confirm that the Bronze pipeline strictly refuses synthetic archives in the official data path."""
    root = get_project_root()
    synthetic_fixture = root / "data/synthetic/fixtures/faers_ascii_2024q1.zip"
    if synthetic_fixture.exists():
        bp = BronzePipeline()
        with pytest.raises(ValueError, match="Synthetic fixtures cannot be ingested"):
            bp.ingest_archive(
                archive_path=synthetic_fixture,
                quarter_id="2024Q1",
                allow_synthetic=False
            )

