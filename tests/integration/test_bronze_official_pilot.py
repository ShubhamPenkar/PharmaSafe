"""
Integration test: official Bronze output (skipped when the archives have not been ingested locally).
Row counts are checked against the ingestion summary AND the Parquet files on disk, and the summary's
independently counted source records must reconcile exactly.
"""

import json

import pyarrow.dataset as ds
import pytest

from src.utils.config_loader import get_project_root

CORE_TABLES = ["DEMO", "DRUG", "REAC", "OUTC", "INDI", "THER", "RPSR"]
BRONZE = get_project_root() / "data/bronze"
SUMMARIES = sorted(BRONZE.glob("*/ingestion_summary.json")) if BRONZE.exists() else []

pytestmark = pytest.mark.skipif(not SUMMARIES, reason="official Bronze data not ingested locally")


@pytest.mark.parametrize("summary_path", SUMMARIES, ids=lambda p: p.parent.name)
def test_official_quarter_bronze_reconciles(summary_path):
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["is_synthetic"] is False
    assert summary["verification_status"] == "VERIFIED_OFFICIAL"
    assert summary["archive_sha256_verified_against_manifest"] is True

    for table in CORE_TABLES:
        t = summary["tables"][table]
        assert t["status"] == "INGESTED", table
        assert t["reconciled"], f"{table}: source {t['source_records']} != ingested {t['rows_ingested']} + quarantined {t['rows_quarantined']}"
        on_disk = ds.dataset(str(summary_path.parent / table.lower()), format="parquet").count_rows()
        assert on_disk == t["rows_ingested"], table
        assert "primaryid" in t["columns"]
