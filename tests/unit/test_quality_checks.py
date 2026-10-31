import json

from src.validation.quality_checks import DataQualityAuditor


def make_auditor(tmp_path, bronze_reconciled=True, silver_raw=100, silver_retained=90, silver_excluded=10):
    for d in ("bronze/2024Q1", "silver", "gold", "raw"):
        (tmp_path / d).mkdir(parents=True, exist_ok=True)
    (tmp_path / "bronze/2024Q1/ingestion_summary.json").write_text(json.dumps({
        "verification_status": "VERIFIED_OFFICIAL", "is_synthetic": False,
        "tables": {"DEMO": {"status": "INGESTED", "source_records": 10, "rows_ingested": 10, "rows_quarantined": 0, "reconciled": bronze_reconciled}}}))
    (tmp_path / "silver/silver_metrics.json").write_text(json.dumps({"deduplication_policy": "x", "tables": {
        "DEMO": {"raw_rows": 10, "retained_report_versions": 9, "excluded_rows": 1, "excluded_by_reason": {"EXACT_DUPLICATE": 1},
                 "superseded_versions": 1, "versions_never_canonical": 0, "versions_missing_caseversion": 0,
                 "cases_removed_by_fda_deletion": 0, "canonical_cases_latest": 8, "missing_pct": {"age": 12.5}},
        "DRUG": {"raw_rows": silver_raw, "retained_rows": silver_retained, "excluded_rows": silver_excluded, "retention_pct": 90.0,
                 "excluded_by_reason": {"ORPHAN_NO_DEMO_RECORD": 10}, "missing_pct": {}}}}))
    cfg = {"storage": {"bronze_dir": str(tmp_path / "bronze"), "silver_dir": str(tmp_path / "silver"), "gold_dir": str(tmp_path / "gold"),
                       "manifest_path": str(tmp_path / "raw/manifest.json")}}
    return DataQualityAuditor(config=cfg, root=tmp_path)


def test_all_checks_pass_when_counts_reconcile(tmp_path):
    data = make_auditor(tmp_path).generate_report()
    assert data["all_checks_passed"] and len(data["checks"]) >= 4
    md = (tmp_path / "reports/data_quality_report.md").read_text()
    assert "ALL CHECKS PASSED" in md and "EXACT_DUPLICATE" in md and "| DEMO | age | 12.5 |" in md


def test_unreconciled_bronze_fails_the_verdict(tmp_path):
    data = make_auditor(tmp_path, bronze_reconciled=False).generate_report()
    assert not data["all_checks_passed"]
    assert "CHECKS FAILED" in (tmp_path / "reports/data_quality_report.md").read_text()


def test_silver_accounting_mismatch_fails(tmp_path):
    data = make_auditor(tmp_path, silver_excluded=9).generate_report()   # 90 + 9 != 100
    assert not data["all_checks_passed"]
    assert any(not c["passed"] and "Silver DRUG" in c["check"] for c in data["checks"])
