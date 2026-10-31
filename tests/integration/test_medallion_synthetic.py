"""
Bronze -> Silver -> Gold on deterministic SYNTHETIC fixtures, fully isolated in a temp directory.
These verify pipeline mechanics only; they say nothing about real drug safety.
"""

import json
from pathlib import Path

import pytest

from src.analytics.gold_pipeline import GoldPipeline
from src.ingestion.bronze_pipeline import BronzePipeline
from src.ingestion.faers_sample_builder import build_synthetic_faers_quarter, build_synthetic_faers_quarter_2
from src.preprocessing.silver_pipeline import SilverPipeline
from src.utils.config_loader import load_data_sources_config


@pytest.fixture(scope="module")
def env(tmp_path_factory, spark):
    root = tmp_path_factory.mktemp("medallion")
    config = load_data_sources_config()
    config["storage"] = {**config["storage"], "raw_dir": str(root / "raw"), "bronze_dir": str(root / "bronze"),
                         "silver_dir": str(root / "silver"), "gold_dir": str(root / "gold")}
    (root / "raw").mkdir()
    q1, q2 = root / "raw/faers_ascii_2024q1.zip", root / "raw/faers_ascii_2024q2.zip"
    build_synthetic_faers_quarter(q1, quarter_id="2024Q1", seed=42)
    build_synthetic_faers_quarter_2(q2, seed=43)

    bronze = BronzePipeline(config=config, spark=spark)
    summaries = {
        "2024Q1": bronze.ingest_archive(q1, "2024Q1", allow_synthetic=True),
        "2024Q2": bronze.ingest_archive(q2, "2024Q2", allow_synthetic=True),
    }
    silver = SilverPipeline(config=config, spark=spark).run()
    gold = GoldPipeline(data_config=config, spark=spark)
    return {"root": root, "config": config, "bronze": bronze, "q1": q1, "summaries": summaries, "silver": silver,
            "gold": gold, "gold_latest": gold.run(), "gold_q1": gold.run(asof_quarter="2024Q1")}


def read(spark, env, layer, *parts):
    return spark.read.parquet(str(Path(env["config"]["storage"][f"{layer}_dir"]).joinpath(*parts)))


def test_bronze_reconciles_every_source_record(env):
    for q, summary in env["summaries"].items():
        for table, t in summary["tables"].items():
            if t["status"] != "INGESTED":
                continue
            assert t["reconciled"], (q, table)
            assert t["rows_ingested"] + t["rows_quarantined"] + t.get("non_numeric_lines_skipped", 0) == t["source_records"]


def test_bronze_quarantines_malformed_row_with_reason(env, spark):
    demo = env["summaries"]["2024Q1"]["tables"]["DEMO"]
    assert demo["rows_quarantined"] == 1
    q = read(spark, env, "bronze", "2024Q1", "quarantine", "demo").collect()
    assert len(q) == 1 and q[0]["reason"].startswith("COLUMN_COUNT_MISMATCH")
    assert (Path(env["config"]["storage"]["bronze_dir"]) / "2024Q1/quarantine/demo_malformed_sample.json").exists()


def test_bronze_is_idempotent_and_rejects_checksum_mismatch(env):
    again = env["bronze"].ingest_archive(env["q1"], "2024Q1", allow_synthetic=True)
    assert again["ingestion_timestamp"] == env["summaries"]["2024Q1"]["ingestion_timestamp"]
    with pytest.raises(ValueError, match="Checksum mismatch"):
        env["bronze"].ingest_archive(env["q1"], "2024Q1", force=True, allow_synthetic=True, expected_sha256="0" * 64)


def test_bronze_refuses_synthetic_archive_without_opt_in(env, tmp_path):
    cfg = dict(env["config"]); cfg["storage"] = {**cfg["storage"], "bronze_dir": str(tmp_path / "b")}
    bp = BronzePipeline(config=cfg, spark=None)
    with pytest.raises(ValueError, match="synthetic"):
        bp.ingest_archive(env["q1"], "2024Q1", allow_synthetic=False)


def test_silver_accounts_for_every_row(env):
    for table, m in env["silver"]["tables"].items():
        if table == "DEMO":
            assert m["raw_rows"] == m["retained_report_versions"] + m["excluded_rows"]
        else:
            assert m["raw_rows"] == m["retained_rows"] + m["excluded_rows"], table


def test_silver_demo_versions_and_duplicates(env):
    d = env["silver"]["tables"]["DEMO"]
    assert d["excluded_by_reason"] == {"EXACT_DUPLICATE": 1}
    assert d["superseded_versions"] == 3          # 10011, 10021, 10061
    assert d["canonical_cases_latest"] == d["report_versions"] - d["superseded_versions"] == 66


def test_silver_distinguishes_orphans_from_superseded_versions(env, spark):
    reasons = env["silver"]["tables"]["DRUG"]["excluded_by_reason"]
    assert reasons.get("ORPHAN_NO_DEMO_RECORD", 0) >= 1
    # rows of superseded versions (e.g. primaryid 10011) are retained in Silver, not treated as orphans
    kept = {r["primaryid"] for r in read(spark, env, "silver", "drug").select("primaryid").distinct().collect()}
    assert "10011" in kept and "99999" not in kept


def test_critical_temporal_requirement(env, spark):
    """Case 1006: v1 received 2024Q1, follow-up v2 in 2024Q2. The Q1 view must not see v2."""
    q1 = {r["caseid"]: r["primaryid"] for r in read(spark, env, "gold", "asof_2024Q1", "case_summary").collect()}
    latest = {r["caseid"]: r["primaryid"] for r in read(spark, env, "gold", "latest", "case_summary").collect()}
    assert q1["1006"] == "10061" and latest["1006"] == "10062"
    assert q1["1001"] == latest["1001"] == "10012"
    assert env["gold_q1"]["canonical_cases"] < env["gold_latest"]["canonical_cases"]
    assert env["gold_q1"]["report_quarters"] == ["2024Q1"]


def test_gold_counts_each_case_once_and_cells_sum_to_n(env, spark):
    n = env["gold_latest"]["canonical_cases"]
    assert n == 66
    assert read(spark, env, "gold", "latest", "case_drug").groupBy("caseid", "drug_key").count().filter("count > 1").count() == 0
    assert read(spark, env, "gold", "latest", "case_reaction").groupBy("caseid", "pt").count().filter("count > 1").count() == 0
    for policy, info in env["gold_latest"]["signals"].items():
        sig = read(spark, env, "gold", "latest", "safety_signals", f"role_policy={policy}")
        assert sig.filter("a + b + c + d != n_eligible_cases").count() == 0
        assert info["eligible_cases_N"] <= n


def test_gold_signal_schema_and_values(env, spark):
    sig = read(spark, env, "gold", "latest", "safety_signals", "role_policy=primary")
    required = {"drug_name", "reaction_term", "analysis_period", "observed_cases", "background_cases", "a", "b", "c", "d",
                "ROR", "ROR_CI_lower", "ROR_CI_upper", "PRR", "screening_status", "method_version", "analysis_timestamp"}
    assert required <= set(sig.columns)
    row = sig.filter("drug_name = 'SYNTH_DRUG_ALPHA' and reaction_term = 'SYNTH_EVENT_TACHY'").collect()[0]
    assert (row["a"], row["b"], row["c"], row["d"]) == (33, 5, 2, 26)
    assert row["ROR"] == pytest.approx(33 * 26 / (5 * 2))
    assert row["PRR"] == pytest.approx((33 / 38) / (2 / 28))
    assert row["screening_status"] == "POTENTIAL_SIGNAL"
    assert row["analysis_period"] == "latest"


def test_silver_and_gold_metrics_written(env):
    root = env["root"]
    assert json.loads((root / "silver/silver_metrics.json").read_text())["quarters"] == ["2024Q1", "2024Q2"]
    assert (root / "gold/latest/gold_metrics.json").exists()
