"""Report-duplicate classification, case version ranking and validity intervals."""

from src.preprocessing.case_deduplicator import (
    build_case_version_history, canonical_cases_asof, resolve_report_duplicates, version_history_metrics,
)
from pyspark.sql.functions import to_date

SRC = ["primaryid", "caseid", "caseversion", "fda_dt"]
COLS = SRC + ["_source_quarter"]


def demo(spark, rows):
    return spark.createDataFrame(rows, COLS)


def history_input(spark, rows):
    """rows: (primaryid, caseid, caseversion_int, fda_dt 'YYYY-MM-DD'|None, quarter)"""
    df = spark.createDataFrame(rows, ["primaryid", "caseid", "caseversion_int", "fda", "_source_quarter"])
    return df.withColumn("fda_dt_date", to_date("fda")).drop("fda")


def versions(history):
    return {r["primaryid"]: r.asDict() for r in history.collect()}


def test_report_duplicates_are_classified(spark):
    df = demo(spark, [
        ("1", "10", "1", "20240101", "2024Q1"),
        ("1", "10", "1", "20240101", "2024Q1"),   # exact duplicate
        ("2", "20", "1", "20240201", "2024Q1"),
        ("2", "20", "1", "20240201", "2024Q2"),   # re-published unchanged in a later quarter
        ("3", "30", "1", "20240301", "2024Q1"),
        ("3", "30", "2", "20240301", "2024Q1"),   # same primaryid, same quarter, different content
    ])
    retained, excluded = resolve_report_duplicates(df, SRC)
    assert retained.count() == 3
    assert sorted(r["primaryid"] for r in retained.collect()) == ["1", "2", "3"]
    reasons = {r["primaryid"]: r["quarantine_reason"] for r in excluded.collect()}
    assert reasons == {"1": "EXACT_DUPLICATE", "2": "EXACT_DUPLICATE", "3": "CONFLICTING_DUPLICATE_PRIMARYID"}
    assert {r["primaryid"]: r["_source_quarter"] for r in retained.collect()}["2"] == "2024Q2"


def test_resubmitted_with_changes_in_later_quarter(spark):
    df = demo(spark, [("5", "50", "1", "20240101", "2024Q1"), ("5", "50", "2", "20240101", "2024Q2")])
    retained, excluded = resolve_report_duplicates(df, SRC)
    assert retained.collect()[0]["_source_quarter"] == "2024Q2"
    assert excluded.collect()[0]["quarantine_reason"] == "PRIMARYID_RESUBMITTED_LATER"


def test_version_ranking_and_validity_interval(spark):
    h = build_case_version_history(history_input(spark, [
        ("11", "1", 1, "2024-01-10", "2024Q1"),
        ("12", "1", 2, "2024-05-10", "2024Q2"),    # follow-up in a later quarter
        ("21", "2", 1, "2024-01-15", "2024Q1"),
        ("22", "2", 2, "2024-01-15", "2024Q1"),    # superseded within the same quarter
    ]))
    v = versions(h)
    assert v["12"]["version_rank"] == 1 and v["12"]["valid_to_quarter"] is None
    assert v["11"]["valid_from_quarter"] == "2024Q1" and v["11"]["valid_to_quarter"] == "2024Q2"
    assert v["11"]["ever_canonical"] is True
    assert v["21"]["valid_to_quarter"] == "2024Q1" and v["21"]["ever_canonical"] is False
    assert v["22"]["is_latest_version"] is True


def test_asof_snapshot_is_not_rewritten_by_later_followup(spark):
    h = build_case_version_history(history_input(spark, [
        ("11", "1", 1, "2024-02-15", "2024Q1"),
        ("12", "1", 2, "2024-05-10", "2024Q2"),
    ]))
    q1 = canonical_cases_asof(h, "2024Q1").collect()
    q2 = canonical_cases_asof(h, "2024Q2").collect()
    latest = canonical_cases_asof(h).collect()
    assert [r["primaryid"] for r in q1] == ["11"]
    assert [r["primaryid"] for r in q2] == ["12"]
    assert [r["primaryid"] for r in latest] == ["12"]
    assert canonical_cases_asof(h, "2023Q4").count() == 0


def test_missing_caseversion_ranks_below_known_never_assumed(spark):
    h = build_case_version_history(history_input(spark, [
        ("31", "3", None, "2024-09-01", "2024Q1"),   # later date but unknown version
        ("32", "3", 1, "2024-01-01", "2024Q1"),
    ]))
    v = versions(h)
    assert v["32"]["version_rank"] == 1
    assert v["31"]["caseversion_int"] is None
    assert version_history_metrics(h)["versions_missing_caseversion"] == 1


def test_tiebreak_uses_numeric_primaryid(spark):
    h = build_case_version_history(history_input(spark, [
        ("9", "4", 1, "2024-01-01", "2024Q1"), ("10", "4", 1, "2024-01-01", "2024Q1"),
    ]))
    assert versions(h)["10"]["version_rank"] == 1   # numeric, not lexicographic ("9" > "10")


def test_fda_deleted_case_leaves_canonical_set(spark):
    deleted = spark.createDataFrame([("1", "2024Q2")], ["caseid", "_source_quarter"])
    h = build_case_version_history(history_input(spark, [
        ("11", "1", 1, "2024-01-10", "2024Q1"), ("21", "2", 1, "2024-01-10", "2024Q1"),
    ]), deleted)
    assert [r["caseid"] for r in canonical_cases_asof(h).collect()] == ["2"]
    assert sorted(r["caseid"] for r in canonical_cases_asof(h, "2024Q1").collect()) == ["1", "2"]
    assert [r["caseid"] for r in canonical_cases_asof(h, "2024Q2").collect()] == ["2"]
    assert version_history_metrics(h)["cases_removed_by_fda_deletion"] == 1
