"""Hand-computed checks for per-quarter contingency tables, persistence and drug-pair analytics."""

import pytest
from pyspark.sql.functions import array_contains

from src.analytics.interaction_analytics import combination_contingency, pair_frequencies
from src.analytics.temporal_analytics import quarterly_contingency, signal_persistence
from src.signal_detection.signal_engine import compute_safety_signals, eligible_population


def case_drug(spark, rows):
    df = spark.createDataFrame([(c, d, list(r)) for c, d, r in rows], ["caseid", "drug_key", "roles"])
    for role in ("ps", "ss", "c", "i"):
        df = df.withColumn(f"is_{role}", array_contains("roles", role.upper()))
    return df


def test_quarterly_contingency_partitions_cases_by_quarter(spark):
    drugs = case_drug(spark, [("1", "X", ["PS"]), ("2", "X", ["PS"]), ("3", "X", ["PS"]), ("4", "Y", ["PS"]),
                              ("5", "X", ["PS"]), ("6", "Y", ["PS"])])
    reac = spark.createDataFrame([("1", "R"), ("2", "R"), ("3", "R"), ("4", "R"), ("5", "S"), ("6", "S")], ["caseid", "pt"])
    summary = spark.createDataFrame([("1", "2024Q1"), ("2", "2024Q1"), ("3", "2024Q1"), ("4", "2024Q1"),
                                     ("5", "2024Q2"), ("6", "2024Q2")], ["caseid", "report_quarter"])
    rows = {(r["period"], r["drug_key"], r["pt"]): r for r in quarterly_contingency(drugs, reac, summary, ["PS"], 1).collect()}
    q1 = rows[("2024Q1", "X", "R")]
    assert (q1["a"], q1["b"], q1["c"], q1["d"], q1["n_eligible_cases"]) == (3, 0, 1, 0, 4)
    q2 = rows[("2024Q2", "X", "S")]
    assert (q2["a"], q2["b"], q2["c"], q2["d"], q2["n_eligible_cases"]) == (1, 0, 1, 0, 2)
    assert ("2024Q2", "X", "R") not in rows       # no quarter-2 case has X with R
    sig = compute_safety_signals(quarterly_contingency(drugs, reac, summary, ["PS"], 1), min_cases=1).collect()
    assert {r["analysis_period"] for r in sig} == {"2024Q1", "2024Q2"}


def test_signal_persistence_summary(spark):
    rows = [("X", "R", "2024Q1", "POTENTIAL_SIGNAL", 5), ("X", "R", "2024Q2", "NOT_FLAGGED", 3),
            ("X", "R", "2024Q3", "POTENTIAL_SIGNAL", 6), ("Y", "S", "2024Q2", "NOT_FLAGGED", 4)]
    df = spark.createDataFrame(rows, ["drug_name", "reaction_term", "analysis_period", "screening_status", "observed_cases"])
    out = {(r["drug_name"], r["reaction_term"]): r for r in signal_persistence(df).collect()}
    x = out[("X", "R")]
    assert (x["quarters_observed"], x["quarters_flagged"]) == (3, 2)
    assert (x["first_flagged_quarter"], x["last_flagged_quarter"], x["flagged_quarters"]) == ("2024Q1", "2024Q3", "2024Q1,2024Q3")
    assert x["total_observed_cases"] == 14 and x["persistence_ratio"] == pytest.approx(2 / 3)
    y = out[("Y", "S")]
    assert y["quarters_flagged"] == 0 and y["first_flagged_quarter"] is None


def test_pair_frequencies_and_combination_counts(spark):
    drugs_df = case_drug(spark, [("1", "A", ["PS"]), ("1", "B", ["SS"]), ("2", "A", ["PS"]), ("2", "B", ["PS"]),
                                 ("3", "A", ["PS"]), ("3", "B", ["PS"]), ("4", "A", ["PS"]), ("4", "B", ["PS"]),
                                 ("5", "A", ["PS"]), ("6", "A", ["PS"]), ("7", "C", ["PS"]), ("8", "C", ["PS"])])
    reac = spark.createDataFrame([("1", "R"), ("2", "R"), ("3", "R"), ("4", "S"), ("5", "S"), ("6", "R"), ("7", "S"), ("8", "S")],
                                 ["caseid", "pt"])
    drugs, reactions, n = eligible_population(drugs_df, reac, ["PS", "SS"])
    assert n == 8
    pairs = pair_frequencies(drugs, n, min_pair_cases=2, limit=100)
    pair = pairs.collect()
    assert len(pair) == 1 and (pair[0]["drug_1"], pair[0]["drug_2"], pair[0]["pair_cases"]) == ("A", "B", 4)
    assert pair[0]["expected_pair_cases"] == pytest.approx(6 * 4 / 8) and pair[0]["lift"] == pytest.approx(4 / 3)

    rows = {r["pt"]: r for r in combination_contingency(drugs, reactions, n, pairs, min_cases=1).collect()}
    r = rows["R"]
    assert (r["drug_key"], r["a"], r["b"], r["c"], r["d"]) == ("A + B", 3, 1, 1, 3)
    assert r["a"] + r["b"] + r["c"] + r["d"] == n
    s = rows["S"]
    assert (s["a"], s["b"], s["c"], s["d"]) == (1, 3, 3, 1)
