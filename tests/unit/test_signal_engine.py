"""Spark signal engine: contingency construction, parity with the scalar metrics, screening."""

import pytest
from pyspark.sql.functions import array_contains

from src.signal_detection.metrics import calculate_ror, calculate_prr
from src.signal_detection.signal_engine import (
    build_contingency_table, compute_safety_signals, eligible_population, screening_policies,
)


def case_drug(spark, rows):
    """rows: (caseid, drug_key, roles tuple)"""
    df = spark.createDataFrame([(c, d, list(r)) for c, d, r in rows], ["caseid", "drug_key", "roles"])
    for role in ("ps", "ss", "c", "i"):
        df = df.withColumn(f"is_{role}", array_contains("roles", role.upper()))
    return df


def reactions(spark, rows):
    return spark.createDataFrame(rows, ["caseid", "pt"])


def contingency(spark, drugs, reacs, roles=("PS",)):
    d, r, n = eligible_population(case_drug(spark, drugs), reactions(spark, reacs), list(roles))
    return build_contingency_table(d, r, n), n


def test_contingency_counts_cases_once_and_cells_sum_to_N(spark):
    # case 1 has two drugs and two reactions (must contribute once per pair, not 4x)
    drugs = [("1", "X", ("PS",)), ("1", "Y", ("PS",)), ("2", "X", ("PS",)), ("3", "Y", ("PS",)), ("4", "Z", ("PS",))]
    reacs = [("1", "R1"), ("1", "R2"), ("2", "R1"), ("3", "R2"), ("4", "R3"), ("5", "R1")]  # case 5 has no drug
    table, n = contingency(spark, drugs, reacs)
    assert n == 4   # case 5 is not eligible: no drug in the role policy
    rows = {(r["drug_key"], r["pt"]): r for r in table.collect()}
    x_r1 = rows[("X", "R1")]
    assert (x_r1["a"], x_r1["b"], x_r1["c"], x_r1["d"]) == (2, 0, 0, 2)
    y_r2 = rows[("Y", "R2")]
    assert (y_r2["a"], y_r2["b"], y_r2["c"], y_r2["d"]) == (2, 0, 0, 2)
    x_r2 = rows[("X", "R2")]
    assert (x_r2["a"], x_r2["b"], x_r2["c"], x_r2["d"]) == (1, 1, 1, 1)
    assert all(r["a"] + r["b"] + r["c"] + r["d"] == n for r in rows.values())


def test_role_policy_filters_drugs(spark):
    drugs = [("1", "X", ("PS",)), ("2", "X", ("C",)), ("3", "W", ("SS",))]
    reacs = [("1", "R"), ("2", "R"), ("3", "R")]
    _, n_primary = contingency(spark, drugs, reacs, ("PS",))
    _, n_suspect = contingency(spark, drugs, reacs, ("PS", "SS"))
    _, n_all = contingency(spark, drugs, reacs, ("PS", "SS", "C", "I"))
    assert (n_primary, n_suspect, n_all) == (1, 2, 3)


@pytest.mark.parametrize("a,b,c,d", [
    (50, 150, 100, 9700), (3, 97, 40, 9860), (500, 1500, 1000, 97000),
    (0, 10, 5, 100), (5, 0, 5, 100), (5, 10, 0, 100), (5, 10, 5, 0), (1, 1, 1, 1),
])
def test_spark_expressions_match_scalar_reference(spark, a, b, c, d):
    df = spark.createDataFrame([("D", "E", a, b, c, d, a + b, a + c, a + b + c + d)],
                               ["drug_key", "pt", "a", "b", "c", "d", "n_drug", "n_event", "n_eligible_cases"])
    row = compute_safety_signals(df).collect()[0]
    ror, prr = calculate_ror(a, b, c, d), calculate_prr(a, b, c, d)
    assert row["ROR"] == pytest.approx(ror["ror"], rel=1e-3, abs=6e-5)  # reference rounds to 4 dp
    assert row["ROR_CI_lower"] == pytest.approx(ror["ror_ci_lower"], rel=1e-3, abs=6e-5)  # reference rounds to 4 dp
    assert row["ROR_CI_upper"] == pytest.approx(ror["ror_ci_upper"], rel=1e-3, abs=6e-5)  # reference rounds to 4 dp
    assert row["PRR"] == pytest.approx(prr["prr"], rel=1e-3, abs=6e-5)  # reference rounds to 4 dp
    assert row["PRR_CI_lower"] == pytest.approx(prr["prr_ci_lower"], rel=1e-3, abs=6e-5)  # reference rounds to 4 dp
    assert row["chi2_yates"] == pytest.approx(prr["chi2_yates"], rel=1e-3, abs=6e-5)
    assert row["zero_corrected"] == ror["zero_corrected"]


def test_screening_thresholds_and_reasons(spark):
    cols = ["drug_key", "pt", "a", "b", "c", "d", "n_drug", "n_event", "n_eligible_cases"]
    df = spark.createDataFrame([
        ("STRONG", "E", 50, 150, 100, 9700, 200, 150, 10000),
        ("RARE", "E", 2, 98, 100, 9800, 100, 102, 10000),
        ("NULLISH", "E", 20, 180, 1000, 8800, 200, 1020, 10000),
    ], cols)
    rows = {r["drug_name"]: r for r in compute_safety_signals(df, min_cases=3, min_chi2=4.0).collect()}
    assert rows["STRONG"]["screening_status"] == "POTENTIAL_SIGNAL" and rows["STRONG"]["screening_reasons"] == []
    assert rows["RARE"]["screening_status"] == "NOT_FLAGGED"
    assert any("Observed cases (2)" in r for r in rows["RARE"]["screening_reasons"])
    assert rows["NULLISH"]["screening_status"] == "NOT_FLAGGED"
    assert rows["STRONG"]["expected_cases"] == pytest.approx(200 * 150 / 10000)
    assert rows["STRONG"]["background_cases"] == 100


def test_screening_policies_inherit_primary_thresholds():
    cfg = {"screening_criteria": {"primary": {"min_cases": 3, "min_prr": 2.0, "target_roles": ["PS"]},
                                  "sensitivity": {"all_suspect": {"min_cases": 5, "target_roles": ["PS", "SS"]}}}}
    p = screening_policies(cfg)
    assert p["all_suspect"] == {"min_cases": 5, "min_prr": 2.0, "target_roles": ["PS", "SS"]}
    assert p["primary"]["min_cases"] == 3
