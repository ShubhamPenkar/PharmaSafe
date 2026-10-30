"""
PharmaSafe Deduplication & Signal Detection Integration Unit Tests
Validates version resolution, orphan filtering, and Spark 2x2 contingency generation.
"""

import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session, stop_spark_session
from tests.fixtures.synthetic_faers import create_synthetic_faers_data
from src.preprocessing.case_deduplicator import (
    deduplicate_demo_cases,
    filter_child_table_by_canonical_cases,
    clean_and_normalize_drugs,
    clean_and_normalize_reactions
)
from src.signal_detection.signal_engine import (
    build_contingency_matrix,
    compute_safety_signals
)


@pytest.fixture(scope="module")
def spark():
    s = get_spark_session(app_name="PharmaSafeTest", master="local[1]", shuffle_partitions=1)
    yield s
    stop_spark_session(s)


def test_case_deduplication_and_orphan_removal(spark):
    """
    Asserts:
    1. DEMO deduplicates from 6 rows to 5 canonical cases.
    2. Case 101 retains version 2 (primaryid 10102).
    3. Superseded primaryid 10101 and orphan 99999 are pruned from DRUG and REAC.
    """
    data = create_synthetic_faers_data(spark)
    canonical_demo, demo_metrics = deduplicate_demo_cases(data["DEMO"])

    assert demo_metrics["raw_demo_count"] == 6
    assert demo_metrics["canonical_case_count"] == 5
    assert demo_metrics["duplicate_versions_removed"] == 1

    # Check that case 101 has primaryid 10102
    case_101 = canonical_demo.filter(canonical_demo.caseid == "101").collect()
    assert len(case_101) == 1
    assert case_101[0]["primaryid"] == "10102"
    assert case_101[0]["caseversion"] == 2

    # Prune DRUG child table
    filtered_drug, drug_metrics = filter_child_table_by_canonical_cases(
        data["DRUG"], canonical_demo, table_name="DRUG"
    )
    assert drug_metrics["DRUG_raw_count"] == 7
    assert drug_metrics["DRUG_retained_count"] == 5  # 10102, 10201, 10301, 10401, 10501
    assert drug_metrics["DRUG_orphans_removed"] == 2

    # Prune REAC child table
    filtered_reac, reac_metrics = filter_child_table_by_canonical_cases(
        data["REAC"], canonical_demo, table_name="REAC"
    )
    assert reac_metrics["REAC_raw_count"] == 7
    assert reac_metrics["REAC_retained_count"] == 5
    assert reac_metrics["REAC_orphans_removed"] == 2


def test_contingency_matrix_and_signal_generation(spark):
    """
    Tests end-to-end contingency table construction and signal computation on synthetic data.
    """
    data = create_synthetic_faers_data(spark)
    canonical_demo, _ = deduplicate_demo_cases(data["DEMO"])
    filtered_drug, _ = filter_child_table_by_canonical_cases(data["DRUG"], canonical_demo, "DRUG")
    filtered_reac, _ = filter_child_table_by_canonical_cases(data["REAC"], canonical_demo, "REAC")

    clean_drugs = clean_and_normalize_drugs(filtered_drug, target_roles=["PS"])
    clean_reac = clean_and_normalize_reactions(filtered_reac)

    # Build contingency matrix with min_observed_cases=1 to inspect all pairs
    contingency_df, total_n = build_contingency_matrix(
        canonical_demo, clean_drugs, clean_reac, min_observed_cases=1
    )
    assert total_n == 5
    contingency_rows = contingency_df.collect()
    assert len(contingency_rows) == 2  # (ATORVASTATIN, RHABDOMYOLYSIS) and (AMOXICILLIN, RASH)

    atorva_row = [r for r in contingency_rows if r["clean_drug_name"] == "ATORVASTATIN"][0]
    assert atorva_row["clean_pt"] == "RHABDOMYOLYSIS"
    assert atorva_row["a"] == 3
    assert atorva_row["b"] == 0
    assert atorva_row["c"] == 0
    assert atorva_row["d"] == 2
    assert atorva_row["total_cases_N"] == 5

    amox_row = [r for r in contingency_rows if r["clean_drug_name"] == "AMOXICILLIN"][0]
    assert amox_row["clean_pt"] == "RASH"
    assert amox_row["a"] == 2
    assert amox_row["b"] == 0
    assert amox_row["c"] == 0
    assert amox_row["d"] == 3

    # Compute safety signals with min_cases=3
    signals_df = compute_safety_signals(contingency_df, min_cases=3, min_ror_ci_lower=1.0, min_prr=2.0)
    signals = signals_df.collect()

    atorva_signal = [s for s in signals if s["drug_name"] == "ATORVASTATIN"][0]
    # In a tiny universe of N=5, 95% CI is wide (lower bound < 1.0), so properly flagged
    assert atorva_signal["observed_case_count"] == 3
    assert atorva_signal["ROR"] == 35.0
    assert atorva_signal["zero_corrected"] is True
    # Screening correctly warns that ROR CI lower is <= 1.0 due to small sample size
    assert "ROR 95% CI lower" in atorva_signal["screening_reasons"][0]

    amox_signal = [s for s in signals if s["drug_name"] == "AMOXICILLIN"][0]
    # Amoxicillin has a=2 < 3, so must be NON_SIGNAL due to observed cases < 3
    assert amox_signal["screening_status"] == "NON_SIGNAL"
    assert any("Observed cases (2) < threshold (3)" in r for r in amox_signal["screening_reasons"])

