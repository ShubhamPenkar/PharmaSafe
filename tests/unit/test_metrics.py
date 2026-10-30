"""
PharmaSafe Statistical Metrics Unit Tests
Tests ROR, PRR, 95% Confidence Intervals, and Haldane-Anscombe zero-cell corrections
against known analytical values.
"""

import math
import pytest
from src.signal_detection.metrics import (
    calculate_ror,
    calculate_prr,
    calculate_contingency_counts,
    evaluate_screening_status
)


def test_contingency_table_exact_math():
    """
    Test case with known analytical results:
    a = 50 (Drug + Reaction)
    b = 950 (Drug + Other Reactions)
    c = 100 (Other Drugs + Reaction)
    d = 8900 (Other Drugs + Other Reactions)
    """
    a, b, c, d = 50, 950, 100, 8900

    # Theoretical ROR
    expected_ror = (50 * 8900) / (950 * 100)  # 4.6842105...
    se_ln_ror = math.sqrt((1/50) + (1/950) + (1/100) + (1/8900))
    expected_ror_lower = math.exp(math.log(expected_ror) - 1.959963984540054 * se_ln_ror)
    expected_ror_upper = math.exp(math.log(expected_ror) + 1.959963984540054 * se_ln_ror)

    res_ror = calculate_ror(a, b, c, d)
    assert pytest.approx(res_ror["ror"], rel=1e-3) == expected_ror
    assert pytest.approx(res_ror["ror_ci_lower"], rel=1e-3) == expected_ror_lower
    assert pytest.approx(res_ror["ror_ci_upper"], rel=1e-3) == expected_ror_upper
    assert not res_ror["zero_corrected"]

    # Theoretical PRR: (50 / 1000) / (100 / 9000) = 0.05 / 0.0111111 = 4.5
    expected_prr = (50 / 1000) / (100 / 9000)
    se_ln_prr = math.sqrt((1/50) - (1/1000) + (1/100) - (1/9000))
    expected_prr_lower = math.exp(math.log(expected_prr) - 1.959963984540054 * se_ln_prr)
    expected_prr_upper = math.exp(math.log(expected_prr) + 1.959963984540054 * se_ln_prr)

    res_prr = calculate_prr(a, b, c, d)
    assert pytest.approx(res_prr["prr"], rel=1e-3) == expected_prr
    assert pytest.approx(res_prr["prr_ci_lower"], rel=1e-3) == expected_prr_lower
    assert pytest.approx(res_prr["prr_ci_upper"], rel=1e-3) == expected_prr_upper
    assert not res_prr["zero_corrected"]


def test_zero_cell_haldane_anscombe_correction():
    """Verify Haldane-Anscombe correction (+0.5) when a cell is zero."""
    a, b, c, d = 0, 50, 10, 500
    a_c, b_c, c_c, d_c, corrected = calculate_contingency_counts(a, b, c, d)
    assert corrected is True
    assert a_c == 0.5
    assert b_c == 50.5
    assert c_c == 10.5
    assert d_c == 500.5

    # Should compute valid non-zero finite bounds
    res_ror = calculate_ror(a, b, c, d)
    assert res_ror["zero_corrected"] is True
    assert res_ror["ror"] > 0
    assert not math.isinf(res_ror["ror"])
    assert not math.isnan(res_ror["ror"])


def test_negative_counts_raise_value_error():
    """Verify that negative counts raise ValueError."""
    with pytest.raises(ValueError):
        calculate_ror(-1, 10, 10, 10)


def test_evaluate_screening_status():
    """Verify regulatory screening logic."""
    # Meets all criteria
    status, reasons = evaluate_screening_status(observed_cases=10, ror_ci_lower=1.8, prr=2.5)
    assert status == "SIGNAL"
    assert len(reasons) == 0

    # Fails min case count
    status, reasons = evaluate_screening_status(observed_cases=2, ror_ci_lower=2.5, prr=3.0, min_cases=3)
    assert status == "NON_SIGNAL"
    assert any("Observed cases" in r for r in reasons)

    # Fails ROR CI lower
    status, reasons = evaluate_screening_status(observed_cases=10, ror_ci_lower=0.85, prr=2.5, min_ror_ci_lower=1.0)
    assert status == "NON_SIGNAL"
    assert any("ROR 95% CI lower" in r for r in reasons)

    # Fails PRR threshold
    status, reasons = evaluate_screening_status(observed_cases=10, ror_ci_lower=1.5, prr=1.4, min_prr=2.0)
    assert status == "NON_SIGNAL"
    assert any("PRR" in r for r in reasons)
