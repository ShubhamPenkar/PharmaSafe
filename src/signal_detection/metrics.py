"""
PharmaSafe Statistical Signal Detection Metrics
Implements validated pharmacovigilance disproportionality algorithms:
- Reporting Odds Ratio (ROR) with 95% Wald Confidence Intervals
- Proportional Reporting Ratio (PRR) with 95% Log-Transformed Confidence Intervals
- Haldane-Anscombe Zero-Cell Correction
- Chi-Square Test with Yates Continuity Correction
"""

import math
from typing import Dict, Any, Optional


def calculate_contingency_counts(
    a: int | float,
    b: int | float,
    c: int | float,
    d: int | float,
    zero_cell_correction: float = 0.5
) -> tuple[float, float, float, float, bool]:
    """
    Validates counts and applies Haldane-Anscombe correction if any cell is zero.
    Returns (a, b, c, d, zero_corrected_flag).
    """
    a_f, b_f, c_f, d_f = float(a), float(b), float(c), float(d)
    zero_corrected = False

    if a_f < 0 or b_f < 0 or c_f < 0 or d_f < 0:
        raise ValueError(f"Contingency counts cannot be negative: a={a}, b={b}, c={c}, d={d}")

    # If any cell is zero, apply Haldane-Anscombe correction to all cells
    if a_f == 0 or b_f == 0 or c_f == 0 or d_f == 0:
        a_f += zero_cell_correction
        b_f += zero_cell_correction
        c_f += zero_cell_correction
        d_f += zero_cell_correction
        zero_corrected = True

    return a_f, b_f, c_f, d_f, zero_corrected


def calculate_ror(
    a: int | float,
    b: int | float,
    c: int | float,
    d: int | float,
    z_score: float = 1.959963984540054,
    zero_cell_correction: float = 0.5
) -> Dict[str, Any]:
    """
    Calculates Reporting Odds Ratio (ROR), its standard error, and 95% Confidence Interval.
    
    ROR = (a * d) / (b * c)
    SE(ln(ROR)) = sqrt(1/a + 1/b + 1/c + 1/d)
    95% CI = exp(ln(ROR) +/- z * SE)
    """
    a_c, b_c, c_c, d_c, zero_corrected = calculate_contingency_counts(
        a, b, c, d, zero_cell_correction=zero_cell_correction
    )

    ror = (a_c * d_c) / (b_c * c_c)
    se_ln_ror = math.sqrt((1.0 / a_c) + (1.0 / b_c) + (1.0 / c_c) + (1.0 / d_c))
    ln_ror = math.log(ror)

    ci_lower = math.exp(ln_ror - z_score * se_ln_ror)
    ci_upper = math.exp(ln_ror + z_score * se_ln_ror)

    return {
        "ror": round(ror, 4),
        "ror_ci_lower": round(ci_lower, 4),
        "ror_ci_upper": round(ci_upper, 4),
        "se_ln_ror": round(se_ln_ror, 4),
        "zero_corrected": zero_corrected
    }


def calculate_prr(
    a: int | float,
    b: int | float,
    c: int | float,
    d: int | float,
    z_score: float = 1.959963984540054,
    zero_cell_correction: float = 0.5
) -> Dict[str, Any]:
    """
    Calculates Proportional Reporting Ratio (PRR), its standard error, 95% CI,
    and Chi-Square with Yates' continuity correction.
    
    PRR = [a / (a + b)] / [c / (c + d)]
    SE(ln(PRR)) = sqrt(1/a - 1/(a+b) + 1/c - 1/(c+d))
    Chi2_Yates = [N * (|a*d - b*c| - N/2)^2] / [(a+b) * (c+d) * (a+c) * (b+d)]
    """
    a_c, b_c, c_c, d_c, zero_corrected = calculate_contingency_counts(
        a, b, c, d, zero_cell_correction=zero_cell_correction
    )

    drug_total = a_c + b_c
    non_drug_total = c_c + d_c
    prr = (a_c / drug_total) / (c_c / non_drug_total)

    # Standard error for PRR
    variance_ln_prr = (1.0 / a_c) - (1.0 / drug_total) + (1.0 / c_c) - (1.0 / non_drug_total)
    variance_ln_prr = max(0.0, variance_ln_prr)
    se_ln_prr = math.sqrt(variance_ln_prr)
    ln_prr = math.log(prr)

    ci_lower = math.exp(ln_prr - z_score * se_ln_prr)
    ci_upper = math.exp(ln_prr + z_score * se_ln_prr)

    # Chi-square with Yates continuity correction (using original raw counts)
    n = float(a + b + c + d)
    ad_bc = abs(float(a) * float(d) - float(b) * float(c))
    numerator = n * (max(0.0, ad_bc - (n / 2.0)) ** 2)
    denominator = (a + b) * (c + d) * (a + c) * (b + d)

    chi2 = (numerator / denominator) if denominator > 0 else 0.0

    return {
        "prr": round(prr, 4),
        "prr_ci_lower": round(ci_lower, 4),
        "prr_ci_upper": round(ci_upper, 4),
        "se_ln_prr": round(se_ln_prr, 4),
        "chi2_yates": round(chi2, 4),
        "zero_corrected": zero_corrected
    }


SIGNAL_STATUS = "POTENTIAL_SIGNAL"
NOT_FLAGGED_STATUS = "NOT_FLAGGED"


def evaluate_screening_status(
    observed_cases: int,
    ror_ci_lower: float,
    prr: float,
    min_cases: int = 3,
    min_ror_ci_lower: float = 1.0,
    min_prr: float = 2.0,
    chi2: Optional[float] = None,
    min_chi2: Optional[float] = None
) -> tuple[str, list[str]]:
    """
    Evaluates a drug-event pair against the configured project screening criteria
    (a >= min_cases, ROR lower CI > min_ror_ci_lower, PRR >= min_prr, and chi2 >= min_chi2 when set).
    These are screening choices, not clinical standards.
    Returns (status, reasons). Status is 'POTENTIAL_SIGNAL' or 'NOT_FLAGGED'.
    """
    reasons = []
    if observed_cases < min_cases:
        reasons.append(f"Observed cases ({observed_cases}) < threshold ({min_cases})")
    if ror_ci_lower <= min_ror_ci_lower:
        reasons.append(f"ROR 95% CI lower ({ror_ci_lower:.4g}) <= threshold ({min_ror_ci_lower})")
    if prr < min_prr:
        reasons.append(f"PRR ({prr:.4g}) < threshold ({min_prr})")
    if min_chi2 is not None and chi2 is not None and chi2 < min_chi2:
        reasons.append(f"Chi2 Yates ({chi2:.4g}) < threshold ({min_chi2})")

    status = SIGNAL_STATUS if len(reasons) == 0 else NOT_FLAGGED_STATUS
    return status, reasons
