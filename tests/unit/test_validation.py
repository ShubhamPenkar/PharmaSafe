import pytest

from src.validation.benchmark_evaluator import (
    classify_reference, jaccard, reference_outcome, render_validation_report,
)


@pytest.mark.parametrize("row,drug,reac,expected", [
    ({"screening_status": "POTENTIAL_SIGNAL", "observed_cases": 40}, True, True, "DETECTED"),
    ({"screening_status": "NOT_FLAGGED", "observed_cases": 40}, True, True, "PRESENT_NOT_FLAGGED"),
    ({"screening_status": "NOT_FLAGGED", "observed_cases": 2}, True, True, "INSUFFICIENT_CASES"),
    (None, False, True, "DRUG_NOT_IN_DATA"),
    (None, True, False, "REACTION_NOT_IN_DATA"),
    (None, True, True, "INSUFFICIENT_CASES"),
])
def test_classify_reference(row, drug, reac, expected):
    assert classify_reference(row, drug, reac, min_cases=3) == expected


def test_reference_outcomes_distinguish_missed_from_not_evaluable():
    assert reference_outcome(True, "DETECTED") == "RECOVERED"
    assert reference_outcome(True, "PRESENT_NOT_FLAGGED") == "MISSED"
    assert reference_outcome(True, "DRUG_NOT_IN_DATA") == "NOT_EVALUABLE"
    assert reference_outcome(False, "DETECTED") == "FALSE_POSITIVE"
    assert reference_outcome(False, "PRESENT_NOT_FLAGGED") == "CORRECTLY_NOT_FLAGGED"
    assert reference_outcome(False, "INSUFFICIENT_CASES") == "NOT_EVALUABLE"


def test_jaccard():
    assert jaccard({1, 2}, {2, 3}) == pytest.approx(1 / 3)
    assert jaccard(set(), set()) == 1.0


def test_report_renders_rows_with_and_without_statistics():
    result = {
        "generated_at": "t", "snapshot": "latest",
        "references": [
            {"drug_name": "A", "reaction_terms": ["R"], "expected_signal": True, "status": "DETECTED", "outcome": "RECOVERED",
             "matched_reaction": "R", "observed_cases": 9, "ROR": 3.0, "ROR_CI_lower": 2.0, "ROR_CI_upper": 4.5, "PRR": 2.5},
            {"drug_name": "B", "reaction_terms": ["S"], "expected_signal": False, "status": "DRUG_NOT_IN_DATA", "outcome": "NOT_EVALUABLE"},
        ],
        "robustness": {"baseline": "k", "baseline_flagged_pairs": 1, "grid": []},
    }
    md = render_validation_report(result)
    assert "| A | R | signal | DETECTED | RECOVERED | 9 | 3.00 (2.00–4.50) | 2.50 |" in md
    assert "| B | S | negative control | DRUG_NOT_IN_DATA | NOT_EVALUABLE | — | — | — |" in md
    assert "NOT_EVALUABLE = 1" in md
