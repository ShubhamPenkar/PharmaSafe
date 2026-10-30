"""
Plain-language explanation of a candidate drug-event signal, shared by the API and the dashboard.
Every explanation states the counting rules and what the statistics cannot establish.
"""

from typing import Any, Dict, List

LIMITATIONS = [
    "A disproportionate reporting pattern is not evidence that the drug caused the reaction.",
    "Spontaneous reports have no exposure denominator, so incidence or patient risk cannot be derived.",
    "Reporting is affected by under-reporting, stimulated reporting after safety communications, and confounding by indication.",
    "Drug co-reporting in one case does not demonstrate a pharmacological interaction.",
]

ROLE_POLICY_TEXT = {
    "primary": "drugs reported as primary suspect (PS)",
    "all_suspect": "drugs reported as primary or secondary suspect (PS, SS)",
    "all_reported": "all reported drugs (PS, SS, C, I)",
}


def explain_signal(s: Dict[str, Any]) -> Dict[str, Any]:
    """Builds the explanation for one row of safety_signals (dict with the Gold column names)."""
    a, b, c, d = (int(s[k]) for k in ("a", "b", "c", "d"))
    n = a + b + c + d
    flagged = s["screening_status"] == "POTENTIAL_SIGNAL"
    policy = ROLE_POLICY_TEXT.get(s.get("role_policy", "primary"), s.get("role_policy", ""))
    drug, reaction = s["drug_name"], s["reaction_term"]

    facts: List[str] = [
        f"{a:,} of {a + b:,} eligible cases reporting {drug} also reported {reaction} ({100 * a / (a + b):.2f}%).",
        f"Among the {c + d:,} eligible cases without {drug}, {c:,} reported {reaction} ({100 * c / (c + d):.2f}%)."
        if (c + d) else "No eligible cases without this drug.",
        f"ROR {s['ROR']:.2f} (95% CI {s['ROR_CI_lower']:.2f} to {s['ROR_CI_upper']:.2f}); PRR {s['PRR']:.2f}.",
    ]
    if s.get("zero_corrected"):
        facts.append("A zero cell was present, so 0.5 was added to every cell (Haldane-Anscombe) before computing ratios.")

    if flagged:
        headline = f"Potential safety signal: {drug} and {reaction} are reported together more often than the reporting background."
    else:
        reasons = s.get("screening_reasons") or []
        headline = f"Not flagged: {drug} with {reaction} did not meet the screening criteria" + (f" ({'; '.join(reasons)})." if reasons else ".")

    return {
        "headline": headline,
        "status": s["screening_status"],
        "what_the_numbers_mean": facts,
        "counting_rules": [
            f"Unit of analysis: unique eligible cases (latest report version per case), N = {n:,}.",
            f"Drug exposure counted from {policy}; each case counts once per drug and once per reaction term.",
            "Eligible case: at least one qualifying drug and at least one reported reaction.",
            f"Method version {s.get('method_version', '?')}, analysis period {s.get('analysis_period', '?')}.",
        ],
        "cannot_establish": LIMITATIONS,
    }
