"""
PharmaSafe Safety Signal Validation

1. Reference-signal evaluation against configs/reference_signals.yaml. Each reference pair is
   classified, never simply pass/fail, because absence in one reporting window is not an error:
     DETECTED               present and meets the screening criteria
     PRESENT_NOT_FLAGGED    cases exist but screening criteria are not met
     INSUFFICIENT_CASES     fewer cases than the minimum, or the pair is not stored
     DRUG_NOT_IN_DATA / REACTION_NOT_IN_DATA   the term never occurs in the analysis population
   For negative controls the expected outcome is not DETECTED.
2. Robustness: how the flagged set changes across min-case and PRR thresholds and role policies.
3. Temporal validation: signals flagged in the discovery quarters re-evaluated in later quarters.
   The split is by source quarter, and a case belongs to one quarter, so no case is on both sides.
"""

import json
import sys
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import (
    load_analysis_config, load_data_sources_config, load_reference_signals, resolve_path,
)
from src.utils.spark_session import get_spark_session
from src.signal_detection.metrics import SIGNAL_STATUS
from src.signal_detection.signal_engine import compute_safety_signals, screening_policies, Z_95

logger = get_logger("PharmaSafe.Validation")


def classify_reference(row: Optional[Dict[str, Any]], drug_present: bool, reaction_present: bool, min_cases: int) -> str:
    if row is None:
        if not drug_present:
            return "DRUG_NOT_IN_DATA"
        if not reaction_present:
            return "REACTION_NOT_IN_DATA"
        return "INSUFFICIENT_CASES"
    if row["screening_status"] == SIGNAL_STATUS:
        return "DETECTED"
    return "INSUFFICIENT_CASES" if row["observed_cases"] < min_cases else "PRESENT_NOT_FLAGGED"


def reference_outcome(expected_signal: bool, status: str) -> str:
    """Whether the evaluation agrees with the reference expectation, or is simply uninformative."""
    uninformative = {"DRUG_NOT_IN_DATA", "REACTION_NOT_IN_DATA", "INSUFFICIENT_CASES"}
    if expected_signal:
        return "RECOVERED" if status == "DETECTED" else ("NOT_EVALUABLE" if status in uninformative else "MISSED")
    return "FALSE_POSITIVE" if status == "DETECTED" else ("NOT_EVALUABLE" if status in uninformative else "CORRECTLY_NOT_FLAGGED")


def jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if (a | b) else 1.0


class SignalValidator:

    def __init__(self, data_config=None, analysis_config=None, spark: Optional[SparkSession] = None):
        self.data_config = data_config or load_data_sources_config()
        self.analysis_config = analysis_config or load_analysis_config()
        self.gold_root = resolve_path(self.data_config.get("storage", {}).get("gold_dir", "data/gold"))
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = get_spark_session(app_name="PharmaSafe.Validation")
        return self._spark

    def evaluate_references(self, snapshot: str = "latest", policy: str = "primary", references: Optional[List[Dict]] = None) -> List[Dict[str, Any]]:
        base = self.gold_root / snapshot
        refs = references if references is not None else load_reference_signals()["reference_signals"]
        min_cases = screening_policies(self.analysis_config)[policy].get("min_cases", 3)
        signals = self.spark.read.parquet(str(base / f"safety_signals/role_policy={policy}"))
        drugs = self.spark.read.parquet(str(base / "drug_summary"))
        reactions = self.spark.read.parquet(str(base / "reaction_summary"))
        results = []
        for ref in refs:
            drug = ref["drug_name"].upper()
            terms = [t.upper() for t in ref.get("reaction_terms", [ref["reaction_term"]])]
            hits = signals.filter((col("drug_name") == drug) & col("reaction_term").isin(terms)).orderBy(col("observed_cases").desc()).limit(1).collect()
            row = hits[0].asDict() if hits else None
            status = classify_reference(
                row,
                drug_present=drugs.filter(col("drug_key") == drug).limit(1).count() > 0,
                reaction_present=reactions.filter(col("pt").isin(terms)).limit(1).count() > 0,
                min_cases=min_cases,
            )
            results.append({
                "drug_name": drug, "reaction_terms": terms, "expected_signal": ref["expected_signal"],
                "evidence_source": ref.get("evidence_source"), "regulatory_action": ref.get("regulatory_action"),
                "status": status, "outcome": reference_outcome(ref["expected_signal"], status),
                "matched_reaction": row["reaction_term"] if row else None,
                **({k: row[k] for k in ("observed_cases", "ROR", "ROR_CI_lower", "ROR_CI_upper", "PRR")} if row else {}),
            })
        return results

    def robustness(self, snapshot: str = "latest") -> Dict[str, Any]:
        """Re-screens stored contingency counts under a grid of thresholds and role policies."""
        base = self.gold_root / snapshot
        grid = self.analysis_config.get("robustness", {})
        stats = self.analysis_config.get("statistics", {})
        policies = screening_policies(self.analysis_config)
        primary = policies["primary"]
        baseline_key = None
        flagged_sets: Dict[str, set] = {}
        counts_cache: Dict[str, DataFrame] = {}
        for policy in policies:
            counts_cache[policy] = self.spark.read.parquet(str(base / f"drug_event_counts/role_policy={policy}"))

        rows = []
        for policy, min_cases, min_prr in product(policies, grid.get("min_cases", [3]), grid.get("min_prr", [2.0])):
            params = policies[policy]
            sig = compute_safety_signals(
                counts_cache[policy].filter(col("a") >= min_cases), analysis_period=snapshot, role_policy=policy,
                min_cases=min_cases, min_ror_ci_lower=params.get("min_ror_ci_lower", 1.0), min_prr=min_prr,
                min_chi2=params.get("min_chi2"), z=stats.get("z_score", Z_95),
                zero_cell_correction=stats.get("zero_cell_correction", 0.5),
            ).filter(col("screening_status") == SIGNAL_STATUS)
            keys = {(r["drug_name"], r["reaction_term"]) for r in sig.select("drug_name", "reaction_term").collect()}
            key = f"{policy}|min_cases={min_cases}|min_prr={min_prr}"
            flagged_sets[key] = keys
            if policy == "primary" and min_cases == primary.get("min_cases", 3) and min_prr == primary.get("min_prr", 2.0):
                baseline_key = key
            rows.append({"role_policy": policy, "min_cases": min_cases, "min_prr": min_prr, "flagged_pairs": len(keys), "_key": key})
        baseline = flagged_sets.get(baseline_key, set())
        for r in rows:
            k = r.pop("_key")
            r["overlap_with_baseline_jaccard"] = round(jaccard(flagged_sets[k], baseline), 4)
            r["retained_from_baseline_pct"] = round(100.0 * len(flagged_sets[k] & baseline) / len(baseline), 2) if baseline else None
        return {"baseline": baseline_key, "baseline_flagged_pairs": len(baseline), "grid": rows}

    def temporal_validation(self, snapshot: str = "latest", policy: str = "primary") -> Dict[str, Any]:
        """Discovery quarters -> evaluation quarters, on the stored per-quarter signals."""
        t = self.analysis_config.get("temporal", {})
        discovery, evaluation = t.get("discovery_quarters", []), t.get("evaluation_quarters", [])
        qs = self.spark.read.parquet(str(self.gold_root / snapshot / "temporal/quarterly_signals"))
        flagged = qs.filter(col("screening_status") == SIGNAL_STATUS)
        in_disc = flagged.filter(col("analysis_period").isin(discovery)).select("drug_name", "reaction_term").distinct()
        in_eval = flagged.filter(col("analysis_period").isin(evaluation)).select("drug_name", "reaction_term").distinct()
        # Evaluable = pair has at least min_cases in some evaluation quarter (otherwise absence is uninformative)
        observed_eval = qs.filter(col("analysis_period").isin(evaluation)).select("drug_name", "reaction_term").distinct()
        disc_n = in_disc.count()
        evaluable = in_disc.join(observed_eval, ["drug_name", "reaction_term"]).count()
        confirmed = in_disc.join(in_eval, ["drug_name", "reaction_term"]).count()
        return {
            "discovery_quarters": discovery, "evaluation_quarters": evaluation,
            "flagged_in_discovery": disc_n, "evaluable_in_evaluation": evaluable, "re_flagged_in_evaluation": confirmed,
            "confirmation_rate_of_evaluable_pct": round(100.0 * confirmed / evaluable, 2) if evaluable else None,
            "new_in_evaluation_not_in_discovery": in_eval.join(in_disc, ["drug_name", "reaction_term"], "left_anti").count(),
        }

    def run(self, snapshot: str = "latest", write_reports: bool = True) -> Dict[str, Any]:
        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(), "snapshot": snapshot,
            "references": self.evaluate_references(snapshot),
            "robustness": self.robustness(snapshot),
        }
        try:
            result["temporal_validation"] = self.temporal_validation(snapshot)
        except Exception as ex:  # temporal outputs may not have been built
            logger.warning("Temporal validation skipped: %s", ex)
        if write_reports:
            out = resolve_path("reports")
            out.mkdir(exist_ok=True)
            (out / "validation_summary.json").write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
            (out / "validation_report.md").write_text(render_validation_report(result), encoding="utf-8")
        return result


def render_validation_report(r: Dict[str, Any]) -> str:
    refs = r["references"]
    lines = [
        "# PharmaSafe Safety Signal Validation Report",
        f"Generated: {r['generated_at']}  |  Snapshot: `{r['snapshot']}`\n",
        "> Screening criteria are project choices, not clinical standards. A reference pair that is not recovered "
        "does not show the method failed: the reporting window, drug-name mapping and case counts all matter. "
        "Reference pairs were not tuned against this output.\n",
        "## 1. Reference-signal evaluation (primary-suspect policy)\n",
        "| Drug | Reaction | Expected | Status | Outcome | Cases | ROR (95% CI) | PRR |",
        "| :--- | :--- | :---: | :--- | :--- | ---: | :--- | ---: |",
    ]
    for x in refs:
        has_stats = "ROR" in x
        ror = f"{x['ROR']:.2f} ({x['ROR_CI_lower']:.2f}–{x['ROR_CI_upper']:.2f})" if has_stats else "—"
        prr = f"{x['PRR']:.2f}" if has_stats else "—"
        reaction = x.get("matched_reaction") or "/".join(x["reaction_terms"])
        expected = "signal" if x["expected_signal"] else "negative control"
        lines.append(f"| {x['drug_name']} | {reaction} | {expected} | {x['status']} | {x['outcome']} | "
                     f"{x.get('observed_cases', '—')} | {ror} | {prr} |")
    tally: Dict[str, int] = {}
    for x in refs:
        tally[x["outcome"]] = tally.get(x["outcome"], 0) + 1
    lines += ["", "Outcome counts: " + ", ".join(f"{k} = {v}" for k, v in sorted(tally.items())), ""]

    rb = r["robustness"]
    lines += ["## 2. Robustness to screening parameters\n",
              f"Baseline `{rb['baseline']}`: {rb['baseline_flagged_pairs']:,} flagged pairs.\n",
              "| Role policy | Min cases | Min PRR | Flagged pairs | Jaccard vs baseline | Baseline retained (%) |",
              "| :--- | ---: | ---: | ---: | ---: | ---: |"]
    for g in rb["grid"]:
        lines.append(f"| {g['role_policy']} | {g['min_cases']} | {g['min_prr']} | {g['flagged_pairs']:,} | "
                     f"{g['overlap_with_baseline_jaccard']} | {g['retained_from_baseline_pct']} |")
    tv = r.get("temporal_validation")
    if tv:
        lines += ["", "## 3. Temporal validation\n",
                  f"Discovery: {', '.join(tv['discovery_quarters'])}. Evaluation: {', '.join(tv['evaluation_quarters'])}. "
                  "Cases belong to a single quarter, so no case appears on both sides.\n",
                  f"- Pairs flagged in discovery: **{tv['flagged_in_discovery']:,}**",
                  f"- Of those, with enough cases to be evaluated later: **{tv['evaluable_in_evaluation']:,}**",
                  f"- Re-flagged in the evaluation quarters: **{tv['re_flagged_in_evaluation']:,}** "
                  f"({tv['confirmation_rate_of_evaluable_pct']}% of evaluable)",
                  f"- Newly flagged only in evaluation: {tv['new_in_evaluation_not_in_discovery']:,}"]
    return "\n".join(lines) + "\n"
