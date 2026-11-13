"""
PharmaSafe Pharmacovigilance Signal Engine
Builds case-level 2x2 contingency tables for every drug-event pair and computes disproportionality
measures (ROR, PRR, 95% CIs, Yates chi-square) as native Spark column expressions, so the work stays
in the JVM executors instead of a per-row Python UDF. src/signal_detection/metrics.py is the scalar
reference implementation; tests assert both produce the same numbers.

Reference population (documented in docs/methodology_and_counting_rules.md):
  An eligible case is a canonical case with at least one drug in the selected role policy AND at
  least one reported reaction. N is the number of eligible cases; drug and reaction counts are taken
  over the same eligible cases, so a + b + c + d == N for every pair by construction.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    abs as spark_abs, array, array_compact, col, concat, count, exp, format_number, greatest, lit,
    log, size, sqrt, when,
)

from src.signal_detection.metrics import SIGNAL_STATUS, NOT_FLAGGED_STATUS
from src.utils.logger import get_logger

logger = get_logger("PharmaSafe.SignalEngine")

Z_95 = 1.959963984540054


def eligible_population(
    case_drug: DataFrame,
    case_reaction: DataFrame,
    target_roles: List[str]
) -> Tuple[DataFrame, DataFrame, int]:
    """
    Restricts drugs to the role policy and both sides to the eligible case population.
    case_drug needs (caseid, drug_key, roles: array<string>); case_reaction needs (caseid, pt).
    Returns (eligible_drugs[caseid, drug_key], eligible_reactions[caseid, pt], N).
    """
    roles = [r.upper() for r in target_roles]
    role_match = None
    for r in roles:
        cond = col(f"is_{r.lower()}")
        role_match = cond if role_match is None else (role_match | cond)

    drugs = case_drug.filter(role_match).select("caseid", "drug_key").distinct()
    reactions = case_reaction.select("caseid", "pt").distinct()
    eligible = drugs.select("caseid").distinct().join(reactions.select("caseid").distinct(), on="caseid", how="inner")

    eligible = eligible.cache()
    n = eligible.count()
    drugs = drugs.join(eligible, on="caseid", how="inner")
    reactions = reactions.join(eligible, on="caseid", how="inner")
    eligible.unpersist()
    return drugs, reactions, n


def build_contingency_table(
    eligible_drugs: DataFrame,
    eligible_reactions: DataFrame,
    n_eligible: int,
    min_observed_cases: int = 1
) -> DataFrame:
    """
    a = cases with drug and event, b = drug without event, c = event without drug, d = neither.
    Inputs are already unique per (caseid, drug_key) and (caseid, pt), so no row explosion can
    inflate a: each case contributes at most once to each pair.
    """
    drug_totals = eligible_drugs.groupBy("drug_key").agg(count("caseid").alias("n_drug"))
    event_totals = eligible_reactions.groupBy("pt").agg(count("caseid").alias("n_event"))
    pairs = (
        eligible_drugs.join(eligible_reactions, on="caseid", how="inner")
        .groupBy("drug_key", "pt")
        .agg(count("caseid").alias("a"))
        .filter(col("a") >= min_observed_cases)
    )
    return (
        pairs
        .join(drug_totals, on="drug_key", how="inner")
        .join(event_totals, on="pt", how="inner")
        .withColumn("b", col("n_drug") - col("a"))
        .withColumn("c", col("n_event") - col("a"))
        .withColumn("d", lit(n_eligible) - col("n_drug") - col("n_event") + col("a"))
        .withColumn("n_eligible_cases", lit(n_eligible))
    )


def _corrected(c: str, any_zero: Column, correction: float) -> Column:
    return col(c).cast("double") + when(any_zero, lit(correction)).otherwise(lit(0.0))


def disproportionality_columns(z: float = Z_95, zero_cell_correction: float = 0.5) -> Dict[str, Column]:
    """Column expressions mirroring metrics.calculate_ror / calculate_prr on (a, b, c, d)."""
    any_zero = (col("a") == 0) | (col("b") == 0) | (col("c") == 0) | (col("d") == 0)
    a, b, c, d = (_corrected(x, any_zero, zero_cell_correction) for x in ("a", "b", "c", "d"))

    ror = (a * d) / (b * c)
    se_ln_ror = sqrt(1.0 / a + 1.0 / b + 1.0 / c + 1.0 / d)
    prr = (a / (a + b)) / (c / (c + d))
    se_ln_prr = sqrt(greatest(lit(0.0), 1.0 / a - 1.0 / (a + b) + 1.0 / c - 1.0 / (c + d)))

    # Yates chi-square on the uncorrected counts, as in metrics.calculate_prr
    ra, rb, rc, rd = (col(x).cast("double") for x in ("a", "b", "c", "d"))
    n = ra + rb + rc + rd
    numerator = n * greatest(lit(0.0), spark_abs(ra * rd - rb * rc) - n / 2.0) ** 2
    denominator = (ra + rb) * (rc + rd) * (ra + rc) * (rb + rd)

    return {
        "zero_corrected": any_zero,
        "expected_cases": (ra + rb) * (ra + rc) / n,
        "ROR": ror,
        "ROR_CI_lower": exp(log(ror) - z * se_ln_ror),
        "ROR_CI_upper": exp(log(ror) + z * se_ln_ror),
        "se_ln_ROR": se_ln_ror,
        "PRR": prr,
        "PRR_CI_lower": exp(log(prr) - z * se_ln_prr),
        "PRR_CI_upper": exp(log(prr) + z * se_ln_prr),
        "se_ln_PRR": se_ln_prr,
        "chi2_yates": when(denominator > 0, numerator / denominator).otherwise(lit(0.0)),
    }


def compute_safety_signals(
    contingency_df: DataFrame,
    analysis_period: str = "ALL",
    role_policy: str = "primary",
    min_cases: int = 3,
    min_ror_ci_lower: float = 1.0,
    min_prr: float = 2.0,
    min_chi2: Optional[float] = None,
    method_version: str = "1.0.0",
    z: float = Z_95,
    zero_cell_correction: float = 0.5
) -> DataFrame:
    """Applies disproportionality statistics and the configured screening criteria to every pair."""
    stats = disproportionality_columns(z=z, zero_cell_correction=zero_cell_correction)
    df = contingency_df
    for name, expression in stats.items():
        df = df.withColumn(name, expression)

    def fmt(c: str) -> Column:
        return format_number(col(c), 4)

    failed = [
        when(col("a") < min_cases, concat(lit("Observed cases ("), col("a").cast("string"), lit(f") < threshold ({min_cases})"))),
        when(col("ROR_CI_lower") <= min_ror_ci_lower, concat(lit("ROR 95% CI lower ("), fmt("ROR_CI_lower"), lit(f") <= threshold ({min_ror_ci_lower})"))),
        when(col("PRR") < min_prr, concat(lit("PRR ("), fmt("PRR"), lit(f") < threshold ({min_prr})"))),
    ]
    if min_chi2 is not None:
        failed.append(when(col("chi2_yates") < min_chi2, concat(lit("Chi2 Yates ("), fmt("chi2_yates"), lit(f") < threshold ({min_chi2})"))))

    return (
        df
        .withColumn("screening_reasons", array_compact(array(*failed)))
        .withColumn("screening_status", when(size(col("screening_reasons")) == 0, lit(SIGNAL_STATUS)).otherwise(lit(NOT_FLAGGED_STATUS)))
        .select(
            col("drug_key").alias("drug_name"),
            col("pt").alias("reaction_term"),
            (col("period") if "period" in contingency_df.columns else lit(analysis_period)).alias("analysis_period"),
            lit(role_policy).alias("role_policy"),
            col("a").alias("observed_cases"),
            "expected_cases",
            col("c").alias("background_cases"),
            "a", "b", "c", "d",
            col("n_drug").alias("drug_cases"),
            col("n_event").alias("reaction_cases"),
            "n_eligible_cases",
            "ROR", "ROR_CI_lower", "ROR_CI_upper", "se_ln_ROR",
            "PRR", "PRR_CI_lower", "PRR_CI_upper", "se_ln_PRR",
            "chi2_yates", "zero_corrected",
            "screening_status", "screening_reasons",
            lit(method_version).alias("method_version"),
            lit(datetime.now(timezone.utc).isoformat()).alias("analysis_timestamp"),
        )
    )


def screening_policies(analysis_config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Expands configs/analysis.yaml screening_criteria into one parameter set per role policy.
    Sensitivity policies inherit every threshold from 'primary' unless they override it.
    """
    criteria = analysis_config.get("screening_criteria", {})
    primary = dict(criteria.get("primary", {}))
    policies = {"primary": primary}
    for name, overrides in (criteria.get("sensitivity") or {}).items():
        policies[name] = {**primary, **overrides}
    return policies
