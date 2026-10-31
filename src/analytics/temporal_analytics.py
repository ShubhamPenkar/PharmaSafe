"""
PharmaSafe Temporal Safety Analytics

Quarter = the source (FDA publication) quarter of a case's canonical report version. It is NOT the
event date, the report receipt date, or the extraction date; those are carried separately in
case_summary (event_dt_date, fda_dt_date). A case belongs to exactly one quarter in a snapshot, so
quarters partition the case population and per-quarter statistics never double-count a case.

Outputs under data/gold/<snapshot>/temporal/:
  quarterly_reaction_counts   cases per (report_quarter, reaction)
  quarterly_signals           ROR / PRR / screening per (report_quarter, drug, reaction)
  signal_persistence          per pair: quarters observed, quarters flagged, first/last flagged quarter

Changes in these counts describe reporting patterns, not changes in clinical incidence.
"""

import sys
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array_join, col, collect_list, count, lit, max as spark_max, min as spark_min, sort_array, struct,
    sum as spark_sum, when,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_analysis_config, load_data_sources_config, resolve_path
from src.utils.spark_session import get_spark_session
from src.signal_detection.metrics import SIGNAL_STATUS
from src.signal_detection.signal_engine import compute_safety_signals, screening_policies, Z_95

logger = get_logger("PharmaSafe.Temporal")


def quarterly_contingency(
    case_drug: DataFrame, case_reaction: DataFrame, case_summary: DataFrame,
    target_roles: list, min_cases: int
) -> DataFrame:
    """Per-quarter drug-event 2x2 counts restricted to the cases reported in that quarter."""
    cond = None
    for r in target_roles:
        c = col(f"is_{r.lower()}")
        cond = c if cond is None else (cond | c)
    quarter_of = case_summary.select("caseid", col("report_quarter").alias("period"))
    drugs = case_drug.filter(cond).select("caseid", "drug_key").distinct()
    reactions = case_reaction.select("caseid", "pt").distinct()
    eligible = drugs.select("caseid").distinct().join(reactions.select("caseid").distinct(), "caseid").join(quarter_of, "caseid")
    drugs, reactions = drugs.join(eligible, "caseid"), reactions.join(eligible, "caseid")

    n_q = eligible.groupBy("period").agg(count(lit(1)).alias("n_eligible_cases"))
    drug_tot = drugs.groupBy("period", "drug_key").agg(count(lit(1)).alias("n_drug"))
    event_tot = reactions.groupBy("period", "pt").agg(count(lit(1)).alias("n_event"))
    pairs = (
        drugs.join(reactions.drop("period"), "caseid")
        .groupBy("period", "drug_key", "pt").agg(count(lit(1)).alias("a"))
        .filter(col("a") >= min_cases)
    )
    return (
        pairs.join(drug_tot, ["period", "drug_key"]).join(event_tot, ["period", "pt"]).join(n_q, "period")
        .withColumn("b", col("n_drug") - col("a"))
        .withColumn("c", col("n_event") - col("a"))
        .withColumn("d", col("n_eligible_cases") - col("n_drug") - col("n_event") + col("a"))
    )


def signal_persistence(quarterly_signals: DataFrame) -> DataFrame:
    """Summarizes, per drug-event pair, in which quarters the screening criteria were met."""
    flagged = col("screening_status") == SIGNAL_STATUS
    return (
        quarterly_signals.groupBy("drug_name", "reaction_term")
        .agg(
            count(lit(1)).alias("quarters_observed"),
            spark_sum(flagged.cast("int")).alias("quarters_flagged"),
            spark_min(when(flagged, col("analysis_period"))).alias("first_flagged_quarter"),
            spark_max(when(flagged, col("analysis_period"))).alias("last_flagged_quarter"),
            array_join(sort_array(collect_list(when(flagged, col("analysis_period")))), ",").alias("flagged_quarters"),
            spark_sum("observed_cases").alias("total_observed_cases"),
        )
        .withColumn("persistence_ratio", col("quarters_flagged") / col("quarters_observed"))
    )


class TemporalAnalytics:

    def __init__(self, data_config=None, analysis_config=None, spark: Optional[SparkSession] = None):
        self.data_config = data_config or load_data_sources_config()
        self.analysis_config = analysis_config or load_analysis_config()
        self.gold_root = resolve_path(self.data_config.get("storage", {}).get("gold_dir", "data/gold"))
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = get_spark_session(app_name="PharmaSafe.Temporal")
        return self._spark

    def run(self, snapshot: str = "latest", policy: str = "primary") -> Dict[str, Any]:
        base = self.gold_root / snapshot
        read = lambda name: self.spark.read.parquet(str(base / name))
        params = screening_policies(self.analysis_config)[policy]
        tcfg = self.analysis_config.get("temporal", {})
        stats = self.analysis_config.get("statistics", {})
        min_cases = tcfg.get("min_cases_per_quarter", params.get("min_cases", 3))
        out = base / "temporal"

        case_summary, case_drug, case_reaction = read("case_summary"), read("case_drug"), read("case_reaction")
        counts = quarterly_contingency(case_drug, case_reaction, case_summary, params.get("target_roles", ["PS"]), min_cases)
        signals = compute_safety_signals(
            counts, role_policy=policy, min_cases=min_cases,
            min_ror_ci_lower=params.get("min_ror_ci_lower", 1.0), min_prr=params.get("min_prr", 2.0), min_chi2=params.get("min_chi2"),
            method_version=self.analysis_config.get("methodology", {}).get("version", "1.0.0"),
            z=stats.get("z_score", Z_95), zero_cell_correction=stats.get("zero_cell_correction", 0.5),
        )
        signals.write.mode("overwrite").parquet(str(out / "quarterly_signals"))
        qs = self.spark.read.parquet(str(out / "quarterly_signals"))
        signal_persistence(qs).write.mode("overwrite").parquet(str(out / "signal_persistence"))
        (case_reaction.join(case_summary.select("caseid", "report_quarter"), "caseid")
         .groupBy("report_quarter", "pt").agg(count(lit(1)).alias("cases"))
         .write.mode("overwrite").parquet(str(out / "quarterly_reaction_counts")))

        summary = qs.agg(count(lit(1)).alias("pair_quarters"),
                         spark_sum((col("screening_status") == SIGNAL_STATUS).cast("int")).alias("flagged_pair_quarters")).collect()[0].asDict()
        logger.info("Temporal analytics [%s/%s]: %s", snapshot, policy, summary)
        return {"snapshot": snapshot, "role_policy": policy, **summary}
