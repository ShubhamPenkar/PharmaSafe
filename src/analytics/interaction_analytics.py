"""
PharmaSafe Drug-Drug Combination (co-reporting) Analytics

For drug pairs co-reported in the same eligible case, tests whether a reaction is reported
disproportionately in those cases against all other eligible cases:
  a = cases with BOTH drugs and the reaction     b = cases with both drugs without the reaction
  c = other cases with the reaction              d = other cases without it
Pair order is canonical (drug_1 < drug_2) so each unordered pair is counted once.

Co-reporting is not an established pharmacological interaction. As context, each row carries the
single-drug ROR of both components (when available) and a flag saying whether the combination's ROR
exceeds both; that flag is a screening aid, not evidence of interaction.

Outputs under data/gold/<snapshot>/combinations/:
  drug_combinations          pair frequency and co-reporting lift vs independence
  combination_signals        ROR / PRR / screening per (drug pair, reaction)
"""

import sys
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col, concat_ws, count, lit, row_number, when
from pyspark.sql import Window

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_analysis_config, load_data_sources_config, resolve_path
from src.utils.spark_session import get_spark_session
from src.signal_detection.metrics import SIGNAL_STATUS
from src.signal_detection.signal_engine import compute_safety_signals, eligible_population, screening_policies, Z_95

logger = get_logger("PharmaSafe.Combinations")


def pair_frequencies(drugs: DataFrame, n_eligible: int, min_pair_cases: int, limit: int) -> DataFrame:
    """Co-reported drug pairs with counts and lift = observed / expected-under-independence pair cases."""
    left, right = drugs.withColumnRenamed("drug_key", "drug_1"), drugs.withColumnRenamed("drug_key", "drug_2")
    pairs = (
        left.join(right, "caseid").filter(col("drug_1") < col("drug_2"))
        .groupBy("drug_1", "drug_2").agg(count(lit(1)).alias("pair_cases"))
        .filter(col("pair_cases") >= min_pair_cases)
    )
    totals = drugs.groupBy("drug_key").agg(count(lit(1)).alias("n"))
    ranked = (
        pairs.join(totals.withColumnRenamed("drug_key", "drug_1").withColumnRenamed("n", "cases_drug_1"), "drug_1")
        .join(totals.withColumnRenamed("drug_key", "drug_2").withColumnRenamed("n", "cases_drug_2"), "drug_2")
        .withColumn("expected_pair_cases", col("cases_drug_1") * col("cases_drug_2") / lit(float(n_eligible)))
        .withColumn("lift", col("pair_cases") / col("expected_pair_cases"))
        .withColumn("_rk", row_number().over(Window.orderBy(col("pair_cases").desc(), col("drug_1"), col("drug_2"))))
    )
    return ranked.filter(col("_rk") <= limit).drop("_rk")


def combination_contingency(drugs: DataFrame, reactions: DataFrame, n_eligible: int, pairs: DataFrame, min_cases: int) -> DataFrame:
    keyed = pairs.select("drug_1", "drug_2", "pair_cases")
    left, right = drugs.withColumnRenamed("drug_key", "drug_1"), drugs.withColumnRenamed("drug_key", "drug_2")
    pair_case = (
        left.join(right, "caseid").filter(col("drug_1") < col("drug_2"))
        .join(broadcast(keyed), ["drug_1", "drug_2"])
    )
    a_counts = (
        pair_case.join(reactions, "caseid").groupBy("drug_1", "drug_2", "pt").agg(count(lit(1)).alias("a"))
        .filter(col("a") >= min_cases)
    )
    event_tot = reactions.groupBy("pt").agg(count(lit(1)).alias("n_event"))
    return (
        a_counts.join(keyed, ["drug_1", "drug_2"]).join(event_tot, "pt")
        .withColumn("drug_key", concat_ws(" + ", "drug_1", "drug_2"))
        .withColumn("n_drug", col("pair_cases"))
        .withColumn("b", col("n_drug") - col("a"))
        .withColumn("c", col("n_event") - col("a"))
        .withColumn("d", lit(n_eligible) - col("n_drug") - col("n_event") + col("a"))
        .withColumn("n_eligible_cases", lit(n_eligible))
    )


class InteractionAnalytics:

    def __init__(self, data_config=None, analysis_config=None, spark: Optional[SparkSession] = None):
        self.data_config = data_config or load_data_sources_config()
        self.analysis_config = analysis_config or load_analysis_config()
        self.gold_root = resolve_path(self.data_config.get("storage", {}).get("gold_dir", "data/gold"))
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = get_spark_session(app_name="PharmaSafe.Combinations")
        return self._spark

    def run(self, snapshot: str = "latest") -> Dict[str, Any]:
        base = self.gold_root / snapshot
        out = base / "combinations"
        cfg = self.analysis_config.get("combinations", {})
        roles = cfg.get("target_roles", ["PS", "SS"])
        min_pair, min_cases = cfg.get("min_pair_cases", 10), cfg.get("min_cases", 3)
        policy = next((n for n, p in screening_policies(self.analysis_config).items() if p.get("target_roles") == roles), None)
        params = screening_policies(self.analysis_config).get(policy or "primary", {})
        stats = self.analysis_config.get("statistics", {})

        case_drug, case_reaction = (self.spark.read.parquet(str(base / n)) for n in ("case_drug", "case_reaction"))
        drugs, reactions, n = eligible_population(case_drug, case_reaction, roles)
        pairs = pair_frequencies(drugs, n, min_pair, cfg.get("top_pairs_limit", 20000))
        pairs.write.mode("overwrite").parquet(str(out / "drug_combinations"))
        pairs = self.spark.read.parquet(str(out / "drug_combinations"))

        counts = combination_contingency(drugs, reactions, n, pairs, min_cases)
        signals = compute_safety_signals(
            counts, analysis_period=snapshot, role_policy="+".join(roles), min_cases=min_cases,
            min_ror_ci_lower=params.get("min_ror_ci_lower", 1.0), min_prr=params.get("min_prr", 2.0), min_chi2=params.get("min_chi2"),
            method_version=self.analysis_config.get("methodology", {}).get("version", "1.0.0"),
            z=stats.get("z_score", Z_95), zero_cell_correction=stats.get("zero_cell_correction", 0.5),
        ).join(counts.select("drug_key", col("pt").alias("reaction_term"), "drug_1", "drug_2").withColumnRenamed("drug_key", "drug_name"),
               ["drug_name", "reaction_term"])

        if policy:
            single = self.spark.read.parquet(str(base / f"safety_signals/role_policy={policy}")).select("drug_name", "reaction_term", "ROR")
            signals = (
                signals
                .join(single.select(col("drug_name").alias("drug_1"), "reaction_term", col("ROR").alias("ror_drug_1")), ["drug_1", "reaction_term"], "left")
                .join(single.select(col("drug_name").alias("drug_2"), "reaction_term", col("ROR").alias("ror_drug_2")), ["drug_2", "reaction_term"], "left")
                .withColumn("combo_exceeds_single_drug_ror",
                            when(col("ror_drug_1").isNotNull() & col("ror_drug_2").isNotNull(),
                                 col("ROR") > (when(col("ror_drug_1") > col("ror_drug_2"), col("ror_drug_1")).otherwise(col("ror_drug_2")))))
            )
        signals.write.mode("overwrite").parquet(str(out / "combination_signals"))
        sig = self.spark.read.parquet(str(out / "combination_signals"))
        result = {"snapshot": snapshot, "target_roles": roles, "eligible_cases_N": n, "pairs": pairs.count(),
                  "combination_signals": sig.count(), "potential_signals": sig.filter(col("screening_status") == SIGNAL_STATUS).count()}
        logger.info("Combination analytics: %s", result)
        return result
