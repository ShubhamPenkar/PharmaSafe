"""
PharmaSafe Gold Layer: Analytics-Ready Case Marts and Safety Signals

Builds, for one snapshot (the latest canonical view, or "as of" a source quarter):
  case_summary       one row per canonical case: demographics, report quarter, outcome flags
  case_drug          one row per (caseid, drug_key) with role flags and reported names
  case_reaction      one row per (caseid, pt)
  case_outcome       one row per (caseid, outcome code)
  drug_summary       drug-level case counts by role
  reaction_summary   reaction-level case counts
  quarterly_trends   case counts per report quarter (quarter of the canonical version)
  drug_event_counts  2x2 contingency counts per role policy
  safety_signals     ROR / PRR / CIs / screening per role policy
  gold_metrics.json  population sizes and run metadata

Every case-level table is deduplicated on its natural key before any join, so a case with several
drugs and reactions cannot be counted more than once per drug, reaction or drug-event pair.
"""

import sys
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array_contains, col, collect_set, explode, count, countDistinct, lit, max as spark_max, min as spark_min,
    size, slice as spark_slice, sort_array, when, sum as spark_sum,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_analysis_config, load_data_sources_config, resolve_path
from src.utils.spark_session import get_spark_session
from src.preprocessing.case_deduplicator import canonical_cases_asof
from src.signal_detection.signal_engine import (
    build_contingency_table, compute_safety_signals, eligible_population, screening_policies, Z_95,
)

logger = get_logger("PharmaSafe.Gold")

SERIOUS_OUTCOMES = ["DE", "LT", "HO", "DS", "CA", "RI", "OT"]
MAX_REPORTED_NAMES = 20
CASE_DESCRIPTORS = [
    "i_f_code", "rept_cod", "occp_cod", "init_fda_dt_date", "fda_dt_date", "event_dt_date",
    "age_years", "age_grp", "sex_norm", "wt_kg", "occr_country_norm", "reporter_country_norm",
]


class GoldPipeline:

    def __init__(
        self,
        data_config: Optional[Dict[str, Any]] = None,
        analysis_config: Optional[Dict[str, Any]] = None,
        spark: Optional[SparkSession] = None
    ):
        self.data_config = data_config or load_data_sources_config()
        self.analysis_config = analysis_config or load_analysis_config()
        storage = self.data_config.get("storage", {})
        self.silver_dir = resolve_path(storage.get("silver_dir", "data/silver"))
        self.gold_root = resolve_path(storage.get("gold_dir", "data/gold"))
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = get_spark_session(app_name="PharmaSafe.Gold")
        return self._spark

    def _silver(self, name: str) -> DataFrame:
        return self.spark.read.parquet(str(self.silver_dir / name))

    def _write(self, df: DataFrame, out_dir: Path, name: str) -> DataFrame:
        path = out_dir / name
        df.write.mode("overwrite").parquet(str(path))
        return self.spark.read.parquet(str(path))

    @staticmethod
    def snapshot_name(asof_quarter: Optional[str]) -> str:
        return f"asof_{asof_quarter.upper()}" if asof_quarter else "latest"

    def run(self, asof_quarter: Optional[str] = None) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        snapshot = self.snapshot_name(asof_quarter)
        out_dir = self.gold_root / snapshot
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True)
        logger.info("Building Gold snapshot '%s' from %s", snapshot, self.silver_dir)

        versions = canonical_cases_asof(self._silver("demo_versions"), asof_quarter)
        # Descriptive fields are optional (FDA has added/renamed DEMO columns over time); keys are not
        canonical = versions.select(
            "caseid", "primaryid", "caseversion_int", col("_source_quarter").alias("report_quarter"),
            *[col(c) if c in versions.columns else lit(None).cast("string").alias(c) for c in CASE_DESCRIPTORS],
        )
        canonical_keys = canonical.select("primaryid")

        # Child rows of the canonical version only (inner join on primaryid)
        drug = self._silver("drug").join(canonical_keys, on="primaryid", how="inner")
        reac = self._silver("reac").join(canonical_keys, on="primaryid", how="inner")
        outc = self._silver("outc").join(canonical_keys, on="primaryid", how="inner")

        case_drug = self._write(
            # One row per (case, active ingredient); a combination product contributes to each of its ingredients
            drug.filter(col("drug_ingredients").isNotNull())
            .withColumn("drug_key", explode("drug_ingredients"))
            .groupBy("caseid", "drug_key")
            .agg(
                sort_array(collect_set("role_cod_norm")).alias("roles"),
                spark_slice(sort_array(collect_set("drugname_norm")), 1, MAX_REPORTED_NAMES).alias("reported_names"),
                count(lit(1)).alias("drug_records"),
                spark_max(when(col("drug_key_source") == "prod_ai", True).otherwise(False)).alias("from_active_ingredient"),
            )
            .withColumn("is_ps", array_contains("roles", "PS"))
            .withColumn("is_ss", array_contains("roles", "SS"))
            .withColumn("is_c", array_contains("roles", "C"))
            .withColumn("is_i", array_contains("roles", "I")),
            out_dir, "case_drug",
        )
        case_reaction = self._write(
            reac.filter(col("pt_norm").isNotNull()).select("caseid", col("pt_norm").alias("pt")).distinct(),
            out_dir, "case_reaction",
        )
        case_outcome = self._write(
            outc.filter(col("outc_cod_norm").isNotNull()).select("caseid", col("outc_cod_norm").alias("outc_cod")).distinct(),
            out_dir, "case_outcome",
        )

        outcome_flags = case_outcome.groupBy("caseid").agg(
            sort_array(collect_set("outc_cod")).alias("outcomes"),
            spark_max(when(col("outc_cod") == "DE", True).otherwise(False)).alias("is_death"),
        )
        drug_counts = case_drug.groupBy("caseid").agg(count(lit(1)).alias("n_drugs"), spark_sum(col("is_ps").cast("int")).alias("n_ps_drugs"))
        reac_counts = case_reaction.groupBy("caseid").agg(count(lit(1)).alias("n_reactions"))
        case_summary = self._write(
            canonical
            .join(outcome_flags, on="caseid", how="left")
            .join(drug_counts, on="caseid", how="left")
            .join(reac_counts, on="caseid", how="left")
            .fillna({"is_death": False, "n_drugs": 0, "n_ps_drugs": 0, "n_reactions": 0})
            .withColumn("is_serious", col("outcomes").isNotNull() & (size("outcomes") > 0)),
            out_dir, "case_summary",
        )

        self._write(
            case_drug.groupBy("drug_key").agg(
                count(lit(1)).alias("cases_any_role"),
                spark_sum(col("is_ps").cast("int")).alias("cases_ps"),
                spark_sum(col("is_ss").cast("int")).alias("cases_ss"),
                spark_sum(col("is_c").cast("int")).alias("cases_c"),
                spark_sum(col("is_i").cast("int")).alias("cases_i"),
                spark_sum("drug_records").alias("drug_records"),
            ),
            out_dir, "drug_summary",
        )
        self._write(case_reaction.groupBy("pt").agg(count(lit(1)).alias("cases")), out_dir, "reaction_summary")
        self._write(
            case_drug.select("drug_key", explode("reported_names").alias("reported_name"), "caseid")
            .groupBy("drug_key", "reported_name").agg(count(lit(1)).alias("cases")),
            out_dir, "drug_name_index",
        )
        self._write(
            case_summary.groupBy("report_quarter").agg(
                count(lit(1)).alias("cases"),
                spark_sum(col("is_serious").cast("int")).alias("serious_cases"),
                spark_sum(col("is_death").cast("int")).alias("death_cases"),
            ),
            out_dir, "quarterly_trends",
        )

        self._write(case_outcome.groupBy("outc_cod").agg(count(lit(1)).alias("cases")), out_dir, "outcome_summary")
        self._build_drug_profiles(case_drug, case_summary, case_outcome, out_dir)

        signal_metrics = self._run_signals(case_drug, case_reaction, out_dir, snapshot)

        metrics = {
            "snapshot": snapshot,
            "asof_quarter": asof_quarter,
            "built_timestamp": started.isoformat(),
            "canonical_cases": case_summary.count(),
            "case_drug_rows": case_drug.count(),
            "distinct_drugs": case_drug.select("drug_key").distinct().count(),
            "case_reaction_rows": case_reaction.count(),
            "distinct_reactions": case_reaction.select("pt").distinct().count(),
            "report_quarters": sorted(r[0] for r in case_summary.select("report_quarter").distinct().collect()),
            "signals": signal_metrics,
        }
        metrics["duration_seconds"] = round((datetime.now(timezone.utc) - started).total_seconds(), 2)
        with open(out_dir / "gold_metrics.json", "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, default=str)
        logger.info("Gold snapshot '%s' complete in %.1fs", snapshot, metrics["duration_seconds"])
        return metrics

    def _build_drug_profiles(self, case_drug: DataFrame, case_summary: DataFrame, case_outcome: DataFrame, out_dir: Path) -> None:
        """Per-drug outcome, demographic and quarterly profiles for each role policy (cases counted once)."""
        demo = case_summary.select(
            "caseid", "report_quarter", "sex_norm", "age_years",
            when(col("age_years").isNull(), "Unknown").when(col("age_years") < 18, "0-17")
            .when(col("age_years") < 45, "18-44").when(col("age_years") < 65, "45-64")
            .when(col("age_years") < 75, "65-74").otherwise("75+").alias("age_band"),
        )
        outcomes, sexes, ages, quarters = [], [], [], []
        for policy, params in screening_policies(self.analysis_config).items():
            cond = None
            for r in params.get("target_roles", ["PS"]):
                c = col(f"is_{r.lower()}")
                cond = c if cond is None else (cond | c)
            drugs = case_drug.filter(cond).select("caseid", "drug_key").distinct().join(demo, "caseid")
            tag = lit(policy).alias("role_policy")
            outcomes.append(drugs.join(case_outcome, "caseid").groupBy("drug_key", "outc_cod").agg(count(lit(1)).alias("cases")).select("drug_key", tag, "outc_cod", "cases"))
            sexes.append(drugs.groupBy("drug_key", "sex_norm").agg(count(lit(1)).alias("cases")).select("drug_key", tag, "sex_norm", "cases"))
            ages.append(drugs.groupBy("drug_key", "age_band").agg(count(lit(1)).alias("cases")).select("drug_key", tag, "age_band", "cases"))
            quarters.append(drugs.groupBy("drug_key", "report_quarter").agg(count(lit(1)).alias("cases")).select("drug_key", tag, "report_quarter", "cases"))
        for name, frames in (("drug_outcomes", outcomes), ("drug_sex", sexes), ("drug_age", ages), ("drug_quarterly", quarters)):
            union = frames[0]
            for f in frames[1:]:
                union = union.unionByName(f)
            self._write(union, out_dir, name)

    def _run_signals(self, case_drug: DataFrame, case_reaction: DataFrame, out_dir: Path, snapshot: str) -> Dict[str, Any]:
        stats = self.analysis_config.get("statistics", {})
        method_version = self.analysis_config.get("methodology", {}).get("version", "1.0.0")
        results: Dict[str, Any] = {}

        for policy, params in screening_policies(self.analysis_config).items():
            roles = params.get("target_roles", ["PS"])
            drugs, reactions, n = eligible_population(case_drug, case_reaction, roles)
            # Pairs below the floor can never pass screening; the floor still allows re-screening at higher thresholds
            floor = min(self.analysis_config.get("output", {}).get("min_stored_cases", 1), params.get("min_cases", 3))
            counts = self._write(
                build_contingency_table(drugs, reactions, n, min_observed_cases=floor),
                out_dir, f"drug_event_counts/role_policy={policy}",
            )
            signals = self._write(
                compute_safety_signals(
                    counts,
                    analysis_period=snapshot,
                    role_policy=policy,
                    min_cases=params.get("min_cases", 3),
                    min_ror_ci_lower=params.get("min_ror_ci_lower", 1.0),
                    min_prr=params.get("min_prr", 2.0),
                    min_chi2=params.get("min_chi2"),
                    method_version=method_version,
                    z=stats.get("z_score", Z_95),
                    zero_cell_correction=stats.get("zero_cell_correction", 0.5),
                ).drop("role_policy"),
                out_dir, f"safety_signals/role_policy={policy}",
            )
            row = signals.agg(
                count(lit(1)).alias("pairs"),
                spark_sum((col("screening_status") == "POTENTIAL_SIGNAL").cast("int")).alias("potential_signals"),
                countDistinct("drug_name").alias("drugs"),
                countDistinct("reaction_term").alias("reactions"),
            ).collect()[0].asDict()
            results[policy] = {"target_roles": roles, "eligible_cases_N": n, **row,
                               "criteria": {k: v for k, v in params.items() if k != "target_roles"}}
            logger.info("Signals [%s] roles=%s N=%d: %s", policy, roles, n, row)
        return results
