"""
PharmaSafe Pharmacovigilance Signal Engine
Computes 2x2 contingency tables and disproportionality metrics (ROR, PRR)
across all drug-adverse event pairs in Apache Spark.
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, udf
from pyspark.sql.types import (
    StructType, StructField, DoubleType, StringType, BooleanType, ArrayType
)
from src.signal_detection.metrics import (
    calculate_ror,
    calculate_prr,
    evaluate_screening_status
)
from src.utils.logger import get_logger

logger = get_logger("PharmaSafe.SignalEngine")


def build_contingency_matrix(
    canonical_demo_df: DataFrame,
    unique_case_drugs_df: DataFrame,
    unique_case_reac_df: DataFrame,
    min_observed_cases: int = 3
) -> tuple[DataFrame, int]:
    """
    Constructs the 2x2 contingency counts for all eligible drug-event pairs:
      a = Cases with Drug and Event
      b = Cases with Drug without Event  (n_drug - a)
      c = Cases without Drug with Event  (n_event - a)
      d = Cases without Drug without Event (N - a - b - c)
    """
    total_eligible_cases = canonical_demo_df.select("caseid").distinct().count()
    logger.info("Total eligible canonical cases in population (N): %d", total_eligible_cases)

    # Total cases per drug: n_drug = a + b
    drug_totals = (
        unique_case_drugs_df
        .groupBy("clean_drug_name")
        .agg(count("caseid").alias("n_drug"))
    )

    # Total cases per reaction: n_event = a + c
    event_totals = (
        unique_case_reac_df
        .groupBy("clean_pt")
        .agg(count("caseid").alias("n_event"))
    )

    # Co-reporting count: a
    drug_event_pairs = (
        unique_case_drugs_df
        .join(unique_case_reac_df, on="caseid", how="inner")
        .groupBy("clean_drug_name", "clean_pt")
        .agg(count("caseid").alias("a"))
        .filter(col("a") >= min_observed_cases)
    )

    # Join totals to compute b, c, d
    contingency_df = (
        drug_event_pairs
        .join(drug_totals, on="clean_drug_name", how="inner")
        .join(event_totals, on="clean_pt", how="inner")
        .withColumn("b", col("n_drug") - col("a"))
        .withColumn("c", col("n_event") - col("a"))
        .withColumn("d", total_eligible_cases - col("n_drug") - col("n_event") + col("a"))
        .withColumn("total_cases_N", col("a") + col("b") + col("c") + col("d"))
    )

    return contingency_df, total_eligible_cases


def compute_safety_signals(
    contingency_df: DataFrame,
    analysis_period: str = "ALL",
    min_cases: int = 3,
    min_ror_ci_lower: float = 1.0,
    min_prr: float = 2.0,
    method_version: str = "1.0.0"
) -> DataFrame:
    """
    Applies disproportionality statistics (ROR, PRR, 95% CIs) and screening gates.
    Returns the analytical gold safety signals DataFrame.
    """
    # Define Spark UDF for batch statistical computation
    metric_schema = StructType([
        StructField("ror", DoubleType(), False),
        StructField("ror_ci_lower", DoubleType(), False),
        StructField("ror_ci_upper", DoubleType(), False),
        StructField("prr", DoubleType(), False),
        StructField("prr_ci_lower", DoubleType(), False),
        StructField("prr_ci_upper", DoubleType(), False),
        StructField("chi2_yates", DoubleType(), False),
        StructField("zero_corrected", BooleanType(), False),
        StructField("screening_status", StringType(), False),
        StructField("screening_reasons", ArrayType(StringType()), False),
    ])

    def _calculate_all_metrics(a, b, c, d):
        r_ror = calculate_ror(a, b, c, d)
        r_prr = calculate_prr(a, b, c, d)
        status, reasons = evaluate_screening_status(
            observed_cases=int(a),
            ror_ci_lower=r_ror["ror_ci_lower"],
            prr=r_prr["prr"],
            min_cases=min_cases,
            min_ror_ci_lower=min_ror_ci_lower,
            min_prr=min_prr
        )
        return (
            float(r_ror["ror"]),
            float(r_ror["ror_ci_lower"]),
            float(r_ror["ror_ci_upper"]),
            float(r_prr["prr"]),
            float(r_prr["prr_ci_lower"]),
            float(r_prr["prr_ci_upper"]),
            float(r_prr["chi2_yates"]),
            bool(r_ror["zero_corrected"]),
            str(status),
            reasons
        )

    calc_udf = udf(_calculate_all_metrics, metric_schema)

    signals_df = (
        contingency_df
        .withColumn("metrics", calc_udf(col("a"), col("b"), col("c"), col("d")))
        .select(
            col("clean_drug_name").alias("drug_name"),
            col("clean_pt").alias("reaction_term"),
            col("a").alias("observed_case_count"),
            col("b").alias("cell_b"),
            col("c").alias("cell_c"),
            col("d").alias("cell_d"),
            col("total_cases_N").alias("eligible_case_count"),
            col("metrics.ror").alias("ROR"),
            col("metrics.ror_ci_lower").alias("ROR_CI_lower"),
            col("metrics.ror_ci_upper").alias("ROR_CI_upper"),
            col("metrics.prr").alias("PRR"),
            col("metrics.prr_ci_lower").alias("PRR_CI_lower"),
            col("metrics.prr_ci_upper").alias("PRR_CI_upper"),
            col("metrics.chi2_yates").alias("chi2_yates"),
            col("metrics.zero_corrected").alias("zero_corrected"),
            col("metrics.screening_status").alias("screening_status"),
            col("metrics.screening_reasons").alias("screening_reasons")
        )
    )

    return signals_df
