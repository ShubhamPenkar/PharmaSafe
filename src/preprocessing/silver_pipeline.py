"""
PharmaSafe Silver Layer Preprocessing Pipeline (PySpark)
Cleans, standardizes, deduplicates and relationally validates all seven FAERS tables.

Outputs under data/silver/:
  demo_versions/   every retained DEMO report version with its validity interval (time-aware view)
  demo/            the currently canonical version of every non-deleted case (latest view)
  drug/ reac/ outc/ indi/ ther/ rpsr/
                   child rows linked to a retained DEMO version (all versions, so any as-of
                   snapshot can be rebuilt by joining to canonical_cases_asof(...))
  deleted_cases/   caseids FDA listed as deleted, with the first quarter they were listed
  quarantine/<table>/   excluded rows with a quarantine_reason
  silver_metrics.json   raw vs retained counts, duplicates, orphans, missing-value profiles

Child-row exclusion reasons distinguish genuinely orphaned records from version bookkeeping:
  ORPHAN_NO_DEMO_RECORD        primaryid never appears in DEMO
  PARENT_DEMO_QUARANTINED      parent DEMO row exists but was itself quarantined (e.g. invalid id)
  PARENT_FROM_OTHER_QUARTER    the primaryid's retained DEMO row came from a different quarter file
  EXACT_DUPLICATE              identical child row repeated within the same report
Superseded case versions are NOT orphans: their child rows stay in Silver and drop out in Gold.
"""

import sys
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, count, lit, sum as spark_sum, when

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_data_sources_config, resolve_path
from src.utils.spark_session import get_spark_session
from src.preprocessing.normalization import (
    AGE_UNIT_TO_YEARS, WEIGHT_UNIT_TO_KG, VALID_ROLE_CODES, VALID_OUTCOME_CODES, VALID_SEX_CODES,
    blank_to_null, normalize_term, is_numeric_id, try_int, try_double, with_parsed_dates,
    unit_conversion, coded_value,
)
from src.preprocessing.drug_normalization import ingredient_array
from src.preprocessing.case_deduplicator import (
    resolve_report_duplicates, build_case_version_history, canonical_cases_asof, version_history_metrics,
)

logger = get_logger("PharmaSafe.Silver")

CHILD_TABLES = ["DRUG", "REAC", "OUTC", "INDI", "THER", "RPSR"]
METADATA_PREFIX = "_"
DEMO_DATE_COLUMNS = ["event_dt", "mfr_dt", "init_fda_dt", "fda_dt", "rept_dt"]
THER_DATE_COLUMNS = ["start_dt", "end_dt"]


def source_columns(df: DataFrame) -> List[str]:
    """Columns that came from the FDA file (excludes pipeline metadata such as _source_quarter)."""
    return [c for c in df.columns if not c.startswith(METADATA_PREFIX)]


def null_profile(df: DataFrame, columns: List[str]) -> Dict[str, float]:
    """Percentage of NULL values per column, computed in one aggregation."""
    if not columns:
        return {}
    row = df.agg(
        count(lit(1)).alias("__n"),
        *[spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns]
    ).collect()[0].asDict()
    n = row.pop("__n")
    return {c: round(100.0 * (v or 0) / n, 2) if n else 0.0 for c, v in row.items()}


def standardize_demo(demo: DataFrame) -> DataFrame:
    """Typed and normalized DEMO fields alongside the reported values."""
    df = with_parsed_dates(demo, DEMO_DATE_COLUMNS)
    return (
        df
        .withColumn("caseversion_int", try_int("caseversion"))
        .withColumn("age_num", try_double("age"))
        .withColumn("age_years", unit_conversion("age", "age_cod", AGE_UNIT_TO_YEARS))
        .withColumn("wt_kg", unit_conversion("wt", "wt_cod", WEIGHT_UNIT_TO_KG))
        .withColumn("sex_norm", coded_value("sex", VALID_SEX_CODES))
        .withColumn("occr_country_norm", normalize_term("occr_country"))
        .withColumn("reporter_country_norm", normalize_term("reporter_country"))
    )


def standardize_child(table: str, df: DataFrame) -> DataFrame:
    """Table-specific normalizations; reported values are kept unchanged."""
    if table == "DRUG":
        return (
            df
            .withColumn("drug_seq_int", try_int("drug_seq"))
            .withColumn("role_cod_norm", coded_value("role_cod", VALID_ROLE_CODES))
            .withColumn("drugname_norm", normalize_term("drugname"))
            .withColumn("prod_ai_norm", normalize_term("prod_ai"))
            # FDA-mapped active ingredient (prod_ai) is the source of the analysis key; fall back to the reported name
            .withColumn(
                "drug_key_source",
                when(col("prod_ai_norm").isNotNull(), lit("prod_ai"))
                .when(col("drugname_norm").isNotNull(), lit("drugname"))
            )
            # Ingredient-level keys: salts stripped, combination products split (see drug_normalization.py)
            .withColumn("drug_ingredients", ingredient_array("prod_ai_norm", "drugname_norm"))
            .withColumn("drug_key", concat_ws("\\", col("drug_ingredients")))
        )
    if table == "REAC":
        return df.withColumn("pt_norm", normalize_term("pt"))
    if table == "OUTC":
        return df.withColumn("outc_cod_norm", coded_value("outc_cod", VALID_OUTCOME_CODES))
    if table == "INDI":
        return df.withColumn("indi_drug_seq_int", try_int("indi_drug_seq")).withColumn("indi_pt_norm", normalize_term("indi_pt"))
    if table == "THER":
        return (
            with_parsed_dates(df, THER_DATE_COLUMNS)
            .withColumn("dsg_drug_seq_int", try_int("dsg_drug_seq"))
            .withColumn("dur_num", try_double("dur"))
        )
    if table == "RPSR":
        return df.withColumn("rpsr_cod_norm", normalize_term("rpsr_cod"))
    return df


class SilverPipeline:
    """Executes Silver standardization, deduplication, version history and relational validation."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, spark: Optional[SparkSession] = None):
        self.config = config or load_data_sources_config()
        storage = self.config.get("storage", {})
        self.bronze_dir = resolve_path(storage.get("bronze_dir", "data/bronze"))
        self.silver_dir = resolve_path(storage.get("silver_dir", "data/silver"))
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = get_spark_session(app_name="PharmaSafe.Silver")
        return self._spark

    def available_quarters(self) -> List[str]:
        """Quarters with a completed Bronze ingestion (summary written last)."""
        return sorted(p.parent.name for p in self.bronze_dir.glob("*/ingestion_summary.json"))

    def load_bronze_table(self, table_name: str, quarters: List[str]) -> Optional[DataFrame]:
        """Unions one Bronze table across quarters; columns added or dropped by FDA are NULL-filled."""
        frames = []
        for q in quarters:
            path = self.bronze_dir / q / table_name.lower()
            if path.exists():
                frames.append(self.spark.read.parquet(str(path)))
            else:
                logger.warning("Bronze table %s missing for %s", table_name, q)
        if not frames:
            return None
        combined = frames[0]
        for f in frames[1:]:
            combined = combined.unionByName(f, allowMissingColumns=True)
        return combined

    def _write(self, df: DataFrame, name: str) -> int:
        out = self.silver_dir / name
        df.write.mode("overwrite").parquet(str(out))
        return self.spark.read.parquet(str(out)).count()

    def _process_demo(self, quarters: List[str]) -> Tuple[DataFrame, Dict[str, Any]]:
        raw = self.load_bronze_table("DEMO", quarters)
        if raw is None:
            raise FileNotFoundError(f"No Bronze DEMO data found under {self.bronze_dir} for {quarters}")
        src_cols = source_columns(raw)
        raw = blank_to_null(raw, src_cols)
        raw_count = raw.count()

        valid_ids = is_numeric_id("primaryid") & is_numeric_id("caseid")
        invalid = raw.filter(~valid_ids).withColumn("quarantine_reason", lit("INVALID_IDENTIFIER"))
        deduped, duplicates = resolve_report_duplicates(raw.filter(valid_ids), src_cols)

        deleted_raw = self.load_bronze_table("DELETED", quarters)
        deleted_cases = None
        if deleted_raw is not None:
            deleted_cases = deleted_raw.select("caseid", "_source_quarter").dropDuplicates()
            self._write(deleted_cases, "deleted_cases")
            deleted_cases = self.spark.read.parquet(str(self.silver_dir / "deleted_cases"))

        history = build_case_version_history(standardize_demo(deduped), deleted_cases)
        versions_written = self._write(history, "demo_versions")
        history = self.spark.read.parquet(str(self.silver_dir / "demo_versions"))
        canonical_written = self._write(canonical_cases_asof(history), "demo")

        excluded = invalid.unionByName(duplicates.drop("_content_hash"), allowMissingColumns=True)
        excluded_written = self._write(excluded, "quarantine/demo")
        reasons = {r["quarantine_reason"]: r["count"] for r in
                   self.spark.read.parquet(str(self.silver_dir / "quarantine/demo")).groupBy("quarantine_reason").count().collect()}

        metrics = {
            "raw_rows": raw_count,
            "retained_report_versions": versions_written,
            "excluded_rows": excluded_written,
            "excluded_by_reason": reasons,
            "canonical_cases_latest": canonical_written,
            **version_history_metrics(history),
            "missing_pct": null_profile(history, src_cols),
        }
        if raw_count != versions_written + excluded_written:
            raise ValueError(f"DEMO accounting mismatch: raw {raw_count} != retained {versions_written} + excluded {excluded_written}")
        return history, metrics

    def _process_child(self, table: str, quarters: List[str], history: DataFrame, raw_demo_ids: DataFrame) -> Dict[str, Any]:
        raw = self.load_bronze_table(table, quarters)
        if raw is None:
            return {"status": "MISSING_IN_BRONZE"}
        src_cols = source_columns(raw)
        raw = blank_to_null(raw, src_cols)
        raw_count = raw.count()

        # Exact duplicates within the same report and quarter file
        deduped = raw.dropDuplicates(src_cols + ["_source_quarter"])
        dup_count = raw_count - deduped.count()

        parents = history.select(
            col("primaryid"), col("_source_quarter"), col("caseid").alias("_demo_caseid"), lit(True).alias("_linked")
        )
        joined = deduped.join(parents, on=["primaryid", "_source_quarter"], how="left")
        linked = joined.filter(col("_linked").isNotNull())

        unlinked = (
            joined.filter(col("_linked").isNull())
            .join(raw_demo_ids, on="primaryid", how="left")
            .withColumn(
                "quarantine_reason",
                when(col("_demo_status") == "retained", lit("PARENT_FROM_OTHER_QUARTER"))
                .when(col("_demo_status") == "quarantined", lit("PARENT_DEMO_QUARANTINED"))
                .otherwise(lit("ORPHAN_NO_DEMO_RECORD"))
            )
            .drop("_linked", "_demo_caseid", "_demo_status")
        )

        # caseid on child rows is informational; the parent DEMO caseid is authoritative
        caseid_mismatch = linked.filter(col("caseid").isNotNull() & (col("caseid") != col("_demo_caseid"))).count() if "caseid" in linked.columns else 0
        silver = standardize_child(
            table,
            linked.withColumnRenamed("caseid", "caseid_reported").withColumnRenamed("_demo_caseid", "caseid").drop("_linked")
        )
        retained = self._write(silver, table.lower())
        quarantined = self._write(unlinked, f"quarantine/{table.lower()}")
        reasons = {r["quarantine_reason"]: r["count"] for r in
                   self.spark.read.parquet(str(self.silver_dir / f"quarantine/{table.lower()}")).groupBy("quarantine_reason").count().collect()}
        if dup_count:
            reasons["EXACT_DUPLICATE"] = dup_count

        if raw_count != retained + quarantined + dup_count:
            raise ValueError(f"{table} accounting mismatch: raw {raw_count} != {retained} + {quarantined} + {dup_count}")

        return {
            "raw_rows": raw_count,
            "retained_rows": retained,
            "excluded_rows": quarantined + dup_count,
            "excluded_by_reason": reasons,
            "retention_pct": round(100.0 * retained / raw_count, 2) if raw_count else 0.0,
            "caseid_mismatch_rows": caseid_mismatch,
            "missing_pct": null_profile(self.spark.read.parquet(str(self.silver_dir / table.lower())), src_cols),
        }

    def run(self, quarters: Optional[List[str]] = None) -> Dict[str, Any]:
        """Builds the complete Silver layer from the given (or all available) Bronze quarters."""
        quarters = sorted(q.upper() for q in (quarters or self.available_quarters()))
        if not quarters:
            raise FileNotFoundError(f"No completed Bronze quarters under {self.bronze_dir}")
        started = datetime.now(timezone.utc)
        logger.info("Building Silver layer from %d quarter(s): %s", len(quarters), quarters)

        if self.silver_dir.exists():
            shutil.rmtree(self.silver_dir)
        self.silver_dir.mkdir(parents=True)

        history, demo_metrics = self._process_demo(quarters)
        logger.info("DEMO: %d versions retained, %d canonical cases", demo_metrics["retained_report_versions"], demo_metrics["canonical_cases_latest"])

        demo_quarantine = self.spark.read.parquet(str(self.silver_dir / "quarantine/demo"))
        raw_demo_ids = (
            history.select("primaryid", lit("retained").alias("_demo_status"))
            .unionByName(demo_quarantine.select("primaryid", lit("quarantined").alias("_demo_status")))
            .dropDuplicates(["primaryid"])
        )

        tables = {"DEMO": demo_metrics}
        for t in CHILD_TABLES:
            tables[t] = self._process_child(t, quarters, history, raw_demo_ids)
            logger.info("%s: %s", t, {k: v for k, v in tables[t].items() if k != "missing_pct"})

        metrics = {
            "quarters": quarters,
            "processed_timestamp": started.isoformat(),
            "duration_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
            "deduplication_policy": "caseversion DESC, fda_dt DESC, primaryid DESC (NULLs last)",
            "tables": tables,
        }
        with open(self.silver_dir / "silver_metrics.json", "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, default=str)
        logger.info("Silver layer complete in %.1fs", metrics["duration_seconds"])
        return metrics
