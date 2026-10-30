"""
PharmaSafe Bronze Layer Ingestion Pipeline
Ingests verified FAERS quarterly ASCII archives into the Bronze Medallion layer.

- Verifies the archive SHA-256 against the provenance manifest (when an expected checksum is given)
  and the ZIP CRCs before anything is parsed.
- Streams each table member out of the archive while independently counting its source lines,
  so the Spark output can be reconciled against a count that does not depend on Spark.
- Parses dollar-delimited records with PySpark DataFrames and writes Parquet natively from the
  executors (no driver-side collection). On Windows without HADOOP_HOME, falls back to a
  partition-streaming PyArrow writer, because Spark's Hadoop file committer needs winutils.exe there.
- Preserves every source column as a raw string; typing and normalization happen in Silver.
- Quarantines every malformed row (column-count mismatch) to Parquet with an auditable reason.
- Ingests the quarterly deleted-case list published inside each archive.
- Guards against synthetic fixtures entering the official data stream and is idempotent.
"""

import os
import sys
import json
import shutil
import hashlib
import zipfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, split, size, lit, trim

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_data_sources_config, get_project_root, resolve_path
from src.utils.spark_session import get_spark_session
from src.ingestion.download_manager import DownloadManager

logger = get_logger("PharmaSafe.Bronze")

CORE_FAERS_TABLES = ["DEMO", "DRUG", "REAC", "OUTC", "INDI", "THER", "RPSR"]
DELETED_TABLE = "DELETED"
QUARANTINE_SAMPLE_SIZE = 100


def count_source_records(path: Path) -> Tuple[int, str]:
    """
    Independently counts data records in a FAERS ASCII file without Spark.
    Returns (non-blank lines excluding the header, header line).
    Line boundaries are '\\n'; a stray '\\r' inside a field is therefore NOT a boundary here,
    which lets reconciliation expose any disagreement with Spark's line reader.
    """
    data_lines = 0
    header = None
    with open(path, "rb") as f:
        for raw in f:
            line = raw.rstrip(b"\r\n")
            if header is None:
                header = line.decode("utf-8", errors="replace")
                continue
            if line.strip():
                data_lines += 1
    return data_lines, header or ""


class BronzePipeline:
    """
    Ingests official FAERS ASCII archives into the Bronze layer with validation and quarantine gates.
    Output layout: data/bronze/<QUARTER>/<table>/part-*.parquet
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        spark: Optional[SparkSession] = None,
        writer: Optional[str] = None
    ):
        self.config = config or load_data_sources_config()
        self.root = get_project_root()
        storage = self.config.get("storage", {})

        self.raw_dir = resolve_path(storage.get("raw_dir", "data/raw"))
        self.bronze_dir = resolve_path(storage.get("bronze_dir", "data/bronze"))
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

        self.tables_config = self.config.get("tables", {})
        self.delimiter = self.config.get("faers_source", {}).get("delimiter", "$")
        self.writer = self._select_writer(writer or self.config.get("bronze", {}).get("writer", "auto"))
        self._spark = spark

    @staticmethod
    def _select_writer(requested: str) -> str:
        """Chooses the Parquet writer: native Spark unless running on Windows without Hadoop binaries."""
        if requested in ("spark", "arrow"):
            return requested
        if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
            return "arrow"
        return "spark"

    @property
    def spark(self) -> SparkSession:
        """Lazily initializes or retrieves the active SparkSession."""
        if self._spark is None:
            self._spark = get_spark_session(app_name="PharmaSafe.Bronze")
        return self._spark

    @staticmethod
    def _find_table_file_in_archive(zf: zipfile.ZipFile, table_name: str) -> Optional[str]:
        """Finds the member path for a given table inside the zip archive (case-insensitive)."""
        pattern = table_name.upper()
        for name in zf.namelist():
            base = Path(name).name.upper()
            if base.startswith(pattern) and base.endswith((".TXT", ".DAT")):
                return name
        return None

    @staticmethod
    def _find_readme_in_archive(zf: zipfile.ZipFile) -> Optional[str]:
        """Finds the README or documentation member in the archive."""
        for name in zf.namelist():
            base = Path(name).name.lower()
            if "readme" in base or "read_me" in base:
                return name
        return None

    @staticmethod
    def _compute_sha256(path: Path) -> str:
        """Calculates SHA-256 checksum using streaming blocks."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _extract_member(zf: zipfile.ZipFile, member: str, dest: Path) -> None:
        """Streams one archive member to a fixed destination path (never trusts member paths)."""
        with zf.open(member) as src, open(dest, "wb") as out:
            shutil.copyfileobj(src, out, length=1 << 20)

    def _write_parquet(self, df: DataFrame, out_dir: Path) -> None:
        """Writes a DataFrame as a Parquet dataset directory using the selected writer."""
        if out_dir.exists():
            shutil.rmtree(out_dir)

        if self.writer == "spark":
            df.write.mode("overwrite").option("compression", "snappy").parquet(str(out_dir))
            return

        # Arrow fallback: stream partitions through the driver one batch at a time (constant memory)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_cols = list(df.columns)
        pa_schema = pa.schema([
            pa.field(c, pa.bool_()) if c == "_is_synthetic" else pa.field(c, pa.string())
            for c in out_cols
        ])

        def make_arrow_batch(iterator):
            import pandas as pd
            rows = list(iterator)
            if rows:
                pdf = pd.DataFrame(rows, columns=out_cols)
                return [pa.RecordBatch.from_pandas(pdf, schema=pa_schema, preserve_index=False)]
            return []

        writer = pq.ParquetWriter(str(out_dir / "part-00000.parquet"), pa_schema, compression="snappy")
        try:
            for batch in df.rdd.mapPartitions(make_arrow_batch).toLocalIterator():
                writer.write_batch(batch)
        finally:
            writer.close()

    def _ingest_deleted_cases(
        self,
        source_file: Path,
        member_name: str,
        quarter: str,
        q_bronze_dir: Path,
        ingestion_ts: str,
        is_synthetic: bool
    ) -> Dict[str, Any]:
        """
        Ingests the quarterly list of deleted case IDs. The file is a bare list of caseids
        (one per line, no header in recent releases); a header line is detected and skipped.
        """
        lines = self.spark.read.text(str(source_file))
        ids = lines.select(trim(col("value")).alias("caseid")).filter(col("caseid") != "")
        header_like = ids.filter(~col("caseid").rlike(r"^\d+$"))
        non_numeric = header_like.count()
        if non_numeric:
            logger.info("Deleted-case file %s: skipping %d non-numeric line(s) (header)", member_name, non_numeric)
        df = (
            ids.filter(col("caseid").rlike(r"^\d+$"))
            .withColumn("_source_quarter", lit(quarter))
            .withColumn("_source_file", lit(member_name))
            .withColumn("_ingestion_timestamp", lit(ingestion_ts))
            .withColumn("_is_synthetic", lit(is_synthetic))
        )
        out_dir = q_bronze_dir / DELETED_TABLE.lower()
        self._write_parquet(df, out_dir)
        written = self.spark.read.parquet(str(out_dir)).count()
        with open(source_file, "rb") as f:   # the deleted list has no guaranteed header: count every non-blank line
            source_lines = sum(1 for raw in f if raw.strip())
        return {
            "status": "INGESTED",
            "member_file": member_name,
            "source_records": source_lines,
            "rows_ingested": written,
            "non_numeric_lines_skipped": non_numeric,
            "rows_quarantined": 0,
            "reconciled": written + non_numeric == source_lines,
            "parquet_path": str(out_dir),
        }

    def ingest_archive(
        self,
        archive_path: Path,
        quarter_id: str,
        force: bool = False,
        allow_synthetic: Optional[bool] = None,
        expected_sha256: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Parses and ingests all FAERS tables from a verified zip archive into the Bronze layer.
        Rejects synthetic fixtures unless allow_synthetic=True is explicitly passed,
        or unless running in an isolated test environment (outside data/raw).
        """
        if not archive_path.exists():
            raise FileNotFoundError(f"Archive not found: {archive_path}")

        # 1. Scan for synthetic markers
        markers = DownloadManager.scan_for_synthetic_markers(archive_path)
        is_synthetic = len(markers) > 0

        if allow_synthetic is None:
            # Production guard: if the archive is inside project data/raw, never allow synthetic fixtures
            prod_raw = (self.root / "data/raw").resolve()
            allow_synthetic = not archive_path.resolve().is_relative_to(prod_raw)

        if is_synthetic and not allow_synthetic:
            raise ValueError(
                f"Ingestion rejected for {archive_path.name}: Archive contains synthetic fixture markers {markers}. "
                f"Synthetic fixtures cannot be ingested into the official Bronze layer."
            )

        quarter_upper = quarter_id.upper()
        q_bronze_dir = self.bronze_dir / quarter_upper
        summary_path = q_bronze_dir / "ingestion_summary.json"

        # Idempotency: the summary is written last, so its presence means a complete prior run
        if not force and summary_path.exists():
            logger.info("Quarter %s already ingested in Bronze at %s. Loading existing summary.", quarter_upper, q_bronze_dir)
            with open(summary_path, "r", encoding="utf-8") as f:
                return json.load(f)

        archive_sha256 = self._compute_sha256(archive_path)
        if expected_sha256 and archive_sha256 != expected_sha256:
            raise ValueError(
                f"Checksum mismatch for {archive_path.name}: manifest {expected_sha256}, actual {archive_sha256}"
            )

        if q_bronze_dir.exists():
            shutil.rmtree(q_bronze_dir)
        q_quarantine_dir = q_bronze_dir / "quarantine"
        q_quarantine_dir.mkdir(parents=True, exist_ok=True)
        ingestion_ts = datetime.now(timezone.utc).isoformat()
        started = datetime.now(timezone.utc)

        logger.info(
            "Starting Bronze ingestion for %s from archive %s (SHA256: %s, is_synthetic=%s, writer=%s)...",
            quarter_upper, archive_path.name, archive_sha256[:12], is_synthetic, self.writer
        )

        # 2. Inspect archive integrity & members
        with zipfile.ZipFile(archive_path, "r") as zf:
            bad_crc = zf.testzip()
            if bad_crc is not None:
                raise ValueError(f"Corrupted ZIP archive detected in {archive_path.name}: bad CRC on {bad_crc}")

            readme_member = self._find_readme_in_archive(zf)
            tables_to_process = list(self.tables_config.keys()) if self.tables_config else CORE_FAERS_TABLES
            found_members: Dict[str, str] = {}
            missing_required: List[str] = []
            for t_name in tables_to_process:
                member = self._find_table_file_in_archive(zf, t_name)
                if member:
                    found_members[t_name] = member
                else:
                    missing_required.append(t_name)
            deleted_member = self._find_table_file_in_archive(zf, "DELETE")

        if missing_required and not is_synthetic:
            raise ValueError(
                f"Official archive {archive_path.name} is incomplete: missing required tables {missing_required}"
            )

        tables_summary: Dict[str, Any] = {}
        total_valid_rows = 0
        total_quarantined_rows = 0
        reconciliation_failures: List[str] = []

        with tempfile.TemporaryDirectory(prefix="pharmasafe_bronze_") as tmp_dir:
            tmp_path = Path(tmp_dir)
            with zipfile.ZipFile(archive_path, "r") as zf:
                for t_name, member in found_members.items():
                    self._extract_member(zf, member, tmp_path / f"{t_name.lower()}.txt")
                if deleted_member:
                    self._extract_member(zf, deleted_member, tmp_path / "deleted.txt")

            for table_name in tables_to_process:
                if table_name not in found_members:
                    logger.warning("Table %s not found in archive %s. Skipping.", table_name, archive_path.name)
                    tables_summary[table_name] = {"status": "MISSING_IN_ARCHIVE"}
                    continue

                member_name = found_members[table_name]
                extracted_file = tmp_path / f"{table_name.lower()}.txt"
                source_records, header_line = count_source_records(extracted_file)

                if not header_line:
                    logger.warning("Table %s is empty in %s.", table_name, archive_path.name)
                    tables_summary[table_name] = {
                        "status": "EMPTY_FILE", "rows_ingested": 0, "rows_quarantined": 0,
                        "source_records": 0, "is_synthetic": is_synthetic
                    }
                    continue

                header_cols = [c.strip().lower() for c in header_line.split(self.delimiter)]
                # A trailing delimiter yields an empty final header name; data lines carry the same empty tail
                trailing_delimiter = header_cols[-1] == ""
                data_cols = header_cols[:-1] if trailing_delimiter else header_cols
                expected_field_count = len(header_cols)

                configured_spec = self.tables_config.get(table_name, {})
                expected_cols = list(configured_spec.get("columns", {}).keys()) if configured_spec else []
                missing_cols = [c for c in expected_cols if c.lower() not in data_cols]
                if missing_cols:
                    logger.warning("Table %s header missing %d configured columns: %s", table_name, len(missing_cols), missing_cols)
                    if "primaryid" in missing_cols:
                        raise ValueError(f"Critical schema violation: 'primaryid' missing in {table_name} for {quarter_upper}")
                extra_cols = [c for c in data_cols if expected_cols and c not in expected_cols]

                raw_df = self.spark.read.text(str(extracted_file))
                lines_df = raw_df.filter((col("value") != header_line) & (trim(col("value")) != ""))
                delim_regex = "\\" + self.delimiter if self.delimiter in "$^|.*+?\\" else self.delimiter
                split_df = (
                    lines_df
                    .withColumn("fields", split(col("value"), delim_regex, -1))
                    .withColumn("field_count", size(col("fields")))
                )

                # Quarantine: every malformed row is persisted, with a small JSON sample for reading
                malformed_df = (
                    split_df.filter(col("field_count") != expected_field_count)
                    .select(
                        lit(f"COLUMN_COUNT_MISMATCH: expected {expected_field_count}").alias("reason"),
                        col("field_count"),
                        col("value").alias("raw_content"),
                        lit(member_name).alias("source_file"),
                        lit(quarter_upper).alias("quarter"),
                        lit(ingestion_ts).alias("ingestion_timestamp"),
                    )
                )
                quarantine_dir = q_quarantine_dir / table_name.lower()
                self._write_parquet(malformed_df, quarantine_dir)
                malformed_count = self.spark.read.parquet(str(quarantine_dir)).count()
                if malformed_count:
                    sample = [r.asDict() for r in self.spark.read.parquet(str(quarantine_dir)).take(QUARANTINE_SAMPLE_SIZE)]
                    with open(q_quarantine_dir / f"{table_name.lower()}_malformed_sample.json", "w", encoding="utf-8") as qf:
                        json.dump(sample, qf, indent=2)
                    logger.warning("Quarantined %d malformed rows for table %s", malformed_count, table_name)
                else:
                    shutil.rmtree(quarantine_dir)

                valid_df = (
                    split_df.filter(col("field_count") == expected_field_count)
                    .select(
                        *[col("fields").getItem(idx).alias(c) for idx, c in enumerate(data_cols)],
                        lit(quarter_upper).alias("_source_quarter"),
                        lit(member_name).alias("_source_file"),
                        lit(ingestion_ts).alias("_ingestion_timestamp"),
                        lit(is_synthetic).alias("_is_synthetic"),
                    )
                )
                out_dir = q_bronze_dir / table_name.lower()
                self._write_parquet(valid_df, out_dir)
                rows_written = self.spark.read.parquet(str(out_dir)).count()

                # Reconciliation: every independently counted source record is either written or quarantined
                reconciled = (rows_written + malformed_count) == source_records
                if not reconciled:
                    reconciliation_failures.append(table_name)
                    logger.error(
                        "Reconciliation FAILED for %s: source=%d, written=%d, quarantined=%d",
                        table_name, source_records, rows_written, malformed_count
                    )

                total_valid_rows += rows_written
                total_quarantined_rows += malformed_count
                tables_summary[table_name] = {
                    "status": "INGESTED",
                    "member_file": member_name,
                    "source_records": source_records,
                    "rows_ingested": rows_written,
                    "rows_quarantined": malformed_count,
                    "reconciled": reconciled,
                    "parquet_path": str(out_dir),
                    "parquet_size_bytes": sum(p.stat().st_size for p in out_dir.glob("*.parquet")),
                    "columns_count": len(data_cols),
                    "columns": data_cols,
                    "trailing_delimiter": trailing_delimiter,
                    "missing_configured_columns": missing_cols,
                    "unconfigured_columns": extra_cols,
                    "is_synthetic": is_synthetic
                }
                logger.info(
                    "Table %s: source=%d, ingested=%d, quarantined=%d, reconciled=%s",
                    table_name, source_records, rows_written, malformed_count, reconciled
                )

            if deleted_member:
                tables_summary[DELETED_TABLE] = self._ingest_deleted_cases(
                    tmp_path / "deleted.txt", deleted_member, quarter_upper, q_bronze_dir, ingestion_ts, is_synthetic
                )
                if not tables_summary[DELETED_TABLE]["reconciled"]:
                    reconciliation_failures.append(DELETED_TABLE)
            else:
                tables_summary[DELETED_TABLE] = {"status": "MISSING_IN_ARCHIVE"}

        if reconciliation_failures:
            raise ValueError(
                f"Bronze reconciliation failed for {quarter_upper}: tables {reconciliation_failures}. "
                f"No ingestion summary written; inspect {q_bronze_dir}."
            )

        ingestion_summary = {
            "quarter": quarter_upper,
            "archive_filename": archive_path.name,
            "archive_sha256": archive_sha256,
            "archive_sha256_verified_against_manifest": bool(expected_sha256),
            "ingestion_timestamp": ingestion_ts,
            "duration_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 2),
            "parquet_writer": self.writer,
            "is_synthetic": is_synthetic,
            "verification_status": "SYNTHETIC_FIXTURE" if is_synthetic else "VERIFIED_OFFICIAL",
            "readme_file": readme_member,
            "total_valid_rows": total_valid_rows,
            "total_quarantined_rows": total_quarantined_rows,
            "tables": tables_summary,
            "bronze_directory": str(q_bronze_dir)
        }

        with open(summary_path, "w", encoding="utf-8") as sf:
            json.dump(ingestion_summary, sf, indent=2)

        logger.info(
            "Bronze ingestion complete for %s. Total rows: %d (quarantined %d) in %.1fs.",
            quarter_upper, total_valid_rows, total_quarantined_rows, ingestion_summary["duration_seconds"]
        )
        return ingestion_summary
