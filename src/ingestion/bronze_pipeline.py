"""
PharmaSafe Bronze Layer Ingestion Pipeline
Ingests verified FAERS quarterly ASCII archives into the Bronze Medallion layer.
Enforces schema validation, preserves raw files, extracts dollar-delimited records using PySpark
DataFrames for distributed processing without in-memory list bottlenecks, quarantines malformed
rows with auditable error reasons, guards against unverified synthetic fixtures entering the official
data stream, and ensures idempotent execution.
"""

import os
import sys
import json
import hashlib
import zipfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, lit

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_data_sources_config, get_project_root
from src.utils.spark_session import get_spark_session
from src.ingestion.download_manager import DownloadManager, SYNTHETIC_MARKERS

logger = get_logger("PharmaSafe.Bronze")

CORE_FAERS_TABLES = ["DEMO", "DRUG", "REAC", "OUTC", "INDI", "THER", "RPSR"]


class BronzePipeline:
    """
    Ingests official FAERS ASCII archives into the Bronze layer with validation and quarantine gates.
    Leverages PySpark DataFrames for scalable parsing and streaming Parquet generation.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        spark: Optional[SparkSession] = None
    ):
        self.config = config or load_data_sources_config()
        self.root = get_project_root()
        
        raw_conf = self.config.get("storage", {}).get("raw_dir", "data/raw")
        self.raw_dir = Path(raw_conf) if Path(raw_conf).is_absolute() else self.root / raw_conf

        bronze_conf = self.config.get("storage", {}).get("bronze_dir", "data/bronze")
        self.bronze_dir = Path(bronze_conf) if Path(bronze_conf).is_absolute() else self.root / bronze_conf
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

        self.tables_config = self.config.get("tables", {})
        self.delimiter = self.config.get("faers_source", {}).get("delimiter", "$")
        self._spark = spark

    @property
    def spark(self) -> SparkSession:
        """Lazily initializes or retrieves the active SparkSession."""
        if self._spark is None:
            self._spark = get_spark_session(app_name="PharmaSafe.Bronze")
        return self._spark

    def _find_table_file_in_archive(self, zf: zipfile.ZipFile, table_name: str) -> Optional[str]:
        """Finds the member path for a given table inside the zip archive (case-insensitive)."""
        pattern = table_name.upper()
        for name in zf.namelist():
            base = Path(name).name.upper()
            if base.startswith(pattern) and base.endswith((".TXT", ".DAT")):
                return name
        return None

    def _find_readme_in_archive(self, zf: zipfile.ZipFile) -> Optional[str]:
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

    def ingest_archive(
        self,
        archive_path: Path,
        quarter_id: str,
        force: bool = False,
        allow_synthetic: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        Parses and ingests all FAERS tables from a verified zip archive into the Bronze layer.
        Rejects synthetic fixtures unless allow_synthetic=True is explicitly passed,
        or unless running in an isolated test environment (outside data/raw).
        Uses PySpark DataFrames for distributed transformation and streaming Parquet generation.
        """
        if not archive_path.exists():
            raise FileNotFoundError(f"Archive not found: {archive_path}")

        # 1. Scan for synthetic markers
        markers = DownloadManager.scan_for_synthetic_markers(archive_path)
        is_synthetic = len(markers) > 0

        if allow_synthetic is None:
            # Production guard: If the archive is inside project data/raw, never allow synthetic fixtures
            try:
                prod_raw = (self.root / "data/raw").resolve()
                archive_resolved = archive_path.resolve()
                if archive_resolved.is_relative_to(prod_raw):
                    allow_synthetic = False
                else:
                    allow_synthetic = True
            except Exception:
                allow_synthetic = not is_synthetic

        if is_synthetic and not allow_synthetic:
            raise ValueError(
                f"Ingestion rejected for {archive_path.name}: Archive contains synthetic fixture markers {markers}. "
                f"Synthetic fixtures cannot be ingested into the official Bronze layer."
            )

        quarter_upper = quarter_id.upper()
        q_bronze_dir = self.bronze_dir / quarter_upper
        q_quarantine_dir = q_bronze_dir / "quarantine"
        summary_path = q_bronze_dir / "ingestion_summary.json"

        # Check idempotency
        if not force and summary_path.exists():
            logger.info("Quarter %s already ingested in Bronze at %s. Loading existing summary.", quarter_upper, q_bronze_dir)
            with open(summary_path, "r", encoding="utf-8") as f:
                return json.load(f)

        q_bronze_dir.mkdir(parents=True, exist_ok=True)
        q_quarantine_dir.mkdir(parents=True, exist_ok=True)
        ingestion_ts = datetime.now(timezone.utc).isoformat()
        archive_sha256 = self._compute_sha256(archive_path)

        logger.info(
            "Starting Bronze ingestion for %s from archive %s (SHA256: %s, is_synthetic=%s)...",
            quarter_upper, archive_path.name, archive_sha256[:12], is_synthetic
        )

        # 2. Inspect archive integrity & members
        with zipfile.ZipFile(archive_path, "r") as zf:
            bad_crc = zf.testzip()
            if bad_crc is not None:
                raise ValueError(f"Corrupted ZIP archive detected in {archive_path.name}: bad CRC on {bad_crc}")
            
            readme_member = self._find_readme_in_archive(zf)
            logger.info("Found archive README member: %s", readme_member)

            # Check that expected tables exist
            tables_to_process = list(self.tables_config.keys()) if self.tables_config else CORE_FAERS_TABLES
            found_members: Dict[str, str] = {}
            missing_required: List[str] = []

            for t_name in tables_to_process:
                member = self._find_table_file_in_archive(zf, t_name)
                if member:
                    found_members[t_name] = member
                else:
                    missing_required.append(t_name)

            if missing_required and not is_synthetic:
                raise ValueError(
                    f"Official archive {archive_path.name} is incomplete: missing required tables {missing_required}"
                )

        # 3. Distributed extraction and Spark ingestion via streaming temp files
        tables_summary: Dict[str, Any] = {}
        total_valid_rows = 0
        total_quarantined_rows = 0

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            logger.info("Extracting ASCII files from %s to temp directory...", archive_path.name)
            
            with zipfile.ZipFile(archive_path, "r") as zf:
                for t_name, member in found_members.items():
                    zf.extract(member, tmp_path)

            spark_inst = self.spark

            for table_name in tables_to_process:
                if table_name not in found_members:
                    logger.warning("Table %s not found in archive %s. Skipping.", table_name, archive_path.name)
                    tables_summary[table_name] = {"status": "MISSING_IN_ARCHIVE"}
                    continue

                member_name = found_members[table_name]
                extracted_file = tmp_path / member_name
                logger.info("Processing table %s (file: %s)...", table_name, member_name)

                # Read raw file with PySpark text reader
                raw_df = spark_inst.read.text(str(extracted_file))
                first_row = raw_df.take(1)
                if not first_row:
                    logger.warning("Table %s is empty in %s.", table_name, archive_path.name)
                    tables_summary[table_name] = {
                        "status": "EMPTY_FILE",
                        "rows_ingested": 0,
                        "rows_quarantined": 0,
                        "is_synthetic": is_synthetic
                    }
                    continue

                # Header extraction and normalization
                header_line = first_row[0][0]
                header_cols = [c.strip().lower() for c in header_line.split(self.delimiter)]
                actual_num_cols = len(header_cols)

                # Schema validation against configured columns
                configured_spec = self.tables_config.get(table_name, {})
                expected_cols = list(configured_spec.get("columns", {}).keys()) if configured_spec else []
                if expected_cols:
                    missing_cols = [c for c in expected_cols if c.lower() not in header_cols]
                    if missing_cols:
                        logger.warning(
                            "Table %s header missing %d configured columns: %s",
                            table_name, len(missing_cols), missing_cols
                        )
                        # If primary/foreign key is missing, fail visibly
                        if "primaryid" in missing_cols:
                            raise ValueError(
                                f"Critical schema violation: 'primaryid' missing in {table_name} for {quarter_upper}"
                            )

                # Filter header line and blank lines
                lines_df = raw_df.filter((col("value") != header_line) & (col("value") != ""))

                # Split on delimiter
                # Note: PySpark split uses regex, escape delimiter if needed
                delim_regex = "\\" + self.delimiter if self.delimiter in ["$", "^", "|", ".", "*", "+", "?"] else self.delimiter
                split_df = lines_df.withColumn("fields", split(col("value"), delim_regex, -1))
                split_df = split_df.withColumn("field_count", size(col("fields")))

                # Separate valid vs malformed lines
                malformed_df = split_df.filter(col("field_count") != actual_num_cols)
                valid_df = split_df.filter(col("field_count") == actual_num_cols)

                # Check for malformed rows for quarantine
                quarantined_records: List[Dict[str, Any]] = []
                malformed_count = malformed_df.count()
                if malformed_count > 0:
                    malformed_rows = malformed_df.select("value", "field_count").take(1000)
                    for r in malformed_rows:
                        quarantined_records.append({
                            "reason": f"COLUMN_COUNT_MISMATCH: expected {actual_num_cols}, got {r['field_count']}",
                            "raw_content": r["value"],
                            "source_file": member_name,
                            "quarter": quarter_upper,
                            "ingestion_timestamp": ingestion_ts
                        })

                # Write quarantine JSON if malformed rows exist
                if quarantined_records:
                    quarantine_file = q_quarantine_dir / f"{table_name.lower()}_malformed.json"
                    with open(quarantine_file, "w", encoding="utf-8") as qf:
                        json.dump(quarantined_records, qf, indent=2)
                    logger.warning(
                        "Quarantined %d malformed rows for table %s into %s",
                        len(quarantined_records), table_name, quarantine_file.name
                    )

                # Project columns and append metadata
                for idx, c in enumerate(header_cols):
                    valid_df = valid_df.withColumn(c, col("fields").getItem(idx))

                valid_df = (
                    valid_df
                    .drop("value", "fields", "field_count")
                    .withColumn("_source_quarter", lit(quarter_upper))
                    .withColumn("_source_file", lit(member_name))
                    .withColumn("_ingestion_timestamp", lit(ingestion_ts))
                    .withColumn("_is_synthetic", lit(is_synthetic))
                )

                out_cols = list(valid_df.columns)
                out_parquet = q_bronze_dir / f"{table_name.lower()}.parquet"

                # Define Arrow schema explicitly with string fields and boolean _is_synthetic
                pa_fields = [
                    pa.field(c, pa.bool_()) if c == "_is_synthetic" else pa.field(c, pa.string())
                    for c in out_cols
                ]
                pa_schema = pa.schema(pa_fields)

                def make_arrow_batch(iterator):
                    import pandas as pd
                    rows = list(iterator)
                    if rows:
                        pdf = pd.DataFrame(rows, columns=out_cols)
                        batch = pa.RecordBatch.from_pandas(pdf, schema=pa_schema)
                        return [batch]
                    return []

                # Stream partition batches via iterator directly into ParquetWriter (constant memory)
                batches = valid_df.rdd.mapPartitions(make_arrow_batch).toLocalIterator()
                writer = None
                rows_written = 0

                for b in batches:
                    if writer is None:
                        writer = pq.ParquetWriter(str(out_parquet), pa_schema, compression="snappy")
                    writer.write_batch(b)
                    rows_written += b.num_rows

                if writer is None:
                    # Table had 0 valid data rows (e.g. empty table with header only)
                    writer = pq.ParquetWriter(str(out_parquet), pa_schema, compression="snappy")
                writer.close()

                total_valid_rows += rows_written
                total_quarantined_rows += len(quarantined_records)

                tables_summary[table_name] = {
                    "status": "INGESTED",
                    "member_file": member_name,
                    "rows_ingested": rows_written,
                    "rows_quarantined": len(quarantined_records),
                    "parquet_path": str(out_parquet),
                    "columns_count": len(header_cols),
                    "columns": header_cols,
                    "is_synthetic": is_synthetic
                }
                logger.info(
                    "Table %s ingested: %d valid rows, %d quarantined, parquet size: %d bytes",
                    table_name, rows_written, len(quarantined_records), out_parquet.stat().st_size
                )

        ingestion_summary = {
            "quarter": quarter_upper,
            "archive_filename": archive_path.name,
            "archive_sha256": archive_sha256,
            "ingestion_timestamp": ingestion_ts,
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
            "Bronze ingestion complete for %s. Total rows: %d. Summary saved to %s",
            quarter_upper, total_valid_rows, summary_path.name
        )
        return ingestion_summary
