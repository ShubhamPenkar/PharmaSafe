"""
PharmaSafe Spark Session Manager
Initializes and configures an optimized PySpark session for local and distributed execution.
Handles environment variables and JVM configurations on Windows.
"""

import os
import sys
from typing import Optional
from pyspark.sql import SparkSession
from src.utils.logger import get_logger

logger = get_logger("PharmaSafe.Spark")


def get_spark_session(
    app_name: str = "PharmaSafe",
    master: str = "local[*]",
    shuffle_partitions: int = 4,
    driver_memory: str = "4g",
    extra_configs: Optional[dict] = None
) -> SparkSession:
    """
    Initializes or retrieves an active SparkSession with Windows-safe Python executable paths
    and optimized local execution settings.
    """
    # Ensure worker processes invoke the active virtual environment Python
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, str(v))

    logger.info("Initializing SparkSession '%s' (master=%s)...", app_name, master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialized successfully (Spark version: %s)", spark.version)
    return spark


def stop_spark_session(spark: Optional[SparkSession] = None) -> None:
    """Safely terminates the active Spark session."""
    if spark:
        logger.info("Stopping SparkSession...")
        spark.stop()
    else:
        active = SparkSession.getActiveSession()
        if active:
            logger.info("Stopping active SparkSession...")
            active.stop()
