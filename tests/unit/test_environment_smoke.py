"""
PharmaSafe Smoke & Environment Unit Test
Verifies that all core packages, configurations, and PySpark session managers operate properly.
"""

import sys
from pathlib import Path
import pytest

# Ensure PharmaSafe root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.config_loader import load_data_sources_config, load_analysis_config, load_reference_signals
from src.utils.spark_session import get_spark_session, stop_spark_session


def test_core_dependencies_import():
    """Verify that all essential third-party libraries import cleanly."""
    import pyspark
    import pandas as pd
    import pyarrow as pa
    import scipy
    import streamlit
    import yaml
    import plotly

    assert pyspark.__version__ is not None
    assert pd.__version__ is not None
    assert pa.__version__ is not None
    assert scipy.__version__ is not None


def test_configurations_load():
    """Verify that YAML configuration files load and contain expected top-level keys."""
    data_sources = load_data_sources_config()
    assert "faers_source" in data_sources
    assert "quarters" in data_sources
    assert "tables" in data_sources
    assert "DEMO" in data_sources["tables"]
    assert "DRUG" in data_sources["tables"]
    assert "REAC" in data_sources["tables"]

    analysis = load_analysis_config()
    assert "screening_criteria" in analysis
    assert "statistics" in analysis
    assert analysis["screening_criteria"]["primary"]["min_cases"] == 3
    assert analysis["screening_criteria"]["primary"]["min_ror_ci_lower"] == 1.0

    ref_signals = load_reference_signals()
    assert "reference_signals" in ref_signals
    assert len(ref_signals["reference_signals"]) > 0


def test_spark_session_lifecycle():
    """Verify that PySpark initializes, computes a basic aggregation, and stops cleanly."""
    spark = get_spark_session(app_name="PharmaSafeSmokeTest", master="local[1]", shuffle_partitions=1)
    try:
        data = [("Aspirin", "Headache", 10), ("Aspirin", "Nausea", 5), ("Ibuprofen", "Headache", 8)]
        df = spark.createDataFrame(data, ["drug", "reaction", "count"])
        assert df.count() == 3

        agg = df.groupBy("drug").sum("count").collect()
        drug_counts = {row["drug"]: row["sum(count)"] for row in agg}
        assert drug_counts["Aspirin"] == 15
        assert drug_counts["Ibuprofen"] == 8
    finally:
        stop_spark_session(spark)
