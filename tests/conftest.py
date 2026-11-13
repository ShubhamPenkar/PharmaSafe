import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session, stop_spark_session

from src.analytics.gold_pipeline import GoldPipeline
from src.analytics.interaction_analytics import InteractionAnalytics
from src.analytics.temporal_analytics import TemporalAnalytics
from src.ingestion.bronze_pipeline import BronzePipeline
from src.ingestion.faers_sample_builder import build_synthetic_faers_quarter, build_synthetic_faers_quarter_2
from src.preprocessing.silver_pipeline import SilverPipeline
from src.utils.config_loader import load_analysis_config, load_data_sources_config


@pytest.fixture(scope="session")
def spark():
    s = get_spark_session(app_name="PharmaSafeTest", master="local[2]", shuffle_partitions=2)
    yield s
    stop_spark_session(s)


@pytest.fixture(scope="session")
def synthetic_gold(tmp_path_factory, spark):
    root = tmp_path_factory.mktemp("serving")
    cfg = load_data_sources_config()
    cfg["storage"] = {**cfg["storage"], "bronze_dir": str(root / "bronze"), "silver_dir": str(root / "silver"), "gold_dir": str(root / "gold")}
    analysis = load_analysis_config()
    analysis["combinations"].update({"min_pair_cases": 1, "min_cases": 1})
    (root / "raw").mkdir()
    bp = BronzePipeline(config=cfg, spark=spark)
    for q, build in (("2024Q1", lambda p: build_synthetic_faers_quarter(p, quarter_id="2024Q1", seed=42)),
                     ("2024Q2", lambda p: build_synthetic_faers_quarter_2(p, seed=43))):
        z = root / f"raw/faers_ascii_{q.lower()}.zip"
        build(z)
        bp.ingest_archive(z, q, allow_synthetic=True)
    SilverPipeline(config=cfg, spark=spark).run()
    GoldPipeline(cfg, analysis, spark).run()
    TemporalAnalytics(cfg, analysis, spark).run()
    InteractionAnalytics(cfg, analysis, spark).run()
    return root


