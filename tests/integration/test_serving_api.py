"""GoldStore and the FastAPI app over the synthetic Gold layer (isolated temp directory)."""

import pytest
from fastapi.testclient import TestClient

from api.main import app
from src.analytics.gold_pipeline import GoldPipeline
from src.analytics.interaction_analytics import InteractionAnalytics
from src.analytics.temporal_analytics import TemporalAnalytics
from src.ingestion.bronze_pipeline import BronzePipeline
from src.ingestion.faers_sample_builder import build_synthetic_faers_quarter, build_synthetic_faers_quarter_2
from src.preprocessing.silver_pipeline import SilverPipeline
from src.serving.gold_store import GoldStore
from src.utils.config_loader import load_analysis_config, load_data_sources_config


@pytest.fixture()
def store(synthetic_gold):
    return GoldStore(synthetic_gold / "gold", silver_dir=synthetic_gold / "silver")


@pytest.fixture()
def client(store):
    app.state.store = store
    yield TestClient(app)
    app.state.store = None


def test_store_overview_and_distinct_counts(store):
    o = store.overview()
    assert o["unique_cases"] == 66 and o["total_report_records_processed"] == 70
    assert o["reporting_quarters"] == 2 and o["potential_signals"] >= 1


def test_search_by_ingredient_and_reported_name(store):
    assert any(r["drug_key"] == "SYNTH_DRUG_ALPHA" for r in store.search_drugs("alpha"))
    assert store.search_drugs("zzzz-no-such-drug") == []


def test_signal_filters_and_sorting(store):
    res = store.signals(drug="SYNTH_DRUG_ALPHA", status=None, exact=True, sort_by="observed_cases")
    assert res["total"] >= 1 and res["items"][0]["observed_cases"] == max(i["observed_cases"] for i in res["items"])
    flagged = store.signals(status="POTENTIAL_SIGNAL")
    assert all(i["screening_status"] == "POTENTIAL_SIGNAL" for i in flagged["items"])
    with pytest.raises(ValueError):
        store.signals(sort_by="not_a_column")


def test_api_endpoints(client):
    assert client.get("/health").json() == {"status": "ok", "gold_available": True}
    ov = client.get("/overview").json()
    assert ov["unique_cases"] == 66 and ov["limitations"]
    assert client.get("/drugs", params={"q": "alpha"}).json()["items"]
    d = client.get("/drugs/SYNTH_DRUG_ALPHA").json()
    assert d["reactions"] and d["outcomes"] is not None and d["reporting_period"] == ["2024Q1", "2024Q2"]
    sig = client.get("/signals/SYNTH_DRUG_ALPHA/SYNTH_EVENT_TACHY").json()
    assert (sig["signal"]["a"], sig["signal"]["b"], sig["signal"]["c"], sig["signal"]["d"]) == (33, 5, 2, 26)
    assert "cannot_establish" in sig["explanation"] and sig["explanation"]["headline"].startswith("Potential safety signal")
    assert client.get("/trends", params={"drug": "SYNTH_DRUG_ALPHA"}).json()["quarterly_trends"]
    assert "note" in client.get("/combinations").json()
    assert client.get("/data-quality").status_code == 200


def test_api_errors(client):
    assert client.get("/drugs/NOPE").status_code == 404
    assert client.get("/signals/NOPE/NOPE").status_code == 404
    assert client.get("/signals", params={"sort_by": "bogus"}).status_code == 422
    assert client.get("/signals", params={"role_policy": "bad"}).status_code == 422
    assert client.get("/drugs", params={"q": "a"}).status_code == 422


def test_api_degrades_without_gold(tmp_path, monkeypatch):
    monkeypatch.setenv("PHARMASAFE_GOLD_DIR", str(tmp_path))
    app.state.store = None
    c = TestClient(app)
    assert c.get("/health").json()["status"] == "degraded"
    assert c.get("/overview").status_code == 503
