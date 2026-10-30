# PharmaSafe: Large-Scale Pharmaceutical Safety Analytics

Disproportionality analysis of FDA FAERS / AEMS adverse-event reports with Apache Spark, Parquet, a FastAPI service and a Streamlit dashboard.

> PharmaSafe reports **potential safety signals**: drug–reaction pairs reported together more often than the reporting background. This is a screening result for further investigation. It is not evidence of causation, and spontaneous reports have no exposure denominator, so no incidence or patient-level risk is computed.

---

## 1. What it does

1. **Acquires and verifies** eight consecutive FAERS quarterly extracts (2024 Q3 – 2026 Q2): SHA-256 and ZIP CRC checks recorded in `data/raw/manifest.json`, with synthetic test fixtures kept strictly separate.
2. **Bronze:** parses all seven tables (DEMO, DRUG, REAC, OUTC, INDI, THER, RPSR) plus FDA's deleted-case lists with PySpark. Each table's output is reconciled against an independent source line count; malformed rows are quarantined to Parquet.
3. **Silver:** types and normalizes fields (reported values are kept next to normalized ones), classifies duplicate reports, keeps **every case version with a validity interval** so a later follow-up never rewrites an earlier snapshot, and separates true orphan rows from superseded versions.
4. **Gold:** case-level marts where every case counts once per drug (active ingredient), reaction and pair; 2×2 tables; ROR, PRR, 95% intervals, Yates chi-square and configurable screening for three drug-role policies; per-quarter statistics and signal persistence; drug-pair (co-reporting) analytics.
5. **Validates:** reference-signal recovery with negative controls, a threshold/role-policy robustness grid, and a discovery→evaluation temporal check.
6. **Serves:** a read-only FastAPI service and a six-page Streamlit dashboard over the Gold tables, with a plain-language explanation for every signal.

See [docs/architecture.md](docs/architecture.md), [docs/methodology_and_counting_rules.md](docs/methodology_and_counting_rules.md), [docs/data_dictionary_and_schemas.md](docs/data_dictionary_and_schemas.md) and [docs/limitations.md](docs/limitations.md).

---

## 2. Results on the eight official quarters

<!-- RESULTS -->

---

## 3. Setup

Requirements: Python 3.12, Java 17+ (tested with OpenJDK 25), ~25 GB free disk for the full dataset and derived layers.

```bash
git clone https://github.com/ShubhamPenkar/PharmaSafe.git && cd PharmaSafe
python3.12 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 4. Get the data

Archives (~514 MB) are not stored in Git.

```bash
python scripts/acquire_and_verify_official_data.py
```

This downloads the eight quarters into `data/raw/official/<quarter>/`, verifies SHA-256 and CRC, and writes `data/raw/manifest.json`. The FDA server can be slow (we observed well under 1 MB/s per connection); downloads can be resumed with `curl -C -`, after which the pipeline re-verifies every archive against the manifest checksum before reading it.

## 5. Run

```bash
python scripts/run_full_pipeline.py            # Bronze → Silver → Gold → temporal → combinations → validation → quality report
python scripts/run_full_pipeline.py --quarters 2024Q3 --skip-validation   # one-quarter pilot
python scripts/run_full_pipeline.py --asof 2025Q2                         # also build a historical (as-of) Gold snapshot
python scripts/run_full_pipeline.py --synthetic                            # isolated synthetic fixtures; writes only under data/synthetic
python -m src.validation.performance_benchmark                             # Pandas vs PySpark benchmark → reports/performance_report.md
```

Individual stages: `scripts/run_bronze_pipeline.py`, `run_silver_pipeline.py`, `run_gold_pipeline.py`. Completed Bronze quarters are skipped unless `--force` is given.

```bash
streamlit run dashboard/app.py                 # dashboard on :8501
uvicorn api.main:app --reload                  # API on :8000, interactive docs at /docs
```

Docker (`Dockerfile`, `docker-compose.yml`): `docker compose run --rm pipeline` then `docker compose up dashboard api`. The image mounts `./data` and `./reports`; it has not been build-tested in the environment this was developed in.

## 6. API

| Endpoint | Purpose |
| :--- | :--- |
| `GET /health` | Service and Gold availability |
| `GET /overview` | Dataset counts, top drugs/reactions/pairs, outcomes, signal counts, quarterly volume |
| `GET /drugs?q=` | Search by active ingredient or reported product name |
| `GET /drugs/{drug}` | Profile: reactions with ROR/PRR, outcomes, roles, demographics, quarterly cases |
| `GET /reactions` | Reaction terms by case count |
| `GET /signals` | Filter, sort and page candidate signals |
| `GET /signals/{drug}/{reaction}` | One pair with 2×2 counts and its explanation |
| `GET /trends` | Quarterly volume, per-quarter signal statistics, persistence |
| `GET /combinations` | Co-reported drug pairs and pair-reaction statistics |
| `GET /data-quality` | Latest audit |

## 7. Tests

```bash
pytest tests/
```

Unit tests cover the ROR/PRR mathematics (including zero cells and Spark-vs-scalar parity), report-duplicate classification, version ranking and as-of snapshots, drug-ingredient normalization, hand-computed per-quarter and drug-pair tables, validation logic and the quality auditor. Integration tests run Bronze → Silver → Gold → API → dashboard pages on deterministic **synthetic** fixtures in temporary directories, and reconcile the official Bronze output when it exists locally. Synthetic data tests software only; it is never reported as safety evidence.

## 8. Repository layout

```
configs/      data_sources.yaml, analysis.yaml (screening, temporal, combinations, robustness), reference_signals.yaml
scripts/      acquire_and_verify_official_data.py, run_{bronze,silver,gold,full}_pipeline.py
src/
  ingestion/        download_manager, bronze_pipeline, faers_sample_builder (test fixtures only)
  preprocessing/    silver_pipeline, case_deduplicator, normalization, drug_normalization
  analytics/        gold_pipeline, temporal_analytics, interaction_analytics
  signal_detection/ metrics (scalar reference), signal_engine (Spark), signal_ranking, explain
  validation/       quality_checks, benchmark_evaluator, performance_benchmark
  serving/          gold_store (read-only PyArrow access)
api/          FastAPI app          dashboard/   Streamlit app and pages
tests/        unit/ and integration/   docs/ reports/   Dockerfile, docker-compose.yml
```

`data/raw/official`, `data/bronze`, `data/silver` and `data/gold` are generated and excluded from Git.

## 9. Scientific limitations

Summarised from [docs/limitations.md](docs/limitations.md): disproportionality is not causation; there is no exposure denominator; spontaneous reporting is biased by under-reporting, stimulated reporting and confounding by indication; quarterly counts show reporting volume, not occurrence; drug keys use FDA-mapped ingredients with conservative salt stripping, so unmapped brand spellings stay separate; reactions are MedDRA preferred terms without grouping; drug co-reporting is not interaction; screening thresholds are project choices (the permissive defaults flag a large share of stored pairs, so rank by ROR lower bound rather than treating every flag as equally strong).
