# PharmaSafe: Large-Scale Pharmaceutical Safety Analytics

> **Post-Marketing Drug Safety Surveillance & Statistical Pharmacovigilance Platform**  
> Built with **Apache Spark / PySpark**, **Apache Parquet**, **PyArrow**, **SciPy**, and **Pandas**.  
> Primary Official Data Source: **FDA Adverse Event Reporting System (FAERS / AEMS)**.

---

## 1. Project Overview

**PharmaSafe** is an open, reproducible pharmaceutical safety analytics platform that processes adverse-event reports from the U.S. Food and Drug Administration (FDA FAERS / AEMS). It identifies disproportionately reported drug–adverse event combinations, analyzes safety reporting trends across time, and structures evidence for pharmacovigilance teams.

### Core Objectives:
1. **Robust Data Engineering:** Ingest, clean, validate, deduplicate, and process large-scale multi-table FAERS datasets into an analytics-ready Medallion architecture (Bronze &rarr; Silver &rarr; Gold) using PySpark and Parquet.
2. **Validated Statistical Pharmacovigilance:** Implement case-based disproportionality signal detection metrics:
   - **Reporting Odds Ratio (ROR)** with exact 95% Wald confidence intervals and Haldane-Anscombe zero-cell correction.
   - **Proportional Reporting Ratio (PRR)** with exact 95% log-transformed confidence intervals.
   - Configurable regulatory screening thresholds ($a \ge 3$, $\text{ROR}_{025} > 1.0$, $\text{PRR} \ge 2.0$).
3. **Rigorous Data Provenance:** Strict cryptographic verification (SHA-256 and ZIP CRC) of official FDA quarterly archives, total isolation from synthetic test fixtures, and comprehensive data-quality auditing.

---

## 2. Architecture & Data Flow

PharmaSafe implements a Medallion Lakehouse architecture optimized for multi-million row adverse-event datasets:

```
  FDA FAERS Quarterly ASCII Archives ($ Delimited)
                         │
                         ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Bronze Layer: Raw Ingestion & Source Manifest              │
  │  - Cryptographic verification (SHA-256 & ZIP CRC32)         │
  │  - PySpark distributed parsing with schema validation       │
  │  - PyArrow streaming writer (Snappy Parquet)                │
  │  - Malformed row quarantine & audit logging                 │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Silver Layer: Cleaned, Deduplicated Case-Level Tables      │
  │  - Deterministic deduplication (max caseversion / fda_dt)   │
  │  - Relational orphan removal across all child tables        │
  │  - Drug name standardization & MedDRA reaction cleaning     │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Gold Layer: Analytical Marts & Signal Detection Engine     │
  │  - Case-level 2x2 contingency matrix generation (a, b, c, d)│
  │  - Exact ROR, PRR, 95% CIs, and regulatory screening        │
  │  - Multi-quarter longitudinal trend analysis                │
  └──────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  Presentation Layer: Interactive Pharmacovigilance Tools    │
  │  - Safety Signal Screener & Drug Explorer                   │
  │  - Benchmark Validation against Known Boxed Warnings        │
  └─────────────────────────────────────────────────────────────┘
```

---

## 3. Technology Stack

- **Distributed Data Processing:** Apache Spark 4.2.0 (PySpark SQL & DataFrame API)
- **High-Performance Storage:** Apache Arrow / PyArrow 26.0.0, Parquet (Snappy compression)
- **Mathematical & Statistical Computations:** NumPy 2.4.4, SciPy 1.17.1, Pandas 3.0.1
- **Language & Runtime:** Python 3.12+ (tested on Windows 11 with OpenJDK 25 LTS / 17+)
- **Data Acquisition & Web Verification:** Requests 2.32.5, Beautiful Soup 4, PyYAML 6.0.3
- **Test Framework:** Pytest 9.1.1 (100% automated test pass rate)

---

## 4. Current Development Status

| Component | Status | Description |
| :--- | :--- | :--- |
| **Data Procurement & Audit** | **Complete** | 8 consecutive quarters (2024Q3–2026Q2, 514 MB) downloaded, SHA-256 & CRC verified in `data/raw/manifest.json`. |
| **Synthetic Fixture Isolation** | **Complete** | Synthetic fixtures quarantined to `data/synthetic/fixtures/` and isolated from production pipelines. |
| **Bronze Ingestion Engine** | **Complete** | Memory-efficient PySpark DataFrame reader with chunked PyArrow streaming to Parquet (Windows `winutils` safe). |
| **Official 2024Q3 Bronze Pilot** | **Complete** | 5,752,873 rows processed across all 7 FAERS tables with 0 malformed rows and 100.00% foreign key integrity. |
| **Silver Deduplication Logic** | **Complete** | Case version resolver and relational orphan pruner verified on synthetic benchmarks. |
| **Signal Engine (ROR / PRR)** | **Complete** | Exact statistical math, Haldane-Anscombe zero-cell correction, and screening classifier verified by unit tests. |
| **Full 8-Quarter Silver Ingestion** | In Progress | Scaling deduplication and child table joins across all 8 official quarters. |
| **Gold Analytics & Dashboard** | Planned | Production of analytical safety marts and interactive Streamlit UI. |

---

## 5. Repository Structure

```
PharmaSafe/
├── configs/
│   ├── data_sources.yaml        # FDA FAERS endpoints, quarter catalog, and table schemas
│   ├── analysis.yaml            # Screening thresholds, statistical parameters, and filters
│   └── reference_signals.yaml   # Benchmark ground truth alerts and negative controls
├── data/
│   ├── raw/
│   │   ├── manifest.json        # Provenance manifest for official FDA downloads
│   │   └── official/            # Official quarterly ZIP archives (excluded from Git)
│   ├── bronze/                  # Official raw Parquet tables (excluded from Git)
│   ├── silver/                  # Cleaned, deduplicated Parquet tables (excluded from Git)
│   ├── gold/                    # Analytical signal marts (excluded from Git)
│   └── synthetic/
│       ├── fixtures/            # Deterministic synthetic test ZIPs for unit tests
│       └── manifest.json        # Synthetic test fixture metadata
├── docs/
│   ├── data_dictionary_and_schemas.md   # Complete FAERS table definitions (DEMO, DRUG, etc.)
│   └── methodology_and_counting_rules.md # Statistical formulas and counting rules
├── reports/
│   ├── data_source_audit.md             # Forensic audit report of existing project data
│   ├── data_source_audit.json           # Machine-readable audit findings
│   ├── bronze_pilot_2024q3_report.md    # Official 2024Q3 Bronze pilot execution report
│   └── bronze_pilot_2024q3_summary.json # Verification counts and schema checks
├── scripts/
│   ├── acquire_and_verify_official_data.py # Automated FDA scraper and archive verifier
│   ├── audit_existing_data.py              # Local data source audit script
│   ├── check_target_quarters.py            # Quarter target validation
│   ├── probe_fda_page.py                   # FDA export index inspection utility
│   ├── run_bronze_pilot_2024q3.py          # Official 2024Q3 Bronze pilot pipeline runner
│   └── run_medallion_pipeline.py           # Synthetic pipeline test harness
├── src/
│   ├── ingestion/               # Bronze pipeline, download manager, sample builder
│   ├── preprocessing/           # Case deduplication and Silver pipeline
│   ├── signal_detection/        # Contingency tables, ROR, PRR, and CI engine
│   ├── validation/              # Data quality gates and audit checkers
│   └── utils/                   # Spark session manager, logging, config loader
├── tests/
│   ├── fixtures/                # Synthetic FAERS generator for unit testing
│   ├── unit/                    # Unit tests for metrics, deduplication, and ingestion
│   └── integration/             # Integration tests for official Bronze artifacts
├── requirements.txt             # Pinned project dependencies
└── README.md                    # Project documentation
```

---

## 6. Setup & Installation

### Prerequisites
- Python 3.10+ (Tested on Python 3.12.6)
- Java 17+ or Java 21+ LTS (Tested with OpenJDK 25)
- Git

### Installation Steps
```powershell
# 1. Clone repository
git clone https://github.com/ShubhamPenkar/PharmaSafe.git
cd PharmaSafe

# 2. Create and activate virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requirements.txt
```

---

## 7. How to Acquire Official FDA Data

Official FAERS archives are not stored in Git due to file sizes (514 MB total across 8 quarters). To acquire and verify the official datasets directly from the FDA:

```powershell
# Run the automated downloader and cryptographic verifier
python scripts/acquire_and_verify_official_data.py
```

This script:
1. Queries the official FDA quarterly data export directory.
2. Downloads all 8 target quarterly archives (`2024Q3` through `2026Q2`) into `data/raw/official/{quarter}/`.
3. Verifies SHA-256 checksums and tests ZIP CRC integrity.
4. Confirms that all 7 required ASCII tables (`DEMO`, `DRUG`, `REAC`, `OUTC`, `INDI`, `THER`, `RPSR`) and documentation are present.
5. Updates `data/raw/manifest.json`.

---

## 8. How to Run the Pipeline

### Running the Official Bronze Pilot (2024 Q3)
To process the real official 2024Q3 archive into verified Bronze Parquet tables:
```powershell
python scripts/run_bronze_pilot_2024q3.py
```
This parses over 5.75 million records with PySpark, validates schemas, quarantines malformed rows (0 in 2024Q3), streams partitions into Snappy-compressed Parquet files under `data/bronze/2024Q3/`, and generates execution reports in `reports/`.

### Running Synthetic Pipeline Tests
To execute an end-to-end test of Bronze, Silver deduplication, and Gold signal detection on isolated synthetic fixtures:
```powershell
python scripts/run_medallion_pipeline.py
```

---

## 9. Running Tests

PharmaSafe includes an automated test suite verifying statistical computations, deduplication logic, download integrity, and official data isolation:

```powershell
pytest tests/ -v
```

All 19 tests run locally and assert:
- Exact ROR and PRR mathematical correctness against manual 2x2 calculations.
- Haldane-Anscombe zero-cell correction when $b=0$ or $c=0$.
- Case deduplication resolving to latest `caseversion` and removal of orphan records.
- Strict rejection and quarantine of corrupt or unverified archives.
- Total isolation of synthetic test fixtures from production pipelines.

---

## 10. Important Scientific Limitations

PharmaSafe is an analytical decision-support and research platform. Users and researchers must interpret findings in accordance with pharmacovigilance principles:
1. **Disproportionality $\ne$ Causation:** Statistical disproportionality indicates an elevated frequency of reporting relative to other drugs in the database; it does **not** prove biological or clinical causation.
2. **Absence of Exposure Denominators:** FAERS captures spontaneous adverse event reports without recording the total number of patients exposed to the drug. Consequently, **true incidence rates and absolute risk cannot be calculated**.
3. **Reporting Biases:** Spontaneous reporting systems are subject to significant biases, including under-reporting, Weber effect (reporting peaks following initial drug launch), notoriety bias (surges following media coverage or regulatory warnings), and confounding by indication.
4. **Synthetic Testing vs. Genuine Findings:** Test fixtures located in `data/synthetic/fixtures/` contain planted markers (e.g., `SYNTH_DRUG_ALPHA`) designed solely for regression testing and software verification. Official analytical findings are derived exclusively from verified FDA data recorded in `data/raw/manifest.json`.
