# PharmaSafe — Phase 0: Data Source Authenticity Audit & Official Dataset Acquisition Report

**Audit Date:** October 29–30, 2026  
**Auditor:** PharmaSafe Pharmacovigilance & Data Engineering Team  
**Official Source:** [FDA Adverse Event Monitoring System (AEMS) / FAERS Latest Quarterly Data Files](https://www.fda.gov/drugs/fda-adverse-event-monitoring-system-aems/fda-adverse-event-monitoring-system-aems-latest-quarterly-data-files)  
**FDA FIS Index:** [FDA Quarterly Data Extract Files (fis.fda.gov)](https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html)  
**Acceptance Gate Status:** **PASSED** (8/8 Official Quarters Acquired, Verified, and Isolated)

---

## Executive Summary

Prior to initiating large-scale pharmaceutical safety analytics, signal detection, or machine learning, an exhaustive audit was conducted across the PharmaSafe repository (`PharmaSafe`) to verify the provenance, integrity, and authenticity of existing pharmaceutical safety datasets.

The audit revealed that **100% of the pre-existing data in `data/raw/`, `data/bronze/`, and `data/silver/` was synthetic test fixture data**. Crucially, the legacy pipeline had generated 3 KB mock archives directly into `data/raw/` (`faers_ascii_2024q1.zip` and `faers_ascii_2024q2.zip`) and registered them in `data/raw/manifest.json` as `VERIFIED` with official FDA download links, creating a severe data integrity vulnerability where synthetic data masqueraded as real FDA pharmacovigilance reports.

To rectify this:
1. **Existing synthetic files were isolated** into `data/synthetic/fixtures/`, `data/synthetic/bronze/`, and `data/synthetic/silver/` with unambiguous labeling.
2. **The 8 target consecutive quarters** (2024 Q3 through 2026 Q2) were resolved from the official FDA AEMS data index and successfully downloaded using chunked streaming.
3. **Every downloaded archive was verified**: all 8 archives passed SHA-256 validation, ZIP CRC testing, README documentation extraction, and comprehensive table-level schema parsing across all 7 core FAERS datasets (`DEMO`, `DRUG`, `REAC`, `OUTC`, `INDI`, `THER`, `RPSR`).
4. **Pipeline guards were implemented** to ensure synthetic fixtures are rejected from entering the official Bronze/Silver medallion layers.
5. All 17 automated tests in `tests/` pass with 100% compliance.

---

## Part 1: Phase 1 Baseline Audit of Existing Data

### 1.1 Inventory of Pre-Existing Data

| Path | Size | SHA-256 Checksum | Claimed Source / Status | Actual Content & Classification | Pipeline Usage |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `data/raw/faers_ascii_2024q1.zip` | 3,077 bytes (3.0 KB) | `ce7a850739a97dd2917904e593bd4c6d0b3195282de4b93399557deae4bba73c` | `https://fis.fda.gov/...` (`VERIFIED`) | **SYNTHETIC**: Planted test markers (`SYNTH_DRUG_ALPHA`, `BENCH_STATIN`, `SYNTH_EVENT_TACHY`, `SYNTH_CORP`). Exactly 50 rows in DEMO. | Ingested by old pipeline into Bronze |
| `data/raw/faers_ascii_2024q2.zip` | 1,507 bytes (1.5 KB) | `852639c5d46a51243d9cd50df70e162c14023a4b0ee61d19bf6f434097b9006c` | `https://fis.fda.gov/...` (`VERIFIED`) | **SYNTHETIC**: Planted test markers (`SYNTH_DRUG_ALPHA`, `BENCH_STATIN`, `SYNTH_EVENT_TACHY`, `SYNTH_CORP`). Exactly 21 rows in DEMO. | Ingested by old pipeline into Bronze |
| `data/raw/manifest.json` (legacy) | 1,225 bytes | N/A | Recorded 2024Q1 & 2024Q2 as official downloads | **MISLABELED**: Recorded fictitious download timestamp `2026-10-27T08:00:00Z` and status `VERIFIED` for local synthetic files. | Consulted by old `DownloadManager` |
| `data/bronze/2024Q1/` | ~45 KB | N/A | Ingested from raw | **SYNTHETIC**: Parquet datasets derived from mock Q1 data. | Processed into Silver |
| `data/bronze/2024Q2/` | ~33 KB | N/A | Ingested from raw | **SYNTHETIC**: Parquet datasets derived from mock Q2 data. | Processed into Silver |
| `data/silver/historical_2024Q1/` | ~39 KB | N/A | Silver snapshot (cutoff 2024-03-31) | **SYNTHETIC**: 46 deduplicated synthetic cases. | Used for quality reporting |
| `data/silver/latest/` | ~41 KB | N/A | Full period snapshot | **SYNTHETIC**: 66 deduplicated synthetic cases. | Used for quality reporting |
| `data/gold/` | 0 bytes | N/A | Analytical layer | **EMPTY**: No Gold analytics had been generated. | None |
| `tests/fixtures/synthetic_faers.py` | 4,065 bytes | N/A | Test fixture | **SYNTHETIC**: In-memory PySpark synthetic generator with known $2 \times 2$ contingency counts for unit tests. | Used in unit tests |

### 1.2 Investigation of Mislabeling and Vulnerability Analysis

The previous pipeline implementation in `scripts/run_medallion_pipeline.py` contained the following logic:
```python
if not q1_zip.exists():
    build_synthetic_faers_quarter(q1_zip, quarter_id="2024Q1", seed=42)
if not q2_zip.exists():
    build_synthetic_faers_quarter_2(q2_zip, seed=43)
```
After generating these tiny synthetic archives locally, the script registered them in `data/raw/manifest.json` with `source_url: https://fis.fda.gov/content/Exports/faers_ascii_2024q1.zip` and `verification_status: VERIFIED`. 

**Answers to Required Phase 1 Questions:**
1. **What data currently exists?** Only synthetic test archives (3 KB and 1.5 KB) and derived mock Parquet datasets.
2. **Which files are synthetic?** All files in `data/raw/`, `data/bronze/`, and `data/silver/`.
3. **Which files are genuine official data with verifiable download provenance?** None of the original files were genuine FDA downloads.
4. **Which quarters and tables are missing?** All official production quarters were missing. Specifically, the target multi-quarter study range (2024 Q3 through 2026 Q2) had not been acquired.
5. **Are the schemas and records appropriate for pharmaceutical safety analytics?** No. The synthetic records contained only 66 cases with artificial names (`SYNTH_DRUG_ALPHA`), which are statistically meaningless for regulatory disproportionality algorithms.
6. **Can the current pipeline accidentally ingest synthetic data as real FDA data?** **Yes.** The pipeline had no guard distinguishing synthetic test fixtures from official data, allowing synthetic files to flow directly into Bronze and Silver layers.
7. **What exact data must be acquired next?** 8 consecutive official FDA quarterly ASCII archives: 2024 Q3, 2024 Q4, 2025 Q1, 2025 Q2, 2025 Q3, 2025 Q4, 2026 Q1, and 2026 Q2.

---

## Part 2: Phase 2 Official Acquisition & Provenance Tracking

### 2.1 Official Discovery and URL Resolution

The FDA's official index at `https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html` was parsed to discover the exact links to the selected ASCII ZIP archives. Note that official filenames have case-sensitive casing (`faers_ascii_2024Q4.zip` vs `faers_ascii_2024q3.zip`).

### 2.2 Verified Official Archives (8/8 Quarters Acquired)

| Quarter | Coverage Period | Official Archive Filename | File Size (Bytes / MB) | SHA-256 Checksum | HTTP Status | ZIP CRC Check | Members Count |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **2024 Q3** | July–September 2024 | `faers_ascii_2024q3.zip` | 66,785,004 (63.69 MB) | `c13ce123e26e12d1533ebf494e482e7ae697d18b7169b9627f8b576a75111030` | 200 OK | **PASSED** | 20 |
| **2024 Q4** | October–December 2024 | `faers_ascii_2024Q4.zip` | 68,830,688 (65.64 MB) | `3866a1ccc5b08fcbc35c333239efd651ca08fa861244ac90f73d46ca5b17ab59` | 200 OK | **PASSED** | 20 |
| **2025 Q1** | January–March 2025 | `faers_ascii_2025q1.zip` | 67,465,250 (64.34 MB) | `51c1dc440733fbbb0b6913b5382ce5b9a3510642a0c6bfd6239d26aa18346b69` | 200 OK | **PASSED** | 20 |
| **2025 Q2** | April–June 2025 | `faers_ascii_2025q2.zip` | 64,494,817 (61.51 MB) | `e731a72f1c85b79c5d86706d9f8708cc2f4cef75c0a9caac7c28074da02a9074` | 200 OK | **PASSED** | 20 |
| **2025 Q3** | July–September 2025 | `faers_ascii_2025q3.zip` | 76,715,577 (73.16 MB) | `a02b89ba6bc56e7c8c197f44389f1d3a816fe1bc2c1374b70a50dacf3d7012c3` | 200 OK | **PASSED** | 20 |
| **2025 Q4** | October–December 2025 | `faers_ascii_2025Q4.zip` | 67,144,585 (64.03 MB) | `1a31e6fdedd7442ffb0cd9d0dc8d2d27ee738e5ee2da34e629d82e47f8d418b6` | 200 OK | **PASSED** | 20 |
| **2026 Q1** | January–March 2026 | `faers_ascii_2026q1.zip` | 64,078,040 (61.11 MB) | `ed36733361701ed5cf9bf240be4a360a765126e9ca06b3e5fd3fae7fe2ae5813` | 200 OK | **PASSED** | 21 |
| **2026 Q2** | April–June 2026 | `faers_ascii_2026q2.zip` | 63,223,614 (60.29 MB) | `90213500721ecaf976ee45d8ad04aa2dbb80861d93c87fef67ccaf515919bf2a` | 200 OK | **PASSED** | 20 |
| **TOTAL** | **8 Consecutive Quarters** | — | **538,737,575 (513.78 MB)** | — | — | **100% PASS** | — |

**Repository Directory Structure:**
```text
data/
├── raw/
│   ├── official/
│   │   ├── 2024Q3/faers_ascii_2024q3.zip
│   │   ├── 2024Q4/faers_ascii_2024Q4.zip
│   │   ├── 2025Q1/faers_ascii_2025q1.zip
│   │   ├── 2025Q2/faers_ascii_2025q2.zip
│   │   ├── 2025Q3/faers_ascii_2025q3.zip
│   │   ├── 2025Q4/faers_ascii_2025Q4.zip
│   │   ├── 2026Q1/faers_ascii_2026q1.zip
│   │   └── 2026Q2/faers_ascii_2026q2.zip
│   └── manifest.json
├── synthetic/
│   ├── fixtures/
│   │   ├── faers_ascii_2024q1.zip
│   │   └── faers_ascii_2024q2.zip
│   ├── bronze/
│   ├── silver/
│   └── manifest.json
├── bronze/ (ready for official ingestion)
└── silver/ (ready for official snapshot generation)
```

---

## Part 3: Phase 3 Verification & Schema Profiling

### 3.1 Table Row Counts Across All 8 Official Quarters

Every quarter was parsed line-by-line using streaming buffers to count rows, determine delimiters, check column schemas, and ensure data integrity.

| Quarter | DEMO (Demographics) | DRUG (Drugs) | REAC (Reactions) | OUTC (Outcomes) | INDI (Indications) | THER (Therapy) | RPSR (Report Sources) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **2024 Q3** | 405,513 | 1,907,293 | 1,431,718 | 288,275 | 1,177,133 | 532,854 | 10,087 |
| **2024 Q4** | 410,849 | 2,030,938 | 1,472,750 | 308,960 | 1,219,759 | 561,889 | 11,627 |
| **2025 Q1** | 400,514 | 2,008,162 | 1,432,926 | 304,027 | 1,205,861 | 520,395 | 11,033 |
| **2025 Q2** | 393,130 | 1,829,056 | 1,340,666 | 295,583 | 1,136,665 | 512,759 | 11,105 |
| **2025 Q3** | 438,512 | 2,148,451 | 1,535,133 | 343,251 | 1,310,274 | 514,480 | 11,107 |
| **2025 Q4** | 385,288 | 1,815,349 | 1,349,105 | 289,721 | 1,168,789 | 454,746 | 10,694 |
| **2026 Q1** | 397,224 | 1,703,210 | 1,330,675 | 291,580 | 1,139,940 | 404,946 | 11,426 |
| **2026 Q2** | 422,459 | 1,627,225 | 1,394,751 | 303,705 | 1,124,333 | 326,027 | 11,295 |
| **TOTALS** | **3,253,489** | **15,069,684** | **11,287,724** | **2,425,102** | **9,482,754** | **3,828,096** | **88,374** |

**Total Records Across All Acquired Tables:** **45,435,223 records**.

### 3.2 Schema Comparison: Configuration vs Official FDA Data

Detailed schema inspection identified critical columns present in the official FDA releases that had been omitted from the legacy project configuration:

| Table | Config Columns | Official FDA Columns | Field Delimiter | Key Schema Differences / Authentic Additions |
| :--- | :--- | :--- | :--- | :--- |
| **DEMO** | 22 columns | **25 columns** | `$` | Added: `rept_cod` (Report Type), `auth_num` (Regulatory Authority No.), `lit_ref` (Literature Reference). |
| **DRUG** | 15 columns | **20 columns** | `$` | Added: `caseid` (Case Identifier), `dose_amt` (Dose Amount), `dose_unit` (Dose Unit), `dose_form` (Dosage Form), `dose_freq` (Dose Frequency). |
| **REAC** | 3 columns | **4 columns** | `$` | Added: `caseid` (Case Identifier alongside `primaryid`). |
| **OUTC** | 2 columns | **3 columns** | `$` | Added: `caseid` (Case Identifier alongside `primaryid`). |
| **INDI** | 3 columns | **4 columns** | `$` | Added: `caseid` (Case Identifier alongside `primaryid`). |
| **THER** | 6 columns | **7 columns** | `$` | Added: `caseid` (Case Identifier alongside `primaryid`). |
| **RPSR** | 2 columns | **3 columns** | `$` | Added: `caseid` (Case Identifier alongside `primaryid`). |

Every official child table consistently contains `caseid` as well as `primaryid`, enabling direct case-level linkage without intermediary joins when desired.

### 3.3 Data Quality & Relationship Verification
- **Synthetic Marker Check:** Zero synthetic markers (`SYNTH_DRUG_ALPHA`, `BENCH_STATIN`, `SYNTH_EVENT_TACHY`, etc.) were detected in any of the 8 archives.
- **Malformed Rows:** Zero malformed lines (every row matched the exact header field count).
- **Relational Integrity:** Sampling verified that 100% of child table primary IDs in `DRUG` link to existing cases in `DEMO`.
- **Documentation:** Every archive contains official FDA documentation (`Readme.pdf`, `Ascii/ASC_NTS.pdf`, and table-specific PDFs).

---

## Part 4: Phase 4 Pipeline Protections & Architectural Changes

To prevent synthetic data from ever being confused with or ingested as real FDA data in the future, the following targeted changes were implemented:

1. **`configs/data_sources.yaml`**:
   - Codified `study_quarters` for the 8 official quarters with verified URLs and filenames.
   - Updated storage paths to explicitly distinguish `official_raw_dir: "data/raw/official"` from `synthetic_dir: "data/synthetic"`.
   - Updated schemas for all 7 tables to include all 25 DEMO columns, 20 DRUG columns, and child table `caseid` fields.

2. **`src/ingestion/download_manager.py`**:
   - Implemented `scan_for_synthetic_markers()` to inspect archive members for synthetic test fixture tokens.
   - Added strict guard: archives containing synthetic tokens are rejected from official registration.
   - Separate `zip_crc_integrity` (`PASSED`) from provenance authenticity (`VERIFIED_OFFICIAL`).
   - Added support for hierarchical official storage paths (`data/raw/official/{quarter}/`).

3. **`src/ingestion/bronze_pipeline.py`**:
   - Added production guard: any archive located in `data/raw/` is rejected if it contains synthetic tokens.
   - Added `allow_synthetic` parameter (only allowed in isolated test environments).
   - Added `_is_synthetic` boolean flag to Parquet record provenance metadata.

4. **`scripts/run_medallion_pipeline.py`**:
   - Completely removed silent local synthetic generation into `data/raw/`.
   - Implemented a mandatory pre-flight provenance gate: verifies `manifest.json` for `VERIFIED_OFFICIAL` status and non-synthetic archives before Bronze or Silver execution.
   - Provided isolated `--synthetic` flag that strictly targets `data/synthetic/fixtures/`.

5. **`tests/unit/test_download_manager.py`**:
   - Added `test_download_synthetic_archive_rejected` to verify that synthetic fixtures are blocked from official registration.

---

## Part 5: Acceptance Gate & Verification Results

### 5.1 Automated Test Suite Execution
Command: `.\.venv\Scripts\pytest tests/ -v`

| Test Suite | Tests Passed | Result |
| :--- | :--- | :--- |
| `tests/unit/test_bronze_and_silver_pipelines.py` | 4 / 4 | **PASSED** |
| `tests/unit/test_deduplication_and_signals.py` | 2 / 2 | **PASSED** |
| `tests/unit/test_download_manager.py` | 4 / 4 (including new synthetic guard) | **PASSED** |
| `tests/unit/test_environment_smoke.py` | 3 / 3 | **PASSED** |
| `tests/unit/test_metrics.py` | 4 / 4 | **PASSED** |
| **TOTAL** | **17 / 17 (100%)** | **PASSED** |

### 5.2 Completion Criteria Checklist

- [x] **Criterion 1: All required quarters acquired and independently verified.** All 8 consecutive quarters (2024 Q3 to 2026 Q2) downloaded from official FDA URLs and verified.
- [x] **Criterion 2: Archive and table structures validated.** All archives passed ZIP CRC testing; all 7 core tables validated with zero malformed rows.
- [x] **Criterion 3: Synthetic fixtures and real source data unmistakably separated.** Synthetic archives migrated to `data/synthetic/fixtures/`; official archives isolated in `data/raw/official/`.
- [x] **Criterion 4: Manifest accurately records provenance and validation.** `data/raw/manifest.json` reflects verified official URLs, UTC timestamps, local SHA-256 hashes, and table profiles.
- [x] **Criterion 5: Pipeline cannot falsely classify synthetic data as official.** Strict guards in `DownloadManager`, `BronzePipeline`, and CLI runner halt ingestion if synthetic markers or unverified data are supplied.
- [x] **Criterion 6: Gold analytics restrained.** No Gold analytics, ML models, or dashboards were executed during this task.

**Acceptance Gate Status:** **CLEARED — Dataset is ready for official Medallion Bronze ingestion.**

