# PharmaSafe Automated Data-Quality & Pipeline Audit Report
**Audit Generated:** 2026-10-27 14:44:58 UTC  
**Evaluation Snapshot:** `latest`  
**Pipeline Status:** PASSED QUALITY AUDIT  

---
## 1. Executive Summary
This report documents the automated quality verification, schema conformance, version deduplication, and orphan handling performed by the PharmaSafe Medallion Pipeline (Bronze &rarr; Silver). All transformations are deterministic, auditable, and non-destructive to raw source data.

---
## 2. Ingested Quarters & Provenance Tracking
| Quarter | Filename | Size (Bytes) | SHA-256 Checksum | Verification Status |
| :--- | :--- | :--- | :--- | :--- |
| 2024Q1 | `faers_ascii_2024q1.zip` | 3,077 | `ce7a850739a97dd2...` | **VERIFIED** |
| 2024Q2 | `faers_ascii_2024q2.zip` | 1,507 | `852639c5d46a5124...` | **VERIFIED** |

---
## 3. Case Deduplication & Version Resolution (DEMO)
The analytical unit is the **Unique Eligible Case** (`caseid`). Raw reports (`primaryid`) containing follow-up updates or duplicate submissions are resolved deterministically.

| Metric | Count | Rate / Proportion | Definition / Formula |
| :--- | :--- | :--- | :--- |
| **Raw DEMO Rows Ingested** | 70 | 100.0% | Denominator for pre-cutoff ingestion |
| **Post-Cutoff DEMO Rows** | 70 | - | Reports on or before snapshot cutoff date |
| **Exact Duplicate Rows Removed** | 1 | - | Identical across all non-metadata fields |
| **Superseded Versions Excluded** | 3 | - | Subsumed by higher `caseversion` or newer `fda_dt` |
| **Canonical Cases Retained** | **66** | - | Unique winning case representations |
| **Case Version Reduction Rate** | - | **5.71%** | `100 * (Superseded + Dups) / PostCutoffRows` |

> **Deduplication Hierarchy Rule:**  
> `Window.partitionBy('caseid').orderBy(caseversion DESC, fda_dt DESC, primaryid DESC)`  
> Excluded records are quarantined in `data/silver/quarantine/` for auditability.

---
## 4. Relational Integrity & Child Table Quarantine
Child tables (`DRUG`, `REAC`, `OUTC`) must join to the canonical `primaryid`. Orphan records and superseded child entries are isolated into quarantine Parquet files.

| Table | Raw Rows | Retained Rows | Quarantined Orphans | Retention Rate | Quarantine Rate |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **DRUG** | 70 | 66 | 4 | 94.29% | 5.71% |
| **REAC** | 70 | 66 | 4 | 94.29% | 5.71% |
| **OUTC** | 68 | 66 | 2 | 97.06% | 2.94% |

---
## 5. Field Normalization & Data Hygiene Summary
- **Drug Names:** Verbatim reported names (`drugname`) are preserved alongside normalized active ingredients (`clean_drug_name`).
- **Adverse Reactions:** MedDRA Preferred Terms (`pt`) are trimmed and uppercased (`clean_pt`).
- **Date Fields:** Unparseable or invalid dates (`99999999`, unformatted text) are converted to `NULL` (no invented values).
- **Malformed Rows:** Lines with delimiter count mismatches are rejected at the Bronze gate and stored in `quarantine/` JSON.

---
## 6. Audit Verdict
> **ACCEPTANCE CRITERIA STATUS: PASSED**  
> The Medallion pipeline successfully enforces relational integrity, removes duplicates deterministically, > preserves raw source files, isolates non-canonical records, and provides verifiable audit metrics.
