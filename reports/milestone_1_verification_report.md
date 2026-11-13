# PharmaSafe Milestone 1: Scope & Dataset Verification Report

**Date:** 2026-10-26  
**Status:** COMPLETE & VERIFIED  
**Gate:** Milestone 1 &rarr; Milestone 2 Gate Cleared

---

## 1. Executive Summary
Milestone 1 establishes the scientific, regulatory, and engineering foundation for **PharmaSafe**—a scalable pharmaceutical safety analytics and pharmacovigilance platform using Apache Spark, Parquet, and Streamlit. All initial specifications, data schemas, case deduplication rules, disproportionality metrics, and development environment components have been codified and verified via automated test suites.

---

## 2. Completed Deliverables

| Deliverable | Location | Verification Status |
| :--- | :--- | :--- |
| **Data Dictionary & Schema Specification** | `docs/data_dictionary_and_schemas.md` | Complete documentation for all 7 FAERS tables (DEMO, DRUG, REAC, OUTC, INDI, THER, RPSR) |
| **Methodology & Counting Rules** | `docs/methodology_and_counting_rules.md` | Documented deterministic deduplication, orphan policy, 2x2 matrix formulas, ROR/PRR, 95% CIs, and epidemiological caveats |
| **Data Sources Configuration** | `configs/data_sources.yaml` | Codified FAERS ASCII format (`$`), pilot quarters (2024Q1, 2024Q2), table column schemas |
| **Analysis Configuration** | `configs/analysis.yaml` | Configured screening criteria ($a \ge 3$, $\text{ROR}_{025} > 1.0$, $\text{PRR} \ge 2.0$), primary suspect policy (`PS`), and sensitivity settings |
| **Reference Signals Benchmark** | `configs/reference_signals.yaml` | Curated regulatory reference set (Rofecoxib, Rosiglitazone, Atorvastatin, Ciprofloxacin, Clozapine, Pembrolizumab) and negative controls |
| **Repository Structure** | `PharmaSafe/` | Clean Medallion architecture layout (`data/raw`, `bronze`, `silver`, `gold`, `src/`, `tests/`, `dashboard/`) |
| **Development Environment** | `PharmaSafe/.venv` | Python 3.12 + PySpark 4.2.0 + Pandas 2.3.3 + PyArrow 26.0.0 + SciPy 1.18.1 + Streamlit 1.66.1 |
| **Automated Test Suite** | `tests/unit/` | 9 automated unit tests verifying smoke lifecycle, exact mathematical formulas, zero-cell corrections, deduplication, and signal detection |

---

## 3. Key Methodological Decisions & Rules

### 3.1 Unit of Analysis
The unit of analysis is the **Unique Eligible Case**. Joins across tables are deduplicated to prevent Cartesian row inflation.

### 3.2 Deterministic Case Deduplication
To address repeat follow-up submissions across quarters:
1. Partition `DEMO` by `caseid`.
2. Rank by `caseversion DESC`, `fda_dt DESC`, `primaryid DESC`.
3. Retain the top-ranked canonical record (`primaryid`).
4. Prune child records in `DRUG`, `REAC`, `OUTC` that do not link to the canonical `primaryid`.

### 3.3 Statistical Formulas & Zero-Cell Correction
- **Reporting Odds Ratio (ROR):** $\frac{a \cdot d}{b \cdot c}$ with Wald 95% CI.
- **Proportional Reporting Ratio (PRR):** $\frac{a / (a+b)}{c / (c+d)}$ with log-transformed 95% CI and Yates' $\chi^2$.
- **Haldane-Anscombe Correction:** Adds $+0.5$ to all contingency cells when any zero count occurs, ensuring finite, robust estimates without mathematical failure.

---

## 4. Test Suite Execution Summary
Execution command: `.\.venv\Scripts\pytest tests/ -v`
- `tests/unit/test_deduplication_and_signals.py::test_case_deduplication_and_orphan_removal` **PASSED**
- `tests/unit/test_deduplication_and_signals.py::test_contingency_matrix_and_signal_generation` **PASSED**
- `tests/unit/test_environment_smoke.py::test_core_dependencies_import` **PASSED**
- `tests/unit/test_environment_smoke.py::test_configurations_load` **PASSED**
- `tests/unit/test_environment_smoke.py::test_spark_session_lifecycle` **PASSED**
- `tests/unit/test_metrics.py::test_contingency_table_exact_math` **PASSED**
- `tests/unit/test_metrics.py::test_zero_cell_haldane_anscombe_correction` **PASSED**
- `tests/unit/test_metrics.py::test_negative_counts_raise_value_error` **PASSED**
- `tests/unit/test_metrics.py::test_evaluate_screening_status` **PASSED**

**Overall Result: 9/9 Tests Passing (100% Success Rate).**
