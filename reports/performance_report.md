# PharmaSafe Performance Report
Generated: 2026-10-30T16:35:35.126732+00:00  |  Snapshot `latest`

Machine: macOS-26.5.1-arm64-arm-64bit, 12 logical CPUs, Python 3.12.15. Spark: local[*] on a single machine (not a multi-node cluster).

## 1. Pipeline stage timings (this machine, measured)

| Stage | Input rows | Output rows | Seconds |
| :--- | ---: | ---: | ---: |
| Bronze 2024Q3 (7 tables + deleted list) | 5,757,842 | 5,757,842 | 8.04 |
| Bronze 2024Q4 (7 tables + deleted list) | 6,021,433 | 6,021,433 | 5.6 |
| Bronze 2025Q1 (7 tables + deleted list) | 5,888,650 | 5,888,650 | 5.74 |
| Bronze 2025Q2 (7 tables + deleted list) | 5,525,335 | 5,525,335 | 5.28 |
| Bronze 2025Q3 (7 tables + deleted list) | 6,306,155 | 6,306,155 | 5.56 |
| Bronze 2025Q4 (7 tables + deleted list) | 5,478,189 | 5,478,189 | 5.08 |
| Bronze 2026Q1 (7 tables + deleted list) | 5,285,136 | 5,285,136 | 5.02 |
| Bronze 2026Q2 (7 tables + deleted list) | 5,214,012 | 5,214,012 | 4.95 |
| Silver (all quarters, 7 tables) | 45,435,223 | 45,161,755 | 232.57 |
| Gold marts + signals (latest) | 17,940,147 | 2,452,064 | 89.47 |
| Gold marts + signals (asof_2025Q2) | 9,508,496 | 1,663,088 | 50.14 |
| Temporal analytics (latest) | None | 612175 | 14.49 |
| Drug-combination analytics (latest) | None | 3862333 | 66.45 |
| Validation (references, robustness, temporal) | None | None | 57.83 |

## 2. Pandas vs PySpark: drug-event 2x2 counts (primary-suspect policy)

Same inputs, same logic, each engine in a fresh process, wall time includes reading Parquet and (for Spark) session start-up.

| Input | Rows | Parquet bytes |
| :--- | ---: | ---: |
| case_drug | 8,959,171 | 77,143,729 |
| case_reaction | 8,980,976 | 47,652,354 |

| Engine | Median s (3 runs) | All runs (s) | Peak memory, process tree incl. JVM (MB, sampled every 0.1 s) |
| :--- | ---: | :--- | ---: |
| pandas | 11.44 | [11.4, 11.44, 11.48] | 3583.2 |
| spark | 22.49 | [23.4, 21.49, 22.49] | 4899.5 |

Both engines produced identical results: **True** (`{'eligible_cases': 2857989, 'pairs_all': 1058302, 'pairs_a_ge_3': 376833, 'sum_a': 8870845}`).

Interpretation: on one machine and data that fits in memory, a single-process engine can be competitive or faster; Spark's value here is that the same code scales past memory and across cores or nodes. No speed-up claim is made beyond these measurements.
