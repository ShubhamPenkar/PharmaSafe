# PharmaSafe Safety Signal Validation Report
Generated: 2026-10-30T16:32:53.072198+00:00  |  Snapshot: `latest`

> Screening criteria are project choices, not clinical standards. A reference pair that is not recovered does not show the method failed: the reporting window, drug-name mapping and case counts all matter. Reference pairs were not tuned against this output.

## 1. Reference-signal evaluation (primary-suspect policy)

| Drug | Reaction | Expected | Status | Outcome | Cases | ROR (95% CI) | PRR |
| :--- | :--- | :---: | :--- | :--- | ---: | :--- | ---: |
| ROFECOXIB | MYOCARDIAL INFARCTION | signal | INSUFFICIENT_CASES | NOT_EVALUABLE | — | — | — |
| ROSIGLITAZONE | MYOCARDIAL INFARCTION | signal | INSUFFICIENT_CASES | NOT_EVALUABLE | — | — | — |
| ATORVASTATIN | RHABDOMYOLYSIS | signal | DETECTED | RECOVERED | 703 | 51.70 (47.61–56.13) | 47.95 |
| CIPROFLOXACIN | TENDON RUPTURE | signal | DETECTED | RECOVERED | 75 | 48.03 (37.94–60.80) | 47.15 |
| ISOTRETINOIN | CONGENITAL ANOMALY | signal | INSUFFICIENT_CASES | NOT_EVALUABLE | — | — | — |
| ISOTRETINOIN | FOETAL MALFORMATION | signal | INSUFFICIENT_CASES | NOT_EVALUABLE | — | — | — |
| PEMBROLIZUMAB | MYOCARDITIS | signal | DETECTED | RECOVERED | 284 | 17.01 (15.03–19.25) | 16.79 |
| PEMBROLIZUMAB | IMMUNE-MEDIATED MYOCARDITIS | signal | DETECTED | RECOVERED | 279 | 79.26 (68.42–91.81) | 78.18 |
| CLOZAPINE | AGRANULOCYTOSIS | signal | DETECTED | RECOVERED | 158 | 10.57 (9.00–12.42) | 10.47 |
| NATALIZUMAB | PROGRESSIVE MULTIFOCAL LEUKOENCEPHALOPATHY | signal | DETECTED | RECOVERED | 113 | 53.27 (43.77–64.82) | 52.32 |
| AMIODARONE | PULMONARY TOXICITY | signal | DETECTED | RECOVERED | 62 | 56.46 (43.63–73.06) | 54.99 |
| AMOXICILLIN | CONGENITAL ANOMALY | negative control | INSUFFICIENT_CASES | NOT_EVALUABLE | — | — | — |
| AMOXICILLIN | MYOCARDIAL INFARCTION | negative control | PRESENT_NOT_FLAGGED | CORRECTLY_NOT_FLAGGED | 4 | 0.21 (0.08–0.56) | 0.21 |
| PEMBROLIZUMAB | TENDON RUPTURE | negative control | INSUFFICIENT_CASES | NOT_EVALUABLE | — | — | — |

Outcome counts: CORRECTLY_NOT_FLAGGED = 1, NOT_EVALUABLE = 6, RECOVERED = 7

## 2. Robustness to screening parameters

Baseline `primary|min_cases=3|min_prr=2.0`: 173,222 flagged pairs.

| Role policy | Min cases | Min PRR | Flagged pairs | Jaccard vs baseline | Baseline retained (%) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| primary | 3 | 1.5 | 181,003 | 0.957 | 100.0 |
| primary | 3 | 2.0 | 173,222 | 1.0 | 100.0 |
| primary | 3 | 3.0 | 152,500 | 0.8804 | 88.04 |
| primary | 5 | 1.5 | 128,866 | 0.669 | 69.9 |
| primary | 5 | 2.0 | 121,085 | 0.699 | 69.9 |
| primary | 5 | 3.0 | 100,363 | 0.5794 | 57.94 |
| primary | 10 | 1.5 | 77,810 | 0.3869 | 40.43 |
| primary | 10 | 2.0 | 70,029 | 0.4043 | 40.43 |
| primary | 10 | 3.0 | 54,989 | 0.3174 | 31.74 |
| all_suspect | 3 | 1.5 | 435,431 | 0.372 | 95.28 |
| all_suspect | 3 | 2.0 | 422,572 | 0.3772 | 94.2 |
| all_suspect | 3 | 3.0 | 386,387 | 0.3581 | 85.19 |
| all_suspect | 5 | 1.5 | 341,676 | 0.3661 | 79.66 |
| all_suspect | 5 | 2.0 | 328,817 | 0.3721 | 78.59 |
| all_suspect | 5 | 3.0 | 292,632 | 0.349 | 69.58 |
| all_suspect | 10 | 1.5 | 228,537 | 0.3213 | 56.4 |
| all_suspect | 10 | 2.0 | 215,678 | 0.327 | 55.32 |
| all_suspect | 10 | 3.0 | 188,480 | 0.3006 | 48.27 |
| all_reported | 3 | 1.5 | 790,717 | 0.2006 | 92.97 |
| all_reported | 3 | 2.0 | 748,344 | 0.2053 | 90.61 |
| all_reported | 3 | 3.0 | 642,816 | 0.2019 | 79.15 |
| all_reported | 5 | 1.5 | 598,894 | 0.2226 | 81.15 |
| all_reported | 5 | 2.0 | 556,521 | 0.23 | 78.79 |
| all_reported | 5 | 3.0 | 450,993 | 0.2298 | 67.33 |
| all_reported | 10 | 1.5 | 393,773 | 0.2315 | 61.53 |
| all_reported | 10 | 2.0 | 351,400 | 0.2428 | 59.17 |
| all_reported | 10 | 3.0 | 269,612 | 0.2402 | 49.51 |

## 3. Temporal validation

Discovery: 2024Q3, 2024Q4, 2025Q1, 2025Q2. Evaluation: 2025Q3, 2025Q4, 2026Q1, 2026Q2. Cases belong to a single quarter, so no case appears on both sides.

- Pairs flagged in discovery: **91,821**
- Of those, with enough cases to be evaluated later: **56,706**
- Re-flagged in the evaluation quarters: **49,497** (87.29% of evaluable)
- Newly flagged only in evaluation: 57,499
