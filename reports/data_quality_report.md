# PharmaSafe Data Quality & Pipeline Audit
Generated: 2026-10-30T16:33:50.905556+00:00  |  Gold snapshot: `latest`

**Verdict:** ALL CHECKS PASSED (24/24)

## 1. Source archives

| Quarter | File | Size (MB) | SHA-256 (prefix) | Status |
| :--- | :--- | ---: | :--- | :--- |
| 2024Q3 | `faers_ascii_2024q3.zip` | 63.69 | `c13ce123e26e12d1…` | VERIFIED_OFFICIAL |
| 2024Q4 | `faers_ascii_2024Q4.zip` | 65.64 | `3866a1ccc5b08fcb…` | VERIFIED_OFFICIAL |
| 2025Q1 | `faers_ascii_2025q1.zip` | 64.34 | `51c1dc440733fbbb…` | VERIFIED_OFFICIAL |
| 2025Q2 | `faers_ascii_2025q2.zip` | 61.51 | `e731a72f1c85b79c…` | VERIFIED_OFFICIAL |
| 2025Q3 | `faers_ascii_2025q3.zip` | 73.16 | `a02b89ba6bc56e7c…` | VERIFIED_OFFICIAL |
| 2025Q4 | `faers_ascii_2025Q4.zip` | 64.03 | `1a31e6fdedd7442f…` | VERIFIED_OFFICIAL |
| 2026Q1 | `faers_ascii_2026q1.zip` | 61.11 | `ed36733361701ed5…` | VERIFIED_OFFICIAL |
| 2026Q2 | `faers_ascii_2026q2.zip` | 60.29 | `90213500721ecaf9…` | VERIFIED_OFFICIAL |

## 2. Bronze ingestion (rows are table records, not cases)

| Quarter | Table | Source records | Ingested | Quarantined | Reconciled |
| :--- | :--- | ---: | ---: | ---: | :---: |
| 2024Q3 | DEMO | 405,513 | 405,513 | 0 | yes |
| 2024Q3 | DRUG | 1,907,293 | 1,907,293 | 0 | yes |
| 2024Q3 | REAC | 1,431,718 | 1,431,718 | 0 | yes |
| 2024Q3 | OUTC | 288,275 | 288,275 | 0 | yes |
| 2024Q3 | INDI | 1,177,133 | 1,177,133 | 0 | yes |
| 2024Q3 | THER | 532,854 | 532,854 | 0 | yes |
| 2024Q3 | RPSR | 10,087 | 10,087 | 0 | yes |
| 2024Q3 | DELETED | 4,969 | 4,969 | 0 | yes |
| 2024Q4 | DEMO | 410,849 | 410,849 | 0 | yes |
| 2024Q4 | DRUG | 2,030,938 | 2,030,938 | 0 | yes |
| 2024Q4 | REAC | 1,472,750 | 1,472,750 | 0 | yes |
| 2024Q4 | OUTC | 308,960 | 308,960 | 0 | yes |
| 2024Q4 | INDI | 1,219,759 | 1,219,759 | 0 | yes |
| 2024Q4 | THER | 561,889 | 561,889 | 0 | yes |
| 2024Q4 | RPSR | 11,627 | 11,627 | 0 | yes |
| 2024Q4 | DELETED | 4,661 | 4,661 | 0 | yes |
| 2025Q1 | DEMO | 400,514 | 400,514 | 0 | yes |
| 2025Q1 | DRUG | 2,008,162 | 2,008,162 | 0 | yes |
| 2025Q1 | REAC | 1,432,926 | 1,432,926 | 0 | yes |
| 2025Q1 | OUTC | 304,027 | 304,027 | 0 | yes |
| 2025Q1 | INDI | 1,205,861 | 1,205,861 | 0 | yes |
| 2025Q1 | THER | 520,395 | 520,395 | 0 | yes |
| 2025Q1 | RPSR | 11,033 | 11,033 | 0 | yes |
| 2025Q1 | DELETED | 5,732 | 5,732 | 0 | yes |
| 2025Q2 | DEMO | 393,130 | 393,130 | 0 | yes |
| 2025Q2 | DRUG | 1,829,056 | 1,829,056 | 0 | yes |
| 2025Q2 | REAC | 1,340,666 | 1,340,666 | 0 | yes |
| 2025Q2 | OUTC | 295,583 | 295,583 | 0 | yes |
| 2025Q2 | INDI | 1,136,665 | 1,136,665 | 0 | yes |
| 2025Q2 | THER | 512,759 | 512,759 | 0 | yes |
| 2025Q2 | RPSR | 11,105 | 11,105 | 0 | yes |
| 2025Q2 | DELETED | 6,371 | 6,371 | 0 | yes |
| 2025Q3 | DEMO | 438,512 | 438,512 | 0 | yes |
| 2025Q3 | DRUG | 2,148,451 | 2,148,451 | 0 | yes |
| 2025Q3 | REAC | 1,535,133 | 1,535,133 | 0 | yes |
| 2025Q3 | OUTC | 343,251 | 343,251 | 0 | yes |
| 2025Q3 | INDI | 1,310,274 | 1,310,274 | 0 | yes |
| 2025Q3 | THER | 514,480 | 514,480 | 0 | yes |
| 2025Q3 | RPSR | 11,107 | 11,107 | 0 | yes |
| 2025Q3 | DELETED | 4,947 | 4,947 | 0 | yes |
| 2025Q4 | DEMO | 385,288 | 385,288 | 0 | yes |
| 2025Q4 | DRUG | 1,815,349 | 1,815,349 | 0 | yes |
| 2025Q4 | REAC | 1,349,105 | 1,349,105 | 0 | yes |
| 2025Q4 | OUTC | 289,721 | 289,721 | 0 | yes |
| 2025Q4 | INDI | 1,168,789 | 1,168,789 | 0 | yes |
| 2025Q4 | THER | 454,746 | 454,746 | 0 | yes |
| 2025Q4 | RPSR | 10,694 | 10,694 | 0 | yes |
| 2025Q4 | DELETED | 4,497 | 4,497 | 0 | yes |
| 2026Q1 | DEMO | 397,224 | 397,224 | 0 | yes |
| 2026Q1 | DRUG | 1,703,210 | 1,703,210 | 0 | yes |
| 2026Q1 | REAC | 1,330,675 | 1,330,675 | 0 | yes |
| 2026Q1 | OUTC | 291,580 | 291,580 | 0 | yes |
| 2026Q1 | INDI | 1,139,940 | 1,139,940 | 0 | yes |
| 2026Q1 | THER | 404,946 | 404,946 | 0 | yes |
| 2026Q1 | RPSR | 11,426 | 11,426 | 0 | yes |
| 2026Q1 | DELETED | 6,135 | 6,135 | 0 | yes |
| 2026Q2 | DEMO | 422,459 | 422,459 | 0 | yes |
| 2026Q2 | DRUG | 1,627,225 | 1,627,225 | 0 | yes |
| 2026Q2 | REAC | 1,394,751 | 1,394,751 | 0 | yes |
| 2026Q2 | OUTC | 303,705 | 303,705 | 0 | yes |
| 2026Q2 | INDI | 1,124,333 | 1,124,333 | 0 | yes |
| 2026Q2 | THER | 326,027 | 326,027 | 0 | yes |
| 2026Q2 | RPSR | 11,295 | 11,295 | 0 | yes |
| 2026Q2 | DELETED | 4,217 | 4,217 | 0 | yes |

## 3. Case version handling (DEMO)

| Measure | Count |
| :--- | ---: |
| Raw DEMO report rows | 3,253,489 |
| Excluded rows | 131 |
| &nbsp;&nbsp;EXACT_DUPLICATE | 131 |
| Report versions retained | 3,253,358 |
| Superseded versions | 382,373 |
| Versions never canonical at any quarter end | 2 |
| Versions with missing/invalid caseversion | 0 |
| Cases removed by FDA deletion lists | 12,912 |
| **Canonical cases (latest view)** | **2,858,073** |

Ranking rule: caseversion DESC, fda_dt DESC, primaryid DESC (NULLs last).

## 4. Relational integrity of child tables

| Table | Raw rows | Retained | Excluded | Retention | Exclusion reasons |
| :--- | ---: | ---: | ---: | ---: | :--- |
| DRUG | 15,069,684 | 15,065,170 | 4,514 | 99.97% | EXACT_DUPLICATE: 3,157; PARENT_FROM_OTHER_QUARTER: 1,357 |
| REAC | 11,287,724 | 11,138,241 | 149,483 | 98.68% | EXACT_DUPLICATE: 148,943; PARENT_FROM_OTHER_QUARTER: 540 |
| OUTC | 2,425,102 | 2,424,978 | 124 | 99.99% | PARENT_FROM_OTHER_QUARTER: 124 |
| INDI | 9,482,754 | 9,463,965 | 18,789 | 99.8% | EXACT_DUPLICATE: 18,216; PARENT_FROM_OTHER_QUARTER: 573 |
| THER | 3,828,096 | 3,727,726 | 100,370 | 97.38% | EXACT_DUPLICATE: 100,175; PARENT_FROM_OTHER_QUARTER: 195 |
| RPSR | 88,374 | 88,317 | 57 | 99.94% | PARENT_FROM_OTHER_QUARTER: 57 |

Superseded case versions are *not* orphans: their child rows stay in Silver and are excluded from Gold by version.

## 5. Missing values in key fields (% of retained rows)

| Table | Field | Missing % |
| :--- | :--- | ---: |
| DEMO | age | 39.25 |
| DEMO | sex | 19.25 |
| DEMO | event_dt | 56.53 |
| DEMO | fda_dt | 0.0 |
| DEMO | occr_country | 6.35 |
| DEMO | caseversion | 0.0 |
| DRUG | drugname | 0.0 |
| DRUG | prod_ai | 1.76 |
| DRUG | role_cod | 0.0 |
| DRUG | route | 38.54 |
| REAC | pt | 0.0 |
| OUTC | outc_cod | 0.0 |

## Gold population

- Canonical cases: **2,858,073**; drug ingredients/names: 91,607; reaction terms: 18,815; report quarters: 2024Q3, 2024Q4, 2025Q1, 2025Q2, 2025Q3, 2025Q4, 2026Q1, 2026Q2
- Policy `primary` (roles PS): N = 2,857,989, pairs stored 376,833, potential signals 173,222
- Policy `all_suspect` (roles PS, SS): N = 2,857,989, pairs stored 704,444, potential signals 422,572
- Policy `all_reported` (roles PS, SS, C, I): N = 2,857,992, pairs stored 1,370,787, potential signals 748,344

## 6. Verification checks

| Check | Result | Detail |
| :--- | :---: | :--- |
| Bronze 2024Q3: source records == ingested + quarantined | pass | all tables reconcile |
| Bronze 2024Q3: official provenance | pass | VERIFIED_OFFICIAL |
| Bronze 2024Q4: source records == ingested + quarantined | pass | all tables reconcile |
| Bronze 2024Q4: official provenance | pass | VERIFIED_OFFICIAL |
| Bronze 2025Q1: source records == ingested + quarantined | pass | all tables reconcile |
| Bronze 2025Q1: official provenance | pass | VERIFIED_OFFICIAL |
| Bronze 2025Q2: source records == ingested + quarantined | pass | all tables reconcile |
| Bronze 2025Q2: official provenance | pass | VERIFIED_OFFICIAL |
| Bronze 2025Q3: source records == ingested + quarantined | pass | all tables reconcile |
| Bronze 2025Q3: official provenance | pass | VERIFIED_OFFICIAL |
| Bronze 2025Q4: source records == ingested + quarantined | pass | all tables reconcile |
| Bronze 2025Q4: official provenance | pass | VERIFIED_OFFICIAL |
| Bronze 2026Q1: source records == ingested + quarantined | pass | all tables reconcile |
| Bronze 2026Q1: official provenance | pass | VERIFIED_OFFICIAL |
| Bronze 2026Q2: source records == ingested + quarantined | pass | all tables reconcile |
| Bronze 2026Q2: official provenance | pass | VERIFIED_OFFICIAL |
| Silver DEMO: raw == retained + excluded | pass | raw 3,253,489 |
| Silver DRUG: raw == retained + excluded | pass | raw 15,069,684 |
| Silver REAC: raw == retained + excluded | pass | raw 11,287,724 |
| Silver OUTC: raw == retained + excluded | pass | raw 2,425,102 |
| Silver INDI: raw == retained + excluded | pass | raw 9,482,754 |
| Silver THER: raw == retained + excluded | pass | raw 3,828,096 |
| Silver RPSR: raw == retained + excluded | pass | raw 88,374 |
| Gold canonical cases == Silver latest canonical cases (latest snapshot) | pass | gold 2,858,073 vs silver 2,858,073 |
