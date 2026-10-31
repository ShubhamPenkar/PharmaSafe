# FDA FAERS Data Dictionary & Schema Specification

## 1. Overview
The FDA Adverse Event Reporting System (FAERS) is a database designed to support the FDA's post-marketing safety surveillance program for drug and therapeutic biological products. FAERS data files are distributed quarterly in ASCII (pipe/dollar-delimited) and XML formats.

PharmaSafe ingests the ASCII distribution where records are delimited by the dollar sign character (`$`).

---

## 2. Relational Hierarchy and Linkages

```
             +----------------------------+
             |         DEMO table         |
             | primaryid (PK), caseid     |
             | caseversion, fda_dt, ...   |
             +-------------+--------------+
                           |
             +-------------+-------------+-------------+
             |                           |             |
+------------v------------+ +------------v----+ +------v-------------------+
|       DRUG table        | |   REAC table    | |       OUTC table          |
| primaryid (FK), drug_seq| | primaryid (FK)  | | primaryid (FK), outc_cod  |
| role_cod (PS, SS, C, I) | | pt (MedDRA term)| | (DE, LT, HO, DS, CA, etc)|
+------------+------------+ +-----------------+ +-------------------------+
             |
     +-------+-------+
     |               |
+----v-------+ +-----v------+
| INDI table | | THER table |
+------------+ +------------+
```

Each submission has a unique `primaryid`.
Each clinical case has a unique `caseid` which persists across updates, while `caseversion` tracks sequential revisions (1, 2, 3...).

---

## 3. Detailed Table Schemas

### 3.1 DEMO (Demographics and Administrative)
Contains primary administrative and patient characteristics for each report version.

| Column | Type | Description | Values / Examples |
| :--- | :--- | :--- | :--- |
| `primaryid` | string | Unique report identifier (Primary Key) | e.g. `100034521` |
| `caseid` | string | Underlying patient case identifier | e.g. `10003452` |
| `caseversion`| integer| Sequential version number for this case | `1`, `2`, `3` ... |
| `i_f_code` | string | Initial or Follow-up code | `I` = Initial, `F` = Follow-up |
| `event_dt` | string | Date event occurred | `YYYYMMDD` (may be partial, e.g. `YYYYMM`) |
| `mfr_dt` | string | Date manufacturer received initial report | `YYYYMMDD` |
| `init_fda_dt`| string | Date FDA first received the initial case | `YYYYMMDD` |
| `fda_dt` | string | Date FDA received this specific version | `YYYYMMDD` |
| `mfr_num` | string | Manufacturer report number | Verbatim string |
| `mfr_sndr` | string | Manufacturer sender code | Verbatim string |
| `age` | float | Patient age numeric value | e.g. `45.0` |
| `age_cod` | string | Age unit code | `YR` (Years), `MON` (Months), `DY` (Days), `WK` (Weeks), `HR` (Hours), `DEC` (Decades) |
| `age_grp` | string | Age group category | `N` (Neonate), `I` (Infant), `C` (Child), `T` (Adolescent), `A` (Adult), `E` (Elderly) |
| `sex` | string | Patient gender | `M` (Male), `F` (Female), `UNK` (Unknown) |
| `e_sub` | string | Electronic submission flag | `Y` (Yes), `N` (No) |
| `wt` | float | Patient weight numeric value | e.g. `70.5` |
| `wt_cod` | string | Weight unit code | `KG` (Kilograms), `LBS` (Pounds) |
| `rept_dt` | string | Date of report | `YYYYMMDD` |
| `to_mfr` | string | Voluntary report to manufacturer | `Y`, `N` |
| `occp_cod` | string | Reporter occupation | `MD` (Physician), `PH` (Pharmacist), `CN` (Consumer), `LW` (Lawyer), `OT` (Other health professional) |
| `reporter_country` | string | Reporter country of origin | ISO 2-letter country code |
| `occr_country` | string | Country where event occurred | ISO 2-letter country code |

---

### 3.2 DRUG (Drug Information & Roles)
Contains information on all medicinal products reported in the case.

| Column | Type | Description | Values / Examples |
| :--- | :--- | :--- | :--- |
| `primaryid` | string | Foreign Key to DEMO | Matches `DEMO.primaryid` |
| `drug_seq` | integer | Sequential counter for drugs in report | `1`, `2`, `3` ... |
| `role_cod` | string | Drug role in the adverse event | `PS` (Primary Suspect), `SS` (Secondary Suspect), `C` (Concomitant), `I` (Interacting) |
| `drugname` | string | Verbatim drug product name reported | e.g. `LIPITOR`, `METFORMIN`, `KEYTRUDA` |
| `prod_ai` | string | Standardized active ingredient (where available)| e.g. `ATORVASTATIN CALCIUM` |
| `val_vbm` | integer | Validation status code | `1` = Validated active ingredient, `2` = Not validated |
| `route` | string | Route of administration | Oral, Intravenous, Subcutaneous, Topical, etc. |
| `dose_vbm` | string | Verbatim dosage description | e.g. `10 MG ONCE DAILY` |
| `cum_dose_chr`| float | Cumulative dose amount | Numeric |
| `cum_dose_unit`| string | Cumulative dose unit | `MG`, `G`, `ML` |
| `dechal` | string | Action taken after discontinuation (Dechallenge) | `Y` (Positive), `N` (Negative), `U` (Unknown), `NA` (Not Applicable) |
| `rechal` | string | Reaction recurrence after reintroduction (Rechallenge) | `Y` (Positive), `N` (Negative), `U` (Unknown), `NA` (Not Applicable) |
| `lot_num` | string | Product lot/batch number | String |
| `exp_dt` | string | Product expiration date | `YYYYMMDD` |
| `nda_num` | string | Regulatory application number | String |

---

### 3.3 REAC (Adverse Reactions)
Contains adverse reaction terms coded according to the Medical Dictionary for Regulatory Activities (MedDRA).

| Column | Type | Description | Values / Examples |
| :--- | :--- | :--- | :--- |
| `primaryid` | string | Foreign Key to DEMO | Matches `DEMO.primaryid` |
| `pt` | string | MedDRA Preferred Term (PT) | e.g. `Myocardial infarction`, `Nausea`, `Acute kidney injury` |
| `drug_rec_act` | string | Drug recurrence action | Optional string |

---

### 3.4 OUTC (Patient Outcomes)
Records severe patient outcomes resulting from the event. A single report may have multiple outcomes.

| Column | Type | Description | Values / Examples |
| :--- | :--- | :--- | :--- |
| `primaryid` | string | Foreign Key to DEMO | Matches `DEMO.primaryid` |
| `outc_cod` | string | Severe outcome classification code | `DE` = Death<br>`LT` = Life-Threatening<br>`HO` = Hospitalization (Initial or Prolonged)<br>`DS` = Disability / Permanent Impairment<br>`CA` = Congenital Anomaly<br>`RI` = Required Intervention to Prevent Permanent Damage<br>`OT` = Other Serious (Important Medical Event) |

---

### 3.5 INDI (Indications)
Records the medical reason/indication for taking each drug.

| Column | Type | Description | Values / Examples |
| :--- | :--- | :--- | :--- |
| `primaryid` | string | Foreign Key to DEMO | Matches `DEMO.primaryid` |
| `indi_drug_seq` | integer | Drug sequence counter corresponding to `DRUG.drug_seq` | Matches `drug_seq` |
| `indi_pt` | string | MedDRA Preferred Term for Indication | e.g. `Type 2 diabetes mellitus`, `Hypertension` |

---

### 3.6 THER (Therapy Dates & Duration)
Records therapy start and end dates and duration of treatment.

| Column | Type | Description | Values / Examples |
| :--- | :--- | :--- | :--- |
| `primaryid` | string | Foreign Key to DEMO | Matches `DEMO.primaryid` |
| `dsg_drug_seq` | integer | Drug sequence counter corresponding to `DRUG.drug_seq` | Matches `drug_seq` |
| `start_dt` | string | Therapy start date | `YYYYMMDD` |
| `end_dt` | string | Therapy end date | `YYYYMMDD` |
| `dur` | float | Therapy duration value | Numeric |
| `dur_cod` | string | Therapy duration unit | `YR`, `MON`, `DY`, `WK`, `HR` |

---

### 3.7 RPSR (Report Sources)
Records who submitted or originated the report.

| Column | Type | Description | Values / Examples |
| :--- | :--- | :--- | :--- |
| `primaryid` | string | Foreign Key to DEMO | Matches `DEMO.primaryid` |
| `rpsr_cod` | string | Report source code | `FGN` (Foreign), `DIST` (Distributor), `HP` (Healthcare Professional), `UF` (User Facility), etc. |

---

# Derived layers

All derived columns sit next to the reported columns; reported values are never overwritten.

## Bronze (`data/bronze/<QUARTER>/<table>/`)
Every source column as a string, plus `_source_quarter`, `_source_file`, `_ingestion_timestamp`, `_is_synthetic`. `quarantine/<table>/` holds rows whose field count differs from the header (columns: `reason`, `field_count`, `raw_content`, `source_file`, `quarter`, `ingestion_timestamp`). `deleted/` holds FDA's deleted-case IDs (`caseid`). `ingestion_summary.json` records, per table, `source_records` (independent line count), `rows_ingested`, `rows_quarantined` and `reconciled`.

## Silver (`data/silver/`)
| Table | Grain | Added columns |
| :--- | :--- | :--- |
| `demo_versions` | one row per report version (`primaryid`) | `caseversion_int`, `<date>_date` + `<date>_precision` (DAY/MONTH/YEAR/INVALID) for event/mfr/init_fda/fda/rept dates, `age_years`, `wt_kg`, `sex_norm`, `*_country_norm`, `version_rank`, `is_latest_version`, `superseded_in_quarter`, `deleted_in_quarter`, `valid_from_quarter`, `valid_to_quarter` (exclusive), `ever_canonical` |
| `demo` | one row per currently canonical case | same as `demo_versions` |
| `drug` | one row per drug record | `drug_seq_int`, `role_cod_norm`, `drugname_norm`, `prod_ai_norm`, `drug_ingredients` (array), `drug_key` (ingredients joined by `\`), `drug_key_source`, `caseid_reported` |
| `reac` / `outc` / `indi` / `ther` / `rpsr` | one row per source record | `pt_norm`, `outc_cod_norm`, `indi_pt_norm`, `dsg_drug_seq_int`, `dur_num`, `start_dt_date`/`end_dt_date` + precision, `rpsr_cod_norm`; `caseid` is taken from the parent DEMO row |
| `deleted_cases` | one row per FDA-deleted case | `_source_quarter` = first quarter listed |
| `quarantine/<table>/` | excluded rows | `quarantine_reason` |

## Gold (`data/gold/<snapshot>/`, snapshot = `latest` or `asof_<QUARTER>`)
| Table | Grain / contents |
| :--- | :--- |
| `case_summary` | one row per canonical case: `caseid`, `primaryid`, `report_quarter`, descriptors, `outcomes`, `is_serious`, `is_death`, `n_drugs`, `n_ps_drugs`, `n_reactions` |
| `case_drug` | one row per (`caseid`, `drug_key` = active ingredient) with `roles`, `is_ps/ss/c/i`, `reported_names`, `drug_records` |
| `case_reaction`, `case_outcome` | one row per (`caseid`, `pt`) / (`caseid`, `outc_cod`) |
| `drug_summary`, `reaction_summary`, `outcome_summary`, `quarterly_trends`, `drug_name_index` | counts of unique cases |
| `drug_outcomes`, `drug_sex`, `drug_age`, `drug_quarterly` | per-drug profiles by `role_policy` |
| `drug_event_counts/role_policy=*` | `drug_key`, `pt`, `a`, `b`, `c`, `d`, `n_drug`, `n_event`, `n_eligible_cases` |
| `safety_signals/role_policy=*` | `drug_name`, `reaction_term`, `analysis_period`, `observed_cases`, `expected_cases`, `background_cases`, `a`-`d`, `ROR`, `ROR_CI_lower/upper`, `PRR`, `PRR_CI_*`, `chi2_yates`, `zero_corrected`, `screening_status`, `screening_reasons`, `method_version`, `analysis_timestamp` |
| `temporal/quarterly_signals`, `temporal/signal_persistence`, `temporal/quarterly_reaction_counts` | per-quarter statistics and persistence |
| `combinations/drug_combinations`, `combinations/combination_signals` | co-reported pairs and pair-reaction statistics |
