# PharmaSafe Methodology & Counting Rules Specification

## 1. Scientific Objective
PharmaSafe is designed to perform large-scale pharmacovigilance disproportionality analysis on spontaneous adverse-event reports from the FDA FAERS / AEMS database.
The objective is to detect potential drug safety signals—statistical disproportionalities where a drug is reported with an adverse event more frequently than expected relative to the background database.

> **CRITICAL SCIENTIFIC PRINCIPLE:**
> A statistical signal is **not** proof of causality, does **not** establish drug-induced harm, and cannot be used to calculate clinical incidence rates. It serves as an exploratory, decision-support alert prompting clinical investigation.

---

## 2. Report vs. Case Distinction and Deduplication Rules

### 2.1 The Problem
- **Report (`primaryid`)**: Represents an individual document submission sent to the FDA.
- **Case (`caseid`)**: Represents the underlying patient experience or incident.
- Over time, pharmaceutical manufacturers and physicians submit follow-up reports as new medical records, lab findings, or autopsy results become available.
- Each follow-up generates a new `primaryid` with an incremented `caseversion` and a new receipt date (`fda_dt`).
- If an analytics pipeline aggregates raw records across quarters without case-level deduplication, repeated follow-ups will artificially inflate report counts for severe or high-profile cases, creating severe statistical artifacts and false positive signals.

### 2.2 Report duplicates, case versions and validity intervals
1. **Report-level duplicates** (the same `primaryid` more than once, across or within quarter files) are classified, never silently merged: `EXACT_DUPLICATE`, `PRIMARYID_RESUBMITTED_LATER` (latest quarter kept) or `CONFLICTING_DUPLICATE_PRIMARYID`. Rows with a non-numeric `primaryid`/`caseid` are quarantined as `INVALID_IDENTIFIER`.
2. **Case versions.** Every retained report version of a case is kept and ranked by `caseversion` DESC, then `fda_dt` DESC, then numeric `primaryid` DESC (NULLs last). A missing or invalid `caseversion` is never replaced by an assumed value; it ranks below any known version.
3. **Validity interval.** Each version carries `valid_from_quarter` (the quarter it was published) and `valid_to_quarter` (exclusive: the first quarter in which a higher-ranked version, or an FDA deletion for the case, appeared). The canonical case set *as of quarter Q* is the versions with `valid_from <= Q < valid_to`. A follow-up published later therefore never rewrites an earlier snapshot.
4. **FDA deleted cases.** The `DELETED` list shipped in each quarter removes the case from the canonical set from that quarter on.
5. **Child tables** keep rows for every retained version. A child row is quarantined only when it is a true orphan (`ORPHAN_NO_DEMO_RECORD`), its parent was quarantined (`PARENT_DEMO_QUARANTINED`), its parent came from another quarter file (`PARENT_FROM_OTHER_QUARTER`) or it is an exact duplicate. Superseded versions are not orphans; they drop out in Gold by joining to the canonical `primaryid`.

Two views are produced: `latest` (currently canonical version of every non-deleted case) and `asof_<quarter>` snapshots.

---

## 3. Unit of Analysis & Counting Rules

### 3.1 What is Counted?
The unit of analysis is the **Unique Eligible Case**.
- If a patient report mentions the same drug multiple times (e.g. different dosage lines in `DRUG`), the case is counted **once** for that drug.
- If a patient report mentions the same reaction term multiple times (e.g. MedDRA PT listed twice in `REAC`), the case is counted **once** for that reaction.
- Joins between `DEMO`, `DRUG`, and `REAC` are deduplicated at the `(caseid, drug_name)` and `(caseid, reaction_term)` level before constructing contingency tables. This avoids Cartesian join explosion.

### 3.1.1 Drug key and eligible population
- **Drug key:** the FDA-mapped active ingredient (`prod_ai`, uppercased, whitespace-collapsed); if absent, the reported `drugname`. Original names are kept alongside (`drugname`, `reported_names`). Brand/generic spellings FDA did not map are not merged.
- **Eligible case:** a canonical case with at least one drug in the selected role policy **and** at least one reported reaction. N is the number of eligible cases, and drug and reaction counts are taken over those same cases, so a + b + c + d = N for every pair.
- **Reaction term:** the MedDRA preferred term (`pt`), uppercased; no higher-level grouping.

### 3.2 Drug Role Population Policy
FAERS classifies each reported drug with a role code (`role_cod`):
- `PS`: **Primary Suspect** (the drug judged most suspect in causing the reaction)
- `SS`: **Secondary Suspect** (other suspect medications)
- `C`: **Concomitant** (drugs taken concurrently but not deemed primarily suspect)
- `I`: **Interacting** (drugs suspected of drug-drug interaction)

**Policy Hierarchy:**
1. **Primary Analysis (Default):** Restrict to `role_cod = 'PS'`. This isolates reports where the reporter explicitly suspected the drug as the primary driver of the reaction.
2. **Sensitivity Analysis 1 (All Suspect):** `role_cod IN ('PS', 'SS')`.
3. **Sensitivity Analysis 2 (All Reported):** `role_cod IN ('PS', 'SS', 'C', 'I')`.

---

## 4. Statistical Signal Detection Engine

### 4.1 The 2x2 Contingency Table
For every unique `(Drug, Adverse Event)` pair within the selected time window and eligible case population:

| | Target Event Reported ($E$) | Target Event Not Reported ($\neg E$) | Total Cases |
| :--- | :---: | :---: | :---: |
| **Target Drug Reported ($D$)** | $a$ | $b$ | $a + b$ |
| **Target Drug Not Reported ($\neg D$)** | $c$ | $d$ | $c + d$ |
| **Total Database Cases** | $a + c$ | $b + d$ | $N = a + b + c + d$ |

Where:
- $a$: Count of unique cases reporting both target Drug $D$ and target Event $E$.
- $b$: Count of unique cases reporting target Drug $D$ without target Event $E$.
- $c$: Count of unique cases reporting target Event $E$ without target Drug $D$.
- $d$: Count of unique cases reporting neither target Drug $D$ nor target Event $E$.
- $N$: Total number of unique deduplicated cases in the analysis window.

### 4.2 Reporting Odds Ratio (ROR)
The Reporting Odds Ratio compares the odds of reporting the event for the drug of interest with the odds of reporting the event for all other drugs in the reference database:

$$\text{ROR} = \frac{a / b}{c / d} = \frac{a \cdot d}{b \cdot c}$$

#### Standard Error and 95% Confidence Interval:
$$\text{SE}(\ln(\text{ROR})) = \sqrt{\frac{1}{a} + \frac{1}{b} + \frac{1}{c} + \frac{1}{d}}$$

$$95\% \text{ CI}_{\text{lower}} = \exp\left(\ln(\text{ROR}) - 1.96 \cdot \text{SE}(\ln(\text{ROR}))\right)$$

$$95\% \text{ CI}_{\text{upper}} = \exp\left(\ln(\text{ROR}) + 1.96 \cdot \text{SE}(\ln(\text{ROR}))\right)$$

### 4.3 Proportional Reporting Ratio (PRR)
The Proportional Reporting Ratio measures the proportion of all reports for the drug that mention the event, divided by the proportion of all other reports in the database that mention the event:

$$\text{PRR} = \frac{a / (a + b)}{c / (c + d)}$$

#### Standard Error and 95% Confidence Interval:
$$\text{SE}(\ln(\text{PRR})) = \sqrt{\frac{1}{a} - \frac{1}{a+b} + \frac{1}{c} - \frac{1}{c+d}}$$

$$95\% \text{ CI}_{\text{lower}} = \exp\left(\ln(\text{PRR}) - 1.96 \cdot \text{SE}(\ln(\text{PRR}))\right)$$

$$95\% \text{ CI}_{\text{upper}} = \exp\left(\ln(\text{PRR}) + 1.96 \cdot \text{SE}(\ln(\text{PRR}))\right)$$

### 4.4 Zero-Count Handling (Haldane-Anscombe Correction)
If any cell ($a, b, c, d$) is equal to 0, direct calculation yields division by zero or infinite odds ratios.
When any cell equals 0, PharmaSafe applies the standard Haldane-Anscombe correction by adding $0.5$ to each cell in the contingency table ($a+0.5, b+0.5, c+0.5, d+0.5$).

### 4.5 Standard Regulatory Screening Thresholds
A drug–event pair is flagged as a **Potential Safety Signal** (`POTENTIAL_SIGNAL`; otherwise `NOT_FLAGGED`) if all of the following project screening criteria are satisfied (they are configurable in `configs/analysis.yaml`, are not clinical or regulatory standards, and are sensitivity-tested in the validation report):
1. **Minimum Case Count ($a$):** $a \ge 3$ (eliminates noise from isolated single/double occurrences).
2. **Lower Confidence Bound of ROR:** $\text{ROR}_{025} > 1.0$ (statistically significant disproportionality at $\alpha=0.05$).
3. **PRR Disproportionality:** $\text{PRR} \ge 2.0$.
4. **Chi-square (Yates) ≥ 4.0** (configured via `min_chi2`).

Pairs with fewer than `output.min_stored_cases` cases are not stored in Gold because they can never pass screening.

---

## 4.6 Temporal analytics
A case's *quarter* is the source (FDA publication) quarter of its canonical report version; it is distinct from event date, receipt date and extraction date. Quarters partition the case population, and per-quarter statistics use only that quarter's cases. Changes describe reporting patterns, not incidence.

## 4.7 Drug-combination analytics
For drug pairs co-reported in an eligible case (suspect roles by default), a = cases with both drugs and the reaction; the comparison group is all other eligible cases. Single-drug RORs are shown as context. Co-reporting is not evidence of a pharmacological interaction.

## 4.8 Validation
Reference-signal recovery (with negative controls and explicit "not evaluable" outcomes), a threshold/role-policy robustness grid, and a discovery-quarter to evaluation-quarter check are produced by `src/validation/benchmark_evaluator.py` into `reports/validation_report.md`.

---

## 5. Methodological Limitations & Epidemiological Caveats
Every dashboard view and analytical report generated by PharmaSafe MUST prominently document the following limitations:
1. **Absence of Denominator:** Spontaneous reporting systems capture numerator counts only. The true number of patients exposed to the drug is unknown; therefore, absolute incidence or risk cannot be calculated.
2. **Under-reporting and Reporting Biases:** Only a fraction of adverse events are reported to the FDA (often estimated at 1-10%). Reporting rates vary by disease severity, patient age, country, and regulatory alerts.
3. **Stimulated Reporting & Notoriety Bias:** Media coverage or regulatory warnings trigger sudden spikes in reporting (the Weber effect), inflating disproportionality without an actual increase in true biological risk.
4. **Confounding by Indication:** Patients with severe underlying diseases (e.g. advanced heart failure) receive specific therapies. Events characteristic of the disease progression (e.g. cardiac arrest) may appear disproportionately associated with the drug without a causal link.
5. **Data Quality Issues:** Verbatim drug names may contain spelling variations, missing dosages, or incomplete demographics. PharmaSafe addresses this via string normalization, role filtering, and automated validation gates.
