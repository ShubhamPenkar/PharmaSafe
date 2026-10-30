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

### 2.2 Deterministic Deduplication Algorithm
To ensure reproducibility and scientific integrity:
1. Group all records in the `DEMO` table by `caseid`.
2. Rank records within each `caseid` window using the following deterministic hierarchy:
   - **Primary sort key:** `caseversion` descending (select the latest version).
   - **Tiebreaker 1:** `fda_dt` (FDA receipt date) descending.
   - **Tiebreaker 2:** `primaryid` descending (deterministic numeric tiebreaker).
3. The top-ranked record is designated the **Canonical Case Representation**.
4. The winning `primaryid` is retained; all superseded `primaryid`s for that `caseid` are filtered out.
5. In child tables (`DRUG`, `REAC`, `OUTC`, `INDI`, `THER`, `RPSR`), only rows matching the Canonical Case `primaryid` are retained. This eliminates orphan rows and prevents mixing conflicting versions of the same case.

---

## 3. Unit of Analysis & Counting Rules

### 3.1 What is Counted?
The unit of analysis is the **Unique Eligible Case**.
- If a patient report mentions the same drug multiple times (e.g. different dosage lines in `DRUG`), the case is counted **once** for that drug.
- If a patient report mentions the same reaction term multiple times (e.g. MedDRA PT listed twice in `REAC`), the case is counted **once** for that reaction.
- Joins between `DEMO`, `DRUG`, and `REAC` are deduplicated at the `(caseid, drug_name)` and `(caseid, reaction_term)` level before constructing contingency tables. This avoids Cartesian join explosion.

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
A drug–event pair is flagged as a **Candidate Safety Signal** if all of the following criteria are satisfied:
1. **Minimum Case Count ($a$):** $a \ge 3$ (eliminates noise from isolated single/double occurrences).
2. **Lower Confidence Bound of ROR:** $\text{ROR}_{025} > 1.0$ (statistically significant disproportionality at $\alpha=0.05$).
3. **PRR Disproportionality:** $\text{PRR} \ge 2.0$.

---

## 5. Methodological Limitations & Epidemiological Caveats
Every dashboard view and analytical report generated by PharmaSafe MUST prominently document the following limitations:
1. **Absence of Denominator:** Spontaneous reporting systems capture numerator counts only. The true number of patients exposed to the drug is unknown; therefore, absolute incidence or risk cannot be calculated.
2. **Under-reporting and Reporting Biases:** Only a fraction of adverse events are reported to the FDA (often estimated at 1-10%). Reporting rates vary by disease severity, patient age, country, and regulatory alerts.
3. **Stimulated Reporting & Notoriety Bias:** Media coverage or regulatory warnings trigger sudden spikes in reporting (the Weber effect), inflating disproportionality without an actual increase in true biological risk.
4. **Confounding by Indication:** Patients with severe underlying diseases (e.g. advanced heart failure) receive specific therapies. Events characteristic of the disease progression (e.g. cardiac arrest) may appear disproportionately associated with the drug without a causal link.
5. **Data Quality Issues:** Verbatim drug names may contain spelling variations, missing dosages, or incomplete demographics. PharmaSafe addresses this via string normalization, role filtering, and automated validation gates.
