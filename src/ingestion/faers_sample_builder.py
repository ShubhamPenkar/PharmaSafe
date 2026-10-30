"""
PharmaSafe Synthetic FAERS Sample Archive Builder
Generates realistic, deterministic quarterly FAERS ASCII zip archives with official $ delimiters.
Includes intentional duplicate versions, tied-version edge cases, malformed rows,
orphaned records, and known statistical signal pairs for reproducible verification.
"""

import os
import sys
import zipfile
import random
from pathlib import Path
from typing import Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import get_project_root

logger = get_logger("PharmaSafe.SampleBuilder")


def build_synthetic_faers_quarter(
    output_zip_path: Path,
    quarter_id: str = "2024Q1",
    seed: int = 42
) -> Dict[str, Any]:
    """
    Constructs a deterministic FAERS quarterly zip archive with the standard FDA ASCII structure:
      ASCII/DEMO<YY>Q<Q>.txt
      ASCII/DRUG<YY>Q<Q>.txt
      ASCII/REAC<YY>Q<Q>.txt
      ASCII/OUTC<YY>Q<Q>.txt
      ASCII/INDI<YY>Q<Q>.txt
      ASCII/THER<YY>Q<Q>.txt
      ASCII/RPSR<YY>Q<Q>.txt
    """
    random.seed(seed)
    yy = quarter_id[2:4]
    q = quarter_id[5:6]
    output_zip_path.parent.mkdir(parents=True, exist_ok=True)

    # 1. DEMO table records:
    # primaryid$caseid$caseversion$i_f_code$event_dt$mfr_dt$init_fda_dt$fda_dt$mfr_num$mfr_sndr$age$age_cod$age_grp$sex$e_sub$wt$wt_cod$rept_dt$to_mfr$occp_cod$reporter_country$occr_country
    demo_headers = [
        "primaryid", "caseid", "caseversion", "i_f_code", "event_dt", "mfr_dt",
        "init_fda_dt", "fda_dt", "mfr_num", "mfr_sndr", "age", "age_cod",
        "age_grp", "sex", "e_sub", "wt", "wt_cod", "rept_dt", "to_mfr",
        "occp_cod", "reporter_country", "occr_country"
    ]
    
    # Controlled Cases:
    # 1001: v1 (fda_dt 20240110) vs v2 (fda_dt 20240220) -> v2 wins
    # 1002: v1 (fda_dt 20240115, primaryid 10021) vs v2 (fda_dt 20240115, primaryid 10022) -> same date, v2 wins
    # 1003: identical duplicate row submitted twice
    # 1004: normal single case
    # 1005: normal single case
    # 1006: multi-quarter time case: v1 fda_dt 20240215 (Q1), will have follow up v2 fda_dt 20240510 (Q2)
    demo_rows = [
        # Case 1001: Version 1 (superseded)
        ["10011", "1001", "1", "I", "20240101", "20240108", "20240110", "20240110", "MFR-01", "PFIZ", "54.0", "YR", "A", "M", "Y", "78.0", "KG", "20240110", "Y", "MD", "US", "US"],
        # Case 1001: Version 2 (canonical winner)
        ["10012", "1001", "2", "F", "20240101", "20240218", "20240110", "20240220", "MFR-01", "PFIZ", "54.0", "YR", "A", "M", "Y", "78.0", "KG", "20240220", "Y", "MD", "US", "US"],
        # Case 1002: Version 1
        ["10021", "1002", "1", "I", "20240105", "20240112", "20240115", "20240115", "MFR-02", "NOV", "62.0", "YR", "A", "F", "Y", "65.0", "KG", "20240115", "Y", "PH", "US", "US"],
        # Case 1002: Version 2 (tied date 20240115, higher version & primaryid)
        ["10022", "1002", "2", "F", "20240105", "20240114", "20240115", "20240115", "MFR-02", "NOV", "62.0", "YR", "A", "F", "Y", "65.0", "KG", "20240115", "Y", "PH", "US", "US"],
        # Case 1003: Row 1
        ["10031", "1003", "1", "I", "20240112", "20240118", "20240120", "20240120", "MFR-03", "ROCH", "45.0", "YR", "A", "M", "Y", "82.0", "KG", "20240120", "Y", "MD", "US", "US"],
        # Case 1003: Exact duplicate of 10031
        ["10031", "1003", "1", "I", "20240112", "20240118", "20240120", "20240120", "MFR-03", "ROCH", "45.0", "YR", "A", "M", "Y", "82.0", "KG", "20240120", "Y", "MD", "US", "US"],
        # Case 1004: Normal
        ["10041", "1004", "1", "I", "20240120", "20240125", "20240128", "20240128", "MFR-04", "AZ", "71.0", "YR", "E", "F", "Y", "59.0", "KG", "20240128", "Y", "CN", "CA", "CA"],
        # Case 1005: Missing weight & reporter country
        ["10051", "1005", "1", "I", "20240201", "20240205", "20240208", "20240208", "MFR-05", "GSK", "38.0", "YR", "A", "F", "Y", "", "", "20240208", "Y", "OT", "", "GB"],
        # Case 1006: Version 1 (Q1 entry)
        ["10061", "1006", "1", "I", "20240210", "20240214", "20240215", "20240215", "MFR-06", "MERCK", "67.0", "YR", "E", "M", "Y", "88.0", "KG", "20240215", "Y", "MD", "US", "US"],
    ]

    # Additional synthetic cohort for statistical signal calculations (Cases 2001 to 2040)
    # Total unique cases in cohort = 6 + 40 = 46 canonical cases
    for cid in range(2001, 2041):
        pid = f"{cid}1"
        age = str(random.randint(25, 80))
        sex = "M" if cid % 2 == 0 else "F"
        fda_dt = f"2024{random.randint(1, 3):02d}{random.randint(1, 28):02d}"
        demo_rows.append([
            pid, str(cid), "1", "I", fda_dt, fda_dt, fda_dt, fda_dt,
            f"MFR-{cid}", "SYNTH_CORP", age, "YR", "A", sex, "Y", "70.0", "KG", fda_dt, "Y", "MD", "US", "US"
        ])

    # Malformed row in DEMO (intentional parsing test case: wrong field count)
    malformed_demo_row = "MALFORMED_ROW$TOO_FEW_COLUMNS$12345\n"

    # 2. DRUG table records:
    # primaryid$drug_seq$role_cod$drugname$prod_ai$val_vbm$route$dose_vbm$cum_dose_chr$cum_dose_unit$dechal$rechal$lot_num$exp_dt$nda_num
    drug_headers = [
        "primaryid", "drug_seq", "role_cod", "drugname", "prod_ai", "val_vbm",
        "route", "dose_vbm", "cum_dose_chr", "cum_dose_unit", "dechal", "rechal",
        "lot_num", "exp_dt", "nda_num"
    ]
    drug_rows = [
        # Case 1001 v1 (superseded)
        ["10011", "1", "PS", "SYNTH_DRUG_ALPHA", "SYNTH_DRUG_ALPHA", "1", "Oral", "10 MG", "", "", "Y", "NA", "L01", "", "NDA001"],
        # Case 1001 v2 (canonical)
        ["10012", "1", "PS", "SYNTH_DRUG_ALPHA", "SYNTH_DRUG_ALPHA", "1", "Oral", "10 MG", "", "", "Y", "NA", "L01", "", "NDA001"],
        # Case 1002 v1 (superseded)
        ["10021", "1", "PS", "SYNTH_DRUG_ALPHA", "SYNTH_DRUG_ALPHA", "1", "Oral", "20 MG", "", "", "Y", "NA", "L02", "", "NDA001"],
        # Case 1002 v2 (canonical)
        ["10022", "1", "PS", "SYNTH_DRUG_ALPHA", "SYNTH_DRUG_ALPHA", "1", "Oral", "20 MG", "", "", "Y", "NA", "L02", "", "NDA001"],
        # Case 1003 v1
        ["10031", "1", "PS", "SYNTH_DRUG_ALPHA", "SYNTH_DRUG_ALPHA", "1", "Oral", "10 MG", "", "", "Y", "NA", "L03", "", "NDA001"],
        # Case 1004 v1 (Different drug)
        ["10041", "1", "PS", "SYNTH_DRUG_BETA", "SYNTH_DRUG_BETA", "1", "Oral", "50 MG", "", "", "N", "NA", "L04", "", "NDA002"],
        # Case 1005 v1
        ["10051", "1", "PS", "SYNTH_DRUG_BETA", "SYNTH_DRUG_BETA", "1", "Oral", "50 MG", "", "", "N", "NA", "L05", "", "NDA002"],
        # Case 1006 v1
        ["10061", "1", "PS", "BENCH_STATIN", "BENCH_STATIN", "1", "Oral", "40 MG", "", "", "Y", "NA", "L06", "", "NDA003"],
        # Orphan record (primaryid 99999 does not exist in DEMO)
        ["99999", "1", "PS", "ORPHAN_DRUG", "ORPHAN_DRUG", "1", "Oral", "10 MG", "", "", "U", "NA", "L99", "", "NDA999"],
    ]

    # Assign drugs to cohort cases 2001-2040:
    # 2001-2015 (15 cases): SYNTH_DRUG_ALPHA (PS)
    # 2016-2025 (10 cases): BENCH_STATIN (PS)
    # 2026-2040 (15 cases): SYNTH_DRUG_BETA (PS)
    for cid in range(2001, 2041):
        pid = f"{cid}1"
        if cid <= 2015:
            dname = "SYNTH_DRUG_ALPHA"
        elif cid <= 2025:
            dname = "BENCH_STATIN"
        else:
            dname = "SYNTH_DRUG_BETA"
        drug_rows.append([
            pid, "1", "PS", dname, dname, "1", "Oral", "10 MG", "", "", "Y", "NA", f"LOT-{cid}", "", "NDA100"
        ])

    # 3. REAC table records:
    # primaryid$pt$drug_rec_act
    reac_headers = ["primaryid", "pt", "drug_rec_act"]
    reac_rows = [
        # Case 1001 v1
        ["10011", "SYNTH_EVENT_TACHY", ""],
        # Case 1001 v2
        ["10012", "SYNTH_EVENT_TACHY", ""],
        # Case 1002 v1
        ["10021", "SYNTH_EVENT_TACHY", ""],
        # Case 1002 v2
        ["10022", "SYNTH_EVENT_TACHY", ""],
        # Case 1003 v1
        ["10031", "SYNTH_EVENT_TACHY", ""],
        # Case 1004 v1
        ["10041", "HEADACHE", ""],
        # Case 1005 v1
        ["10051", "NAUSEA", ""],
        # Case 1006 v1
        ["10061", "BENCH_RHABDOMYOLYSIS", ""],
        # Orphan record
        ["99999", "SYNTH_EVENT_ORPHAN", ""],
    ]

    # Assign reactions to cohort:
    # SYNTH_DRUG_ALPHA (cases 2001-2015): 10 cases have SYNTH_EVENT_TACHY, 5 have HEADACHE
    # BENCH_STATIN (cases 2016-2025): 8 cases have BENCH_RHABDOMYOLYSIS, 2 have HEADACHE
    # SYNTH_DRUG_BETA (cases 2026-2040): 2 cases have SYNTH_EVENT_TACHY, 13 have NAUSEA
    for cid in range(2001, 2041):
        pid = f"{cid}1"
        if cid <= 2010:
            event = "SYNTH_EVENT_TACHY"
        elif cid <= 2015:
            event = "HEADACHE"
        elif cid <= 2023:
            event = "BENCH_RHABDOMYOLYSIS"
        elif cid <= 2025:
            event = "HEADACHE"
        elif cid <= 2027:
            event = "SYNTH_EVENT_TACHY"
        else:
            event = "NAUSEA"
        reac_rows.append([pid, event, ""])

    # 4. OUTC table records:
    # primaryid$outc_cod
    outc_headers = ["primaryid", "outc_cod"]
    outc_rows = [
        ["10012", "HO"],
        ["10022", "HO"],
        ["10031", "OT"],
        ["10041", "OT"],
        ["10051", "OT"],
        ["10061", "HO"],
        ["99999", "DE"],  # orphan outcome
    ]
    for cid in range(2001, 2041):
        pid = f"{cid}1"
        code = "HO" if cid % 3 == 0 else "OT"
        outc_rows.append([pid, code])

    # 5. INDI table records:
    # primaryid$indi_drug_seq$indi_pt
    indi_headers = ["primaryid", "indi_drug_seq", "indi_pt"]
    indi_rows = [
        ["10012", "1", "HYPERTENSION"],
        ["10022", "1", "HYPERTENSION"],
        ["10031", "1", "HYPERTENSION"],
        ["10041", "1", "DIABETES"],
        ["10051", "1", "DIABETES"],
        ["10061", "1", "HYPERCHOLESTEROLEMIA"],
    ]

    # 6. THER table records:
    # primaryid$dsg_drug_seq$start_dt$end_dt$dur$dur_cod
    ther_headers = ["primaryid", "dsg_drug_seq", "start_dt", "end_dt", "dur", "dur_cod"]
    ther_rows = [
        ["10012", "1", "20240101", "20240110", "10", "DY"],
        ["10022", "1", "20240105", "20240115", "10", "DY"],
    ]

    # 7. RPSR table records:
    # primaryid$rpsr_cod
    rpsr_headers = ["primaryid", "rpsr_cod"]
    rpsr_rows = [
        ["10012", "FGN"],
        ["10022", "HP"],
        ["10031", "HP"],
        ["10041", "CSM"],
        ["10051", "CSM"],
        ["10061", "HP"],
    ]

    # Write files into zip archive
    tables_to_write = {
        f"ASCII/DEMO{yy}Q{q}.txt": (demo_headers, demo_rows, malformed_demo_row),
        f"ASCII/DRUG{yy}Q{q}.txt": (drug_headers, drug_rows, None),
        f"ASCII/REAC{yy}Q{q}.txt": (reac_headers, reac_rows, None),
        f"ASCII/OUTC{yy}Q{q}.txt": (outc_headers, outc_rows, None),
        f"ASCII/INDI{yy}Q{q}.txt": (indi_headers, indi_rows, None),
        f"ASCII/THER{yy}Q{q}.txt": (ther_headers, ther_rows, None),
        f"ASCII/RPSR{yy}Q{q}.txt": (rpsr_headers, rpsr_rows, None),
    }

    with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for archive_rel_path, (headers, rows, malformed_extra) in tables_to_write.items():
            content_lines = ["$".join(headers)]
            for r in rows:
                content_lines.append("$".join(r))
            full_content = "\n".join(content_lines) + "\n"
            if malformed_extra:
                full_content += malformed_extra
            zf.writestr(archive_rel_path, full_content.encode("utf-8"))

    logger.info("Generated synthetic FAERS archive at %s (size: %d bytes)", output_zip_path, output_zip_path.stat().st_size)

    return {
        "quarter": quarter_id,
        "archive_path": str(output_zip_path),
        "total_demo_rows": len(demo_rows) + 1,  # +1 for malformed row
        "canonical_cases_expected": 46,
        "duplicate_versions_expected": 2,       # 1001 v1, 1002 v1
        "exact_duplicates_expected": 1,         # 10031 duplicate
        "orphan_primaryids": ["99999"],
        "known_signal_pairs": {
            ("SYNTH_DRUG_ALPHA", "SYNTH_EVENT_TACHY"): {
                "expected_signal": True,
                "notes": "Controlled planted signal: highly disproportionate"
            },
            ("BENCH_STATIN", "BENCH_RHABDOMYOLYSIS"): {
                "expected_signal": True,
                "notes": "Synthetic benchmark proxy for statin rhabdomyolysis"
            },
            ("SYNTH_DRUG_BETA", "SYNTH_EVENT_TACHY"): {
                "expected_signal": False,
                "notes": "Controlled negative: background rate"
            }
        }
    }


def build_synthetic_faers_quarter_2(
    output_zip_path: Path,
    seed: int = 43
) -> Dict[str, Any]:
    """
    Constructs a second quarter (2024Q2) archive for testing:
    - Multi-quarter expansion.
    - Historical cutoff test: Case 1006 receives follow-up Version 2 in Q2 (fda_dt 20240510).
    """
    random.seed(seed)
    output_zip_path.parent.mkdir(parents=True, exist_ok=True)
    
    demo_headers = [
        "primaryid", "caseid", "caseversion", "i_f_code", "event_dt", "mfr_dt",
        "init_fda_dt", "fda_dt", "mfr_num", "mfr_sndr", "age", "age_cod",
        "age_grp", "sex", "e_sub", "wt", "wt_cod", "rept_dt", "to_mfr",
        "occp_cod", "reporter_country", "occr_country"
    ]
    
    # In Q2: Case 1006 gets a follow-up v2 with date 20240510, plus new cases 3001-3020
    demo_rows = [
        # Case 1006 Version 2 (received in Q2)
        ["10062", "1006", "2", "F", "20240210", "20240508", "20240215", "20240510", "MFR-06", "MERCK", "67.0", "YR", "E", "M", "Y", "88.0", "KG", "20240510", "Y", "MD", "US", "US"]
    ]
    for cid in range(3001, 3021):
        pid = f"{cid}1"
        fda_dt = f"2024{random.randint(4, 6):02d}{random.randint(1, 28):02d}"
        demo_rows.append([
            pid, str(cid), "1", "I", fda_dt, fda_dt, fda_dt, fda_dt,
            f"MFR-{cid}", "SYNTH_CORP", "50", "YR", "A", "M", "Y", "70.0", "KG", fda_dt, "Y", "MD", "US", "US"
        ])

    drug_headers = ["primaryid", "drug_seq", "role_cod", "drugname", "prod_ai", "val_vbm", "route", "dose_vbm", "cum_dose_chr", "cum_dose_unit", "dechal", "rechal", "lot_num", "exp_dt", "nda_num"]
    drug_rows = [
        ["10062", "1", "PS", "BENCH_STATIN", "BENCH_STATIN", "1", "Oral", "40 MG", "", "", "Y", "NA", "L06", "", "NDA003"]
    ]
    for cid in range(3001, 3021):
        pid = f"{cid}1"
        drug_rows.append([pid, "1", "PS", "SYNTH_DRUG_ALPHA", "SYNTH_DRUG_ALPHA", "1", "Oral", "10 MG", "", "", "Y", "NA", f"LOT-{cid}", "", "NDA100"])

    reac_headers = ["primaryid", "pt", "drug_rec_act"]
    reac_rows = [
        ["10062", "BENCH_RHABDOMYOLYSIS", ""]
    ]
    for cid in range(3001, 3021):
        pid = f"{cid}1"
        reac_rows.append([pid, "SYNTH_EVENT_TACHY", ""])

    outc_headers = ["primaryid", "outc_cod"]
    outc_rows = [["10062", "HO"]]
    for cid in range(3001, 3021):
        outc_rows.append([f"{cid}1", "HO"])

    with zipfile.ZipFile(output_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("ASCII/DEMO24Q2.txt", ("$".join(demo_headers) + "\n" + "\n".join("$".join(r) for r in demo_rows) + "\n").encode("utf-8"))
        zf.writestr("ASCII/DRUG24Q2.txt", ("$".join(drug_headers) + "\n" + "\n".join("$".join(r) for r in drug_rows) + "\n").encode("utf-8"))
        zf.writestr("ASCII/REAC24Q2.txt", ("$".join(reac_headers) + "\n" + "\n".join("$".join(r) for r in reac_rows) + "\n").encode("utf-8"))
        zf.writestr("ASCII/OUTC24Q2.txt", ("$".join(outc_headers) + "\n" + "\n".join("$".join(r) for r in outc_rows) + "\n").encode("utf-8"))

    logger.info("Generated synthetic FAERS Q2 archive at %s (size: %d bytes)", output_zip_path, output_zip_path.stat().st_size)
    return {
        "quarter": "2024Q2",
        "archive_path": str(output_zip_path),
        "total_demo_rows": len(demo_rows)
    }
