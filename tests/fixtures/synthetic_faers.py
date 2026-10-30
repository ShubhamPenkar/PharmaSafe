"""
PharmaSafe Synthetic FAERS Data Generator
Generates controlled, deterministic multi-table test cases with known:
- Duplicate report versions
- Orphan child records
- Drug roles (PS, SS, C, I)
- Pre-calculated contingency counts for mathematical assertion
"""

from typing import Dict
from pyspark.sql import SparkSession, DataFrame


def create_synthetic_faers_data(spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Creates synthetic DEMO, DRUG, REAC, and OUTC DataFrames with known ground truth.
    
    Structure:
    - DEMO:
      * Case 101: Version 1 (fda_dt='20240101') AND Version 2 (fda_dt='20240215') -> Version 2 must be selected!
      * Case 102: Version 1 (fda_dt='20240110')
      * Case 103: Version 1 (fda_dt='20240112')
      * Case 104: Version 1 (fda_dt='20240115')
      * Case 105: Version 1 (fda_dt='20240120')
      Total raw DEMO = 6 rows, Unique canonical cases = 5 cases.
      
    - DRUG:
      * Primaryid 10101 (superseded): Atorvastatin
      * Primaryid 10102 (canonical): Atorvastatin (PS)
      * Primaryid 10201 (canonical): Atorvastatin (PS)
      * Primaryid 10301 (canonical): Atorvastatin (PS)
      * Primaryid 10401 (canonical): Amoxicillin (PS)
      * Primaryid 10501 (canonical): Amoxicillin (PS)
      * Primaryid 99999 (orphan, not in DEMO): Lisinopril
      
    - REAC:
      * Primaryid 10101 (superseded): Myalgia
      * Primaryid 10102 (canonical): Rhabdomyolysis
      * Primaryid 10201 (canonical): Rhabdomyolysis
      * Primaryid 10301 (canonical): Rhabdomyolysis
      * Primaryid 10401 (canonical): Rash
      * Primaryid 10501 (canonical): Rash
      * Primaryid 99999 (orphan, not in DEMO): Headache
      
    - OUTC:
      * Primaryid 10102: HO (Hospitalization)
      * Primaryid 10201: HO
      * Primaryid 10301: DE (Death)
      * Primaryid 10401: OT
      * Primaryid 10501: OT
    """
    demo_data = [
        # primaryid, caseid, caseversion, fda_dt, age, sex, occr_country
        ("10101", "101", 1, "20240101", 55.0, "M", "US"),  # Older version of 101
        ("10102", "101", 2, "20240215", 55.0, "M", "US"),  # Canonical winner of 101
        ("10201", "102", 1, "20240110", 62.0, "F", "US"),
        ("10301", "103", 1, "20240112", 70.0, "M", "US"),
        ("10401", "104", 1, "20240115", 34.0, "F", "CA"),
        ("10501", "105", 1, "20240120", 29.0, "M", "GB"),
    ]
    demo_cols = ["primaryid", "caseid", "caseversion", "fda_dt", "age", "sex", "occr_country"]

    drug_data = [
        # primaryid, drug_seq, role_cod, drugname, prod_ai
        ("10101", 1, "PS", "Lipitor", "ATORVASTATIN"),      # from superseded report
        ("10102", 1, "PS", "Lipitor", "ATORVASTATIN"),      # canonical
        ("10201", 1, "PS", "Atorvastatin", "ATORVASTATIN"), # canonical
        ("10301", 1, "PS", "Atorvastatin", "ATORVASTATIN"), # canonical
        ("10401", 1, "PS", "Amoxil", "AMOXICILLIN"),        # canonical
        ("10501", 1, "PS", "Amoxicillin", "AMOXICILLIN"),   # canonical
        ("99999", 1, "PS", "Prinivil", "LISINOPRIL"),       # orphan record
    ]
    drug_cols = ["primaryid", "drug_seq", "role_cod", "drugname", "prod_ai"]

    reac_data = [
        # primaryid, pt
        ("10101", "Myalgia"),          # superseded
        ("10102", "Rhabdomyolysis"),   # canonical
        ("10201", "Rhabdomyolysis"),   # canonical
        ("10301", "Rhabdomyolysis"),   # canonical
        ("10401", "Rash"),             # canonical
        ("10501", "Rash"),             # canonical
        ("99999", "Headache"),         # orphan
    ]
    reac_cols = ["primaryid", "pt"]

    outc_data = [
        # primaryid, outc_cod
        ("10102", "HO"),
        ("10201", "HO"),
        ("10301", "DE"),
        ("10401", "OT"),
        ("10501", "OT"),
    ]
    outc_cols = ["primaryid", "outc_cod"]

    return {
        "DEMO": spark.createDataFrame(demo_data, demo_cols),
        "DRUG": spark.createDataFrame(drug_data, drug_cols),
        "REAC": spark.createDataFrame(reac_data, reac_cols),
        "OUTC": spark.createDataFrame(outc_data, outc_cols),
    }
