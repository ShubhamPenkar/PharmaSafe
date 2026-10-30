"""
PharmaSafe Silver Layer Preprocessing Pipeline
Cleans, standardizes, validates relational integrity, and deduplicates FAERS data.
Satisfies the Critical Temporal Requirement:
- Latest-version snapshot for cross-sectional analysis across the entire observation period.
- Time-aware historical quarterly snapshots (with explicit quarter cutoff dates)
  ensuring later follow-up reports do not retroactively rewrite historical quarterly data.
- Quarantines orphan child records and exact duplicate rows.
- Saves analytics-ready tables as Parquet under data/silver/.
"""

import os
import sys
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_data_sources_config, get_project_root

logger = get_logger("PharmaSafe.Silver")


def parse_fda_date_safe(date_str: Any) -> Optional[str]:
    """
    Safely parses an FDA date string (expected format YYYYMMDD or partial YYYYMM).
    Returns normalized YYYYMMDD string or None if missing, malformed, or unparseable.
    Never invents or guesses date values.
    """
    if pd.isna(date_str) or date_str is None:
        return None
    s = str(date_str).strip()
    # Check if purely 8 digits and reasonable year (1900-2100)
    if re.match(r"^(19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$", s):
        return s
    # Partial year-month (YYYYMM): preserve as YYYYMM
    if re.match(r"^(19|20)\d{2}(0[1-9]|1[0-2])$", s):
        return s
    return None


class SilverPipeline:
    """
    Executes Silver layer standardization, relational validation, deterministic deduplication,
    orphan quarantining, and temporal quarterly snapshots.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or load_data_sources_config()
        self.root = get_project_root()
        self.bronze_dir = self.root / self.config.get("storage", {}).get("bronze_dir", "data/bronze")
        self.silver_dir = self.root / self.config.get("storage", {}).get("silver_dir", "data/silver")
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir = self.silver_dir / "quarantine"
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

    def load_bronze_table(self, table_name: str, quarters: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Loads and concatenates a Bronze Parquet table across specified or all available quarters.
        """
        dfs = []
        pattern = f"*/{table_name.lower()}.parquet"
        found_files = list(self.bronze_dir.glob(pattern))

        if not found_files:
            logger.warning("No Bronze files found for table %s in %s", table_name, self.bronze_dir)
            return pd.DataFrame()

        for f in found_files:
            q_dir = f.parent.name
            if quarters and q_dir.upper() not in [q.upper() for q in quarters]:
                continue
            try:
                df = pd.read_parquet(f)
                dfs.append(df)
            except Exception as e:
                logger.error("Failed reading Bronze parquet %s: %s", f, e)

        if not dfs:
            return pd.DataFrame()
        combined = pd.concat(dfs, ignore_index=True)
        logger.info("Loaded Bronze table %s: %d total rows from %d files", table_name, len(combined), len(dfs))
        return combined

    @staticmethod
    def deduplicate_demo(
        demo_df: pd.DataFrame,
        cutoff_date: Optional[str] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """
        Executes deterministic case deduplication on DEMO records:
        1. Optionally filter by cutoff_date (fda_dt <= cutoff_date) for historical snapshots.
        2. Profile exact duplicate rows (identical in all user columns) and remove them.
        3. For each caseid, select canonical record using:
           - max caseversion (integer)
           - tiebreaker 1: max fda_dt
           - tiebreaker 2: max primaryid (deterministic string/int comparison)
        Returns:
            (canonical_df, superseded_and_duplicate_df, metrics)
        """
        initial_count = len(demo_df)
        if initial_count == 0:
            return pd.DataFrame(), pd.DataFrame(), {}

        df = demo_df.copy()
        df["_parsed_fda_dt"] = df["fda_dt"].apply(parse_fda_date_safe)

        # Apply time-aware cutoff if specified
        if cutoff_date:
            df = df[df["_parsed_fda_dt"].fillna("").le(cutoff_date)].copy()
        
        post_cutoff_count = len(df)

        # Profile and isolate exact duplicates
        cols_for_exact_match = [c for c in df.columns if not c.startswith("_")]
        exact_dups_mask = df.duplicated(subset=cols_for_exact_match, keep="first")
        exact_duplicate_rows = df[exact_dups_mask].copy()
        df_no_exact_dups = df[~exact_dups_mask].copy()
        exact_duplicates_count = len(exact_duplicate_rows)

        # Convert version to numeric for proper sort comparison
        df_no_exact_dups["_caseversion_num"] = pd.to_numeric(df_no_exact_dups["caseversion"], errors="coerce").fillna(1)
        df_no_exact_dups["_fda_dt_str"] = df_no_exact_dups["_parsed_fda_dt"].fillna("")
        df_no_exact_dups["_primaryid_str"] = df_no_exact_dups["primaryid"].astype(str)

        # Sort deterministically
        df_sorted = df_no_exact_dups.sort_values(
            by=["caseid", "_caseversion_num", "_fda_dt_str", "_primaryid_str"],
            ascending=[True, False, False, False]
        )

        # Canonical records: first per caseid
        canonical_mask = ~df_sorted.duplicated(subset=["caseid"], keep="first")
        canonical_df = df_sorted[canonical_mask].copy()
        superseded_df = df_sorted[~canonical_mask].copy()

        # Clean helper columns
        canonical_df.drop(columns=["_caseversion_num", "_fda_dt_str", "_primaryid_str"], inplace=True, errors="ignore")
        superseded_df.drop(columns=["_caseversion_num", "_fda_dt_str", "_primaryid_str"], inplace=True, errors="ignore")

        canonical_count = len(canonical_df)
        superseded_count = len(superseded_df)
        total_unique_cases = canonical_df["caseid"].nunique()

        metrics = {
            "initial_demo_rows": initial_count,
            "post_cutoff_rows": post_cutoff_count,
            "exact_duplicate_rows": exact_duplicates_count,
            "superseded_versions_count": superseded_count,
            "canonical_case_count": canonical_count,
            "unique_case_ids": total_unique_cases,
            "case_version_reduction_pct": round(((superseded_count + exact_duplicates_count) / post_cutoff_count * 100.0), 2) if post_cutoff_count > 0 else 0.0
        }

        # Combine superseded and exact duplicates for auditable logging
        all_excluded = pd.concat([exact_duplicate_rows, superseded_df], ignore_index=True)
        return canonical_df, all_excluded, metrics

    def process_silver_snapshot(
        self,
        quarters: Optional[List[str]] = None,
        cutoff_date: Optional[str] = None,
        snapshot_name: str = "latest"
    ) -> Dict[str, Any]:
        """
        Executes complete Silver processing for a given temporal scope (e.g. 'latest' or 'historical_2024Q1').
        """
        logger.info("Executing Silver processing for snapshot '%s' (cutoff: %s)...", snapshot_name, cutoff_date)
        snapshot_dir = self.silver_dir / snapshot_name
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        quarantine_dir = self.quarantine_dir / snapshot_name
        quarantine_dir.mkdir(parents=True, exist_ok=True)

        # 1. DEMO
        raw_demo = self.load_bronze_table("DEMO", quarters=quarters)
        if raw_demo.empty:
            logger.error("No DEMO records found to process.")
            return {"status": "FAILED_NO_DEMO"}

        canonical_demo, excluded_demo, demo_metrics = self.deduplicate_demo(raw_demo, cutoff_date=cutoff_date)
        canonical_primaryids = set(canonical_demo["primaryid"].astype(str))

        # Save canonical DEMO
        demo_out_path = snapshot_dir / "demo.parquet"
        canonical_demo.to_parquet(demo_out_path, index=False, engine="pyarrow")
        if not excluded_demo.empty:
            excluded_demo.to_parquet(quarantine_dir / "demo_superseded_and_duplicates.parquet", index=False, engine="pyarrow")

        # 2. DRUG
        raw_drug = self.load_bronze_table("DRUG", quarters=quarters)
        drug_metrics = {}
        if not raw_drug.empty:
            raw_drug["_primaryid_str"] = raw_drug["primaryid"].astype(str)
            # Link check: is primaryid in canonical DEMO?
            canonical_drug_mask = raw_drug["_primaryid_str"].isin(canonical_primaryids)
            canonical_drug = raw_drug[canonical_drug_mask].copy()
            orphan_drug = raw_drug[~canonical_drug_mask].copy()

            # Normalization: preserve original drugname and create clean_drug_name
            canonical_drug["drugname_verbatim"] = canonical_drug["drugname"]
            canonical_drug["clean_drug_name"] = (
                canonical_drug["prod_ai"].fillna("").str.strip().str.upper()
            )
            # If prod_ai is empty, fallback to clean drugname
            mask_empty_prod_ai = canonical_drug["clean_drug_name"] == ""
            canonical_drug.loc[mask_empty_prod_ai, "clean_drug_name"] = (
                canonical_drug.loc[mask_empty_prod_ai, "drugname"].fillna("").str.strip().str.upper()
            )
            canonical_drug["clean_role_cod"] = canonical_drug["role_cod"].fillna("UNKNOWN").str.strip().str.upper()

            canonical_drug.drop(columns=["_primaryid_str"], inplace=True, errors="ignore")
            orphan_drug.drop(columns=["_primaryid_str"], inplace=True, errors="ignore")

            canonical_drug.to_parquet(snapshot_dir / "drug.parquet", index=False, engine="pyarrow")
            if not orphan_drug.empty:
                orphan_drug["quarantine_reason"] = "ORPHAN_CHILD_NO_CANONICAL_DEMO"
                orphan_drug.to_parquet(quarantine_dir / "drug_orphans.parquet", index=False, engine="pyarrow")

            drug_metrics = {
                "drug_raw_count": len(raw_drug),
                "drug_retained_count": len(canonical_drug),
                "drug_orphans_quarantined": len(orphan_drug),
                "drug_retention_pct": round(len(canonical_drug) / len(raw_drug) * 100.0, 2) if len(raw_drug) > 0 else 0.0
            }

        # 3. REAC
        raw_reac = self.load_bronze_table("REAC", quarters=quarters)
        reac_metrics = {}
        if not raw_reac.empty:
            raw_reac["_primaryid_str"] = raw_reac["primaryid"].astype(str)
            canonical_reac_mask = raw_reac["_primaryid_str"].isin(canonical_primaryids)
            canonical_reac = raw_reac[canonical_reac_mask].copy()
            orphan_reac = raw_reac[~canonical_reac_mask].copy()

            # Normalization
            canonical_reac["pt_verbatim"] = canonical_reac["pt"]
            canonical_reac["clean_pt"] = canonical_reac["pt"].fillna("").str.strip().str.upper()

            canonical_reac.drop(columns=["_primaryid_str"], inplace=True, errors="ignore")
            orphan_reac.drop(columns=["_primaryid_str"], inplace=True, errors="ignore")

            canonical_reac.to_parquet(snapshot_dir / "reac.parquet", index=False, engine="pyarrow")
            if not orphan_reac.empty:
                orphan_reac["quarantine_reason"] = "ORPHAN_CHILD_NO_CANONICAL_DEMO"
                orphan_reac.to_parquet(quarantine_dir / "reac_orphans.parquet", index=False, engine="pyarrow")

            reac_metrics = {
                "reac_raw_count": len(raw_reac),
                "reac_retained_count": len(canonical_reac),
                "reac_orphans_quarantined": len(orphan_reac),
                "reac_retention_pct": round(len(canonical_reac) / len(raw_reac) * 100.0, 2) if len(raw_reac) > 0 else 0.0
            }

        # 4. OUTC
        raw_outc = self.load_bronze_table("OUTC", quarters=quarters)
        outc_metrics = {}
        if not raw_outc.empty:
            raw_outc["_primaryid_str"] = raw_outc["primaryid"].astype(str)
            canonical_outc_mask = raw_outc["_primaryid_str"].isin(canonical_primaryids)
            canonical_outc = raw_outc[canonical_outc_mask].copy()
            orphan_outc = raw_outc[~canonical_outc_mask].copy()

            canonical_outc["clean_outc_cod"] = canonical_outc["outc_cod"].fillna("UNKNOWN").str.strip().str.upper()
            canonical_outc.drop(columns=["_primaryid_str"], inplace=True, errors="ignore")
            orphan_outc.drop(columns=["_primaryid_str"], inplace=True, errors="ignore")

            canonical_outc.to_parquet(snapshot_dir / "outc.parquet", index=False, engine="pyarrow")
            if not orphan_outc.empty:
                orphan_outc["quarantine_reason"] = "ORPHAN_CHILD_NO_CANONICAL_DEMO"
                orphan_outc.to_parquet(quarantine_dir / "outc_orphans.parquet", index=False, engine="pyarrow")

            outc_metrics = {
                "outc_raw_count": len(raw_outc),
                "outc_retained_count": len(canonical_outc),
                "outc_orphans_quarantined": len(orphan_outc),
                "outc_retention_pct": round(len(canonical_outc) / len(raw_outc) * 100.0, 2) if len(raw_outc) > 0 else 0.0
            }

        # Save snapshot metrics
        snapshot_metrics = {
            "snapshot_name": snapshot_name,
            "cutoff_date": cutoff_date,
            "processed_timestamp": datetime.now(timezone.utc).isoformat(),
            "demo": demo_metrics,
            "drug": drug_metrics,
            "reac": reac_metrics,
            "outc": outc_metrics
        }

        with open(snapshot_dir / "silver_metrics.json", "w", encoding="utf-8") as smf:
            json.dump(snapshot_metrics, smf, indent=2)

        logger.info("Silver snapshot '%s' complete. Retained %d unique canonical cases.", snapshot_name, demo_metrics.get("canonical_case_count", 0))
        return snapshot_metrics
