"""
Read-only access to the Gold layer for the API and dashboard.
Uses PyArrow datasets with filter push-down, so lookups read only the relevant row groups instead of
loading multi-million-row tables. Nothing here recomputes statistics: every number served comes from
the Gold Parquet tables written by the pipeline.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from src.signal_detection.explain import explain_signal

POLICIES = ["primary", "all_suspect", "all_reported"]
SIGNAL_COLUMNS = [
    "drug_name", "reaction_term", "analysis_period", "role_policy", "observed_cases", "expected_cases", "background_cases",
    "a", "b", "c", "d", "n_eligible_cases", "ROR", "ROR_CI_lower", "ROR_CI_upper", "PRR", "PRR_CI_lower", "PRR_CI_upper",
    "chi2_yates", "zero_corrected", "screening_status", "screening_reasons", "method_version", "analysis_timestamp",
]
SORTABLE = {"ROR", "ROR_CI_lower", "PRR", "observed_cases", "chi2_yates", "drug_name", "reaction_term"}


def _records(table: pa.Table) -> List[Dict[str, Any]]:
    return table.to_pylist()


class GoldStore:

    def __init__(self, gold_root: Path, snapshot: str = "latest", reports_dir: Optional[Path] = None, silver_dir: Optional[Path] = None):
        self.base = Path(gold_root) / snapshot
        self.snapshot = snapshot
        self.reports_dir = Path(reports_dir) if reports_dir else None
        self.silver_dir = Path(silver_dir) if silver_dir else None
        self._metrics: Optional[Dict[str, Any]] = None
        if not (self.base / "gold_metrics.json").exists():
            raise FileNotFoundError(f"No Gold snapshot at {self.base}. Run scripts/run_gold_pipeline.py first.")

    # -- low level -----------------------------------------------------------------------------
    def _dataset(self, name: str) -> ds.Dataset:
        return ds.dataset(str(self.base / name), format="parquet")

    def exists(self, name: str) -> bool:
        return (self.base / name).exists()

    def table(self, name: str, columns: Optional[List[str]] = None, filter: Optional[pc.Expression] = None) -> pa.Table:
        return self._dataset(name).to_table(columns=columns, filter=filter)

    # -- overview ------------------------------------------------------------------------------
    def metrics(self) -> Dict[str, Any]:
        if self._metrics is None:
            self._metrics = json.loads((self.base / "gold_metrics.json").read_text(encoding="utf-8"))
        return self._metrics

    def silver_metrics(self) -> Dict[str, Any]:
        p = self.silver_dir / "silver_metrics.json" if self.silver_dir else None
        return json.loads(p.read_text(encoding="utf-8")) if p and p.exists() else {}

    def overview(self, role_policy: str = "primary") -> Dict[str, Any]:
        m, sm = self.metrics(), self.silver_metrics().get("tables", {})
        sig = m["signals"][role_policy]
        drug_records = sm.get("DRUG", {}).get("retained_rows")
        return {
            "snapshot": self.snapshot, "role_policy": role_policy,
            "total_report_records_processed": sm.get("DEMO", {}).get("raw_rows"),
            "unique_cases": m["canonical_cases"],
            "drug_records": drug_records,
            "distinct_drug_entities": m["distinct_drugs"],
            "distinct_reaction_terms": m["distinct_reactions"],
            "potential_signals": sig["potential_signals"],
            "drug_event_pairs_screened": sig["pairs"],
            "reporting_quarters": len(m["report_quarters"]),
            "quarters": m["report_quarters"],
            "eligible_cases_N": sig["eligible_cases_N"],
            "criteria": sig["criteria"],
        }

    def top_reactions(self, n: int = 20) -> List[Dict[str, Any]]:
        t = self.table("reaction_summary").sort_by([("cases", "descending")]).slice(0, n)
        return _records(t)

    def top_drugs(self, n: int = 20, role_policy: str = "primary") -> List[Dict[str, Any]]:
        col = {"primary": "cases_ps", "all_suspect": "cases_ps", "all_reported": "cases_any_role"}[role_policy]
        t = self.table("drug_summary", columns=["drug_key", col]).sort_by([(col, "descending")]).slice(0, n)
        return [{"drug_key": r["drug_key"], "cases": r[col]} for r in _records(t)]

    def top_pairs(self, n: int = 20, role_policy: str = "primary") -> List[Dict[str, Any]]:
        t = self.signals_table(role_policy).sort_by([("observed_cases", "descending")]).slice(0, n)
        return _records(t.select(["drug_name", "reaction_term", "observed_cases", "ROR", "screening_status"]))

    def outcomes(self) -> List[Dict[str, Any]]:
        return _records(self.table("outcome_summary").sort_by([("cases", "descending")]))

    def quarterly_trends(self) -> List[Dict[str, Any]]:
        return _records(self.table("quarterly_trends").sort_by("report_quarter"))

    def signal_status_counts(self, role_policy: str = "primary") -> List[Dict[str, Any]]:
        t = self.signals_table(role_policy, columns=["screening_status"])
        return [{"screening_status": k["values"], "pairs": k["counts"]} for k in pc.value_counts(t["screening_status"]).to_pylist()]

    # -- drugs ---------------------------------------------------------------------------------
    def search_drugs(self, query: str, limit: int = 25) -> List[Dict[str, Any]]:
        """Matches the normalized ingredient or any reported product name (case-insensitive substring)."""
        q = query.strip().upper()
        if not q:
            return []
        by_key = self.table("drug_summary", filter=pc.match_substring(pc.field("drug_key"), q))
        names = self.table("drug_name_index", filter=pc.match_substring(pc.field("reported_name"), q))
        keys = {r["drug_key"]: r["cases_any_role"] for r in _records(by_key.select(["drug_key", "cases_any_role"]))}
        matched_via: Dict[str, str] = {}
        for r in sorted(_records(names), key=lambda r: -r["cases"]):
            matched_via.setdefault(r["drug_key"], r["reported_name"])
        missing = [k for k in matched_via if k not in keys]
        if missing:
            extra = self.table("drug_summary", columns=["drug_key", "cases_any_role"], filter=pc.field("drug_key").isin(missing))
            keys.update({r["drug_key"]: r["cases_any_role"] for r in _records(extra)})
        out = [{"drug_key": k, "cases_any_role": n, "matched_reported_name": matched_via.get(k)} for k, n in keys.items()]
        exact_first = sorted(out, key=lambda r: (r["drug_key"] != q, -r["cases_any_role"]))
        return exact_first[:limit]

    def drug_detail(self, drug_key: str, role_policy: str = "primary") -> Optional[Dict[str, Any]]:
        key = drug_key.upper()
        summary = _records(self.table("drug_summary", filter=pc.field("drug_key") == key))
        if not summary:
            return None
        f = (pc.field("drug_key") == key) & (pc.field("role_policy") == role_policy)
        names = _records(self.table("drug_name_index", filter=pc.field("drug_key") == key).sort_by([("cases", "descending")]).slice(0, 25))
        quarterly = self.table("drug_quarterly", filter=f).sort_by("report_quarter")
        return {
            "drug_key": key, "role_policy": role_policy, "summary": summary[0],
            "reported_names": [n["reported_name"] for n in names],
            "reporting_period": [quarterly["report_quarter"][0].as_py(), quarterly["report_quarter"][-1].as_py()] if quarterly.num_rows else None,
            "role_distribution": {r: summary[0][f"cases_{r}"] for r in ("ps", "ss", "c", "i")},
            "outcomes": _records(self.table("drug_outcomes", filter=f).select(["outc_cod", "cases"]).sort_by([("cases", "descending")])),
            "sex": _records(self.table("drug_sex", filter=f).select(["sex_norm", "cases"]).sort_by([("cases", "descending")])),
            "age_bands": _records(self.table("drug_age", filter=f).select(["age_band", "cases"])),
            "quarterly_cases": _records(quarterly.select(["report_quarter", "cases"])),
        }

    # -- signals -------------------------------------------------------------------------------
    def _signal_columns(self, role_policy: str) -> List[str]:
        """The documented signal columns that exist in the stored table (role_policy is a partition, not a column)."""
        present = set(self._dataset(f"safety_signals/role_policy={role_policy}").schema.names)
        return [c for c in SIGNAL_COLUMNS if c in present]

    def signals_table(self, role_policy: str = "primary", columns: Optional[List[str]] = None, filter: Optional[pc.Expression] = None) -> pa.Table:
        return self.table(f"safety_signals/role_policy={role_policy}", columns=columns, filter=filter)

    def signals(self, role_policy: str = "primary", drug: Optional[str] = None, reaction: Optional[str] = None,
                status: Optional[str] = "POTENTIAL_SIGNAL", min_cases: int = 0, sort_by: str = "ROR_CI_lower",
                descending: bool = True, limit: int = 100, offset: int = 0, exact: bool = False) -> Dict[str, Any]:
        if sort_by not in SORTABLE:
            raise ValueError(f"sort_by must be one of {sorted(SORTABLE)}")
        expr = pc.field("observed_cases") >= min_cases
        if status:
            expr &= pc.field("screening_status") == status
        for field, value in (("drug_name", drug), ("reaction_term", reaction)):
            if value:
                v = value.strip().upper()
                expr &= (pc.field(field) == v) if exact else pc.match_substring(pc.field(field), v)
        t = self.signals_table(role_policy, columns=self._signal_columns(role_policy), filter=expr)
        total = t.num_rows
        t = t.sort_by([(sort_by, "descending" if descending else "ascending")]).slice(offset, limit)
        items = [{**r, "role_policy": role_policy} for r in _records(t)]
        return {"total": total, "offset": offset, "limit": limit, "items": items}

    def signal(self, drug: str, reaction: str, role_policy: str = "primary") -> Optional[Dict[str, Any]]:
        t = self.signals_table(role_policy, filter=(pc.field("drug_name") == drug.upper()) & (pc.field("reaction_term") == reaction.upper()))
        if t.num_rows == 0:
            return None
        row = _records(t)[0]
        row["role_policy"] = role_policy
        return {"signal": row, "explanation": explain_signal(row)}

    # -- temporal / combinations -----------------------------------------------------------------
    def temporal_signals(self, drug: str, reaction: Optional[str] = None) -> List[Dict[str, Any]]:
        if not self.exists("temporal/quarterly_signals"):
            return []
        expr = pc.field("drug_name") == drug.upper()
        if reaction:
            expr &= pc.field("reaction_term") == reaction.upper()
        t = self.table("temporal/quarterly_signals", filter=expr,
                       columns=["drug_name", "reaction_term", "analysis_period", "observed_cases", "ROR", "ROR_CI_lower", "ROR_CI_upper", "PRR", "screening_status"])
        return _records(t.sort_by([("analysis_period", "ascending"), ("observed_cases", "descending")]))

    def persistence(self, drug: Optional[str] = None, min_quarters_flagged: int = 1, limit: int = 200) -> List[Dict[str, Any]]:
        if not self.exists("temporal/signal_persistence"):
            return []
        expr = pc.field("quarters_flagged") >= min_quarters_flagged
        if drug:
            expr &= pc.field("drug_name") == drug.upper()
        t = self.table("temporal/signal_persistence", filter=expr)
        return _records(t.sort_by([("quarters_flagged", "descending"), ("total_observed_cases", "descending")]).slice(0, limit))

    def quarterly_reaction_counts(self, reaction: str) -> List[Dict[str, Any]]:
        if not self.exists("temporal/quarterly_reaction_counts"):
            return []
        t = self.table("temporal/quarterly_reaction_counts", filter=pc.field("pt") == reaction.upper())
        return _records(t.sort_by("report_quarter"))

    def combinations(self, drug: Optional[str] = None, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        if not self.exists("combinations/combination_signals"):
            return []
        expr = pc.scalar(True)
        if drug:
            d = drug.upper()
            expr &= (pc.field("drug_1") == d) | (pc.field("drug_2") == d)
        if status:
            expr &= pc.field("screening_status") == status
        t = self.table("combinations/combination_signals", filter=expr)
        return _records(t.sort_by([("ROR_CI_lower", "descending")]).slice(0, limit))

    def top_combinations(self, limit: int = 50) -> List[Dict[str, Any]]:
        if not self.exists("combinations/drug_combinations"):
            return []
        return _records(self.table("combinations/drug_combinations").sort_by([("pair_cases", "descending")]).slice(0, limit))

    # -- documentation of the run ------------------------------------------------------------------
    def data_quality(self) -> Dict[str, Any]:
        p = self.silver_dir / "data_quality_metrics.json" if self.silver_dir else None
        return json.loads(p.read_text(encoding="utf-8")) if p and p.exists() else {}

