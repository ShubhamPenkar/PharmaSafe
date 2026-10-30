"""
PharmaSafe Data Quality Auditor
Compiles reports/data_quality_report.md and data/silver/data_quality_metrics.json from the metrics
the pipelines write (Bronze ingestion summaries, silver_metrics.json, gold_metrics.json).
The verdict is computed from explicit checks; nothing is declared passed unless it was verified.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger
from src.utils.config_loader import load_data_sources_config, resolve_path

logger = get_logger("PharmaSafe.DataQuality")

KEY_COLUMNS = {
    "DEMO": ["age", "sex", "event_dt", "fda_dt", "occr_country", "caseversion"],
    "DRUG": ["drugname", "prod_ai", "role_cod", "route"],
    "REAC": ["pt"],
    "OUTC": ["outc_cod"],
}


def _load(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


class DataQualityAuditor:

    def __init__(self, config: Optional[Dict[str, Any]] = None, root: Optional[Path] = None):
        self.config = config or load_data_sources_config()
        storage = self.config.get("storage", {})
        self.bronze_dir = resolve_path(storage.get("bronze_dir", "data/bronze"))
        self.silver_dir = resolve_path(storage.get("silver_dir", "data/silver"))
        self.gold_dir = resolve_path(storage.get("gold_dir", "data/gold"))
        self.manifest_path = resolve_path(storage.get("manifest_path", "data/raw/manifest.json"))
        self.reports_dir = (root or resolve_path(".")) / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def collect(self, snapshot: str = "latest") -> Dict[str, Any]:
        bronze = {p.parent.name: _load(p) for p in sorted(self.bronze_dir.glob("*/ingestion_summary.json"))}
        silver = _load(self.silver_dir / "silver_metrics.json")
        gold = _load(self.gold_dir / snapshot / "gold_metrics.json")
        manifest = _load(self.manifest_path)

        checks: List[Dict[str, Any]] = []

        def check(name: str, ok: bool, detail: str) -> None:
            checks.append({"check": name, "passed": bool(ok), "detail": detail})

        for q, b in bronze.items():
            bad = [t for t, m in b.get("tables", {}).items() if m.get("status") == "INGESTED" and m.get("reconciled") is False]
            check(f"Bronze {q}: source records == ingested + quarantined", not bad, f"failing tables: {bad}" if bad else "all tables reconcile")
            check(f"Bronze {q}: official provenance", b.get("verification_status") == "VERIFIED_OFFICIAL" and not b.get("is_synthetic"),
                  b.get("verification_status", "unknown"))
        for table, m in silver.get("tables", {}).items():
            if table == "DEMO":
                ok = m["raw_rows"] == m["retained_report_versions"] + m["excluded_rows"]
            else:
                ok = m.get("raw_rows") == m.get("retained_rows", 0) + m.get("excluded_rows", 0)
            check(f"Silver {table}: raw == retained + excluded", ok, f"raw {m.get('raw_rows'):,}" if m.get("raw_rows") is not None else "n/a")
        if gold and silver:
            demo = silver["tables"]["DEMO"]
            check("Gold canonical cases == Silver latest canonical cases (latest snapshot)",
                  snapshot != "latest" or gold["canonical_cases"] == demo["canonical_cases_latest"],
                  f"gold {gold['canonical_cases']:,} vs silver {demo['canonical_cases_latest']:,}")

        return {
            "audit_timestamp": datetime.now(timezone.utc).isoformat(), "snapshot": snapshot,
            "manifest_archives": manifest.get("archives", {}), "bronze": bronze, "silver": silver, "gold": gold,
            "checks": checks, "all_checks_passed": bool(checks) and all(c["passed"] for c in checks),
        }

    def generate_report(self, snapshot: str = "latest") -> Dict[str, Any]:
        data = self.collect(snapshot)
        (self.silver_dir).mkdir(parents=True, exist_ok=True)
        (self.silver_dir / "data_quality_metrics.json").write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        (self.reports_dir / "data_quality_report.md").write_text(self.render(data), encoding="utf-8")
        logger.info("Data quality report written (%d checks, all passed: %s)", len(data["checks"]), data["all_checks_passed"])
        return data

    @staticmethod
    def render(d: Dict[str, Any]) -> str:
        L: List[str] = [
            "# PharmaSafe Data Quality & Pipeline Audit",
            f"Generated: {d['audit_timestamp']}  |  Gold snapshot: `{d['snapshot']}`\n",
            f"**Verdict:** {'ALL CHECKS PASSED' if d['all_checks_passed'] else 'CHECKS FAILED OR INCOMPLETE — see section 6'} "
            f"({sum(c['passed'] for c in d['checks'])}/{len(d['checks'])})\n",
            "## 1. Source archives\n",
            "| Quarter | File | Size (MB) | SHA-256 (prefix) | Status |", "| :--- | :--- | ---: | :--- | :--- |",
        ]
        for q, a in d["manifest_archives"].items():
            L.append(f"| {q} | `{a.get('filename')}` | {a.get('file_size_mb')} | `{a.get('sha256_checksum', '')[:16]}…` | {a.get('verification_status')} |")

        L += ["", "## 2. Bronze ingestion (rows are table records, not cases)\n",
              "| Quarter | Table | Source records | Ingested | Quarantined | Reconciled |", "| :--- | :--- | ---: | ---: | ---: | :---: |"]
        for q, b in d["bronze"].items():
            for t, m in b.get("tables", {}).items():
                if m.get("status") == "INGESTED":
                    L.append(f"| {q} | {t} | {m.get('source_records', 0):,} | {m['rows_ingested']:,} | "
                             f"{m.get('rows_quarantined', 0):,} | {'yes' if m.get('reconciled', True) else '**NO**'} |")

        demo = d["silver"].get("tables", {}).get("DEMO", {})
        if demo:
            L += ["", "## 3. Case version handling (DEMO)\n", "| Measure | Count |", "| :--- | ---: |",
                  f"| Raw DEMO report rows | {demo['raw_rows']:,} |",
                  f"| Excluded rows | {demo['excluded_rows']:,} |"]
            for reason, n in sorted(demo.get("excluded_by_reason", {}).items()):
                L.append(f"| &nbsp;&nbsp;{reason} | {n:,} |")
            L += [f"| Report versions retained | {demo['retained_report_versions']:,} |",
                  f"| Superseded versions | {demo['superseded_versions']:,} |",
                  f"| Versions never canonical at any quarter end | {demo['versions_never_canonical']:,} |",
                  f"| Versions with missing/invalid caseversion | {demo['versions_missing_caseversion']:,} |",
                  f"| Cases removed by FDA deletion lists | {demo['cases_removed_by_fda_deletion']:,} |",
                  f"| **Canonical cases (latest view)** | **{demo['canonical_cases_latest']:,}** |",
                  "", f"Ranking rule: {d['silver'].get('deduplication_policy')}."]

        L += ["", "## 4. Relational integrity of child tables\n",
              "| Table | Raw rows | Retained | Excluded | Retention | Exclusion reasons |", "| :--- | ---: | ---: | ---: | ---: | :--- |"]
        for t, m in d["silver"].get("tables", {}).items():
            if t == "DEMO" or "raw_rows" not in m:
                continue
            reasons = "; ".join(f"{k}: {v:,}" for k, v in sorted(m.get("excluded_by_reason", {}).items())) or "—"
            L.append(f"| {t} | {m['raw_rows']:,} | {m['retained_rows']:,} | {m['excluded_rows']:,} | {m['retention_pct']}% | {reasons} |")
        L.append("\nSuperseded case versions are *not* orphans: their child rows stay in Silver and are excluded from Gold by version.")

        L += ["", "## 5. Missing values in key fields (% of retained rows)\n", "| Table | Field | Missing % |", "| :--- | :--- | ---: |"]
        for t, cols in KEY_COLUMNS.items():
            prof = d["silver"].get("tables", {}).get(t, {}).get("missing_pct", {})
            for c in cols:
                if c in prof:
                    L.append(f"| {t} | {c} | {prof[c]} |")

        g = d.get("gold")
        if g:
            L += ["", "## Gold population\n", f"- Canonical cases: **{g['canonical_cases']:,}**; drug ingredients/names: {g['distinct_drugs']:,}; "
                  f"reaction terms: {g['distinct_reactions']:,}; report quarters: {', '.join(g['report_quarters'])}"]
            for p, s in g.get("signals", {}).items():
                L.append(f"- Policy `{p}` (roles {', '.join(s['target_roles'])}): N = {s['eligible_cases_N']:,}, "
                         f"pairs stored {s['pairs']:,}, potential signals {s['potential_signals']:,}")

        L += ["", "## 6. Verification checks\n", "| Check | Result | Detail |", "| :--- | :---: | :--- |"]
        for c in d["checks"]:
            L.append(f"| {c['check']} | {'pass' if c['passed'] else '**FAIL**'} | {c['detail']} |")
        return "\n".join(L) + "\n"
