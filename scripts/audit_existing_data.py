import zipfile
import hashlib
import json
from pathlib import Path
import pandas as pd

raw_dir = Path("data/raw")
q1_zip = raw_dir / "faers_ascii_2024q1.zip"
q2_zip = raw_dir / "faers_ascii_2024q2.zip"
manifest_path = raw_dir / "manifest.json"

audit_findings = {
    "raw_archives": {},
    "manifest": {},
    "bronze_datasets": {},
    "silver_datasets": {}
}

if manifest_path.exists():
    with open(manifest_path, "r", encoding="utf-8") as f:
        audit_findings["manifest"] = json.load(f)

for p in [q1_zip, q2_zip]:
    if p.exists():
        data = p.read_bytes()
        sha256 = hashlib.sha256(data).hexdigest()
        info = {
            "path": str(p),
            "size_bytes": len(data),
            "sha256": sha256,
            "members": {},
            "synthetic_markers_found": []
        }
        with zipfile.ZipFile(p, "r") as zf:
            for name in zf.namelist():
                content = zf.read(name).decode("utf-8", errors="replace")
                lines = content.strip().splitlines()
                header = lines[0] if lines else ""
                row_count = len(lines) - 1 if len(lines) > 1 else 0
                markers = [m for m in ["SYNTH_DRUG_ALPHA", "SYNTH_DRUG_BETA", "SYNTH_EVENT_TACHY", "BENCH_STATIN"] if m in content]
                if markers:
                    info["synthetic_markers_found"].extend(markers)
                info["members"][name] = {
                    "lines": len(lines),
                    "row_count": row_count,
                    "header": header,
                    "markers": markers
                }
        info["synthetic_markers_found"] = list(set(info["synthetic_markers_found"]))
        info["classification"] = "SYNTHETIC" if info["synthetic_markers_found"] else "UNKNOWN"
        audit_findings["raw_archives"][p.name] = info

# Audit Bronze
bronze_dir = Path("data/bronze")
if bronze_dir.exists():
    for q_dir in bronze_dir.iterdir():
        if q_dir.is_dir():
            q_name = q_dir.name
            audit_findings["bronze_datasets"][q_name] = {}
            for pf in q_dir.glob("*.parquet"):
                try:
                    df = pd.read_parquet(pf)
                    has_synth = any(df.astype(str).apply(lambda col: col.str.contains("SYNTH_").any()))
                    audit_findings["bronze_datasets"][q_name][pf.name] = {
                        "rows": len(df),
                        "columns": df.columns.tolist(),
                        "contains_synthetic_markers": bool(has_synth)
                    }
                except Exception as e:
                    audit_findings["bronze_datasets"][q_name][pf.name] = {"error": str(e)}

# Audit Silver
silver_dir = Path("data/silver")
if silver_dir.exists():
    for s_dir in silver_dir.iterdir():
        if s_dir.is_dir() and s_dir.name != "quarantine":
            s_name = s_dir.name
            audit_findings["silver_datasets"][s_name] = {}
            for pf in s_dir.glob("*.parquet"):
                try:
                    df = pd.read_parquet(pf)
                    has_synth = any(df.astype(str).apply(lambda col: col.str.contains("SYNTH_").any()))
                    audit_findings["silver_datasets"][s_name][pf.name] = {
                        "rows": len(df),
                        "columns": df.columns.tolist(),
                        "contains_synthetic_markers": bool(has_synth)
                    }
                except Exception as e:
                    audit_findings["silver_datasets"][s_name][pf.name] = {"error": str(e)}

print(json.dumps(audit_findings, indent=2))
with open("data_audit_raw_result.json", "w", encoding="utf-8") as out:
    json.dump(audit_findings, out, indent=2)
