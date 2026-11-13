"""
PharmaSafe performance evaluation. Reports measured numbers only; nothing is projected or estimated.

1. Stage timings recorded by scripts/run_full_pipeline.py (reports/performance_stages.json).
2. Pandas vs PySpark on an identical workload (drug-event 2x2 counts for the primary-suspect policy)
   over the same Gold Parquet inputs. Each engine runs in a fresh subprocess so peak RSS is not shared.
   Spark runs locally (local[*]) on one machine; this is not a multi-node cluster measurement.

    python -m src.validation.performance_benchmark [--repeats 3] [--snapshot latest]
"""

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _workload_pandas(base: Path) -> Dict[str, Any]:
    import pandas as pd
    drug = pd.read_parquet(base / "case_drug", columns=["caseid", "drug_key", "is_ps"])
    reac = pd.read_parquet(base / "case_reaction", columns=["caseid", "pt"])
    drug = drug[drug["is_ps"]][["caseid", "drug_key"]].drop_duplicates()
    reac = reac.drop_duplicates()
    eligible = set(drug["caseid"]) & set(reac["caseid"])
    drug, reac = drug[drug["caseid"].isin(eligible)], reac[reac["caseid"].isin(eligible)]
    n = len(eligible)
    pairs = drug.merge(reac, on="caseid").groupby(["drug_key", "pt"], sort=False).size().rename("a").reset_index()
    n_drug = drug.groupby("drug_key").size().rename("n_drug").reset_index()
    n_event = reac.groupby("pt").size().rename("n_event").reset_index()
    out = pairs.merge(n_drug, on="drug_key").merge(n_event, on="pt")
    out["b"], out["c"] = out["n_drug"] - out["a"], out["n_event"] - out["a"]
    out["d"] = n - out["n_drug"] - out["n_event"] + out["a"]
    kept = out[out["a"] >= 3]
    return {"eligible_cases": n, "pairs_all": len(out), "pairs_a_ge_3": len(kept), "sum_a": int(kept["a"].sum())}


def _workload_spark(base: Path) -> Dict[str, Any]:
    from pyspark.sql.functions import col, count, lit, sum as spark_sum
    from src.utils.spark_session import get_spark_session
    spark = get_spark_session(app_name="PharmaSafe.Benchmark")
    drug = spark.read.parquet(str(base / "case_drug")).filter("is_ps").select("caseid", "drug_key").distinct()
    reac = spark.read.parquet(str(base / "case_reaction")).select("caseid", "pt").distinct()
    elig = drug.select("caseid").distinct().join(reac.select("caseid").distinct(), "caseid")
    n = elig.count()
    drug, reac = drug.join(elig, "caseid"), reac.join(elig, "caseid")
    pairs = drug.join(reac, "caseid").groupBy("drug_key", "pt").agg(count(lit(1)).alias("a"))
    n_drug = drug.groupBy("drug_key").agg(count(lit(1)).alias("n_drug"))
    n_event = reac.groupBy("pt").agg(count(lit(1)).alias("n_event"))
    out = (pairs.join(n_drug, "drug_key").join(n_event, "pt")
           .withColumn("b", col("n_drug") - col("a")).withColumn("c", col("n_event") - col("a"))
           .withColumn("d", lit(n) - col("n_drug") - col("n_event") + col("a")))
    kept = out.filter(col("a") >= 3)
    row = kept.agg(count(lit(1)).alias("k"), spark_sum("a").alias("s")).collect()[0]
    all_pairs = out.count()
    spark.stop()
    return {"eligible_cases": n, "pairs_all": all_pairs, "pairs_a_ge_3": row["k"], "sum_a": int(row["s"])}


def child_main(engine: str, base: str) -> None:
    start = time.perf_counter()
    result = (_workload_pandas if engine == "pandas" else _workload_spark)(Path(base))
    print("BENCH_RESULT " + json.dumps({"engine": engine, "seconds": time.perf_counter() - start, "result": result}))


def _tree_rss_mb(root_pid: int) -> float:
    """Sum of resident memory (MB) of a process and all its descendants, from one `ps` snapshot."""
    out = subprocess.run(["ps", "-axo", "pid=,ppid=,rss="], capture_output=True, text=True).stdout
    rows = [tuple(int(x) for x in line.split()) for line in out.splitlines() if line.strip()]
    children: Dict[int, List[int]] = {}
    rss = {}
    for pid, ppid, kb in rows:
        children.setdefault(ppid, []).append(pid)
        rss[pid] = kb
    total, stack = 0, [root_pid]
    while stack:
        pid = stack.pop()
        total += rss.get(pid, 0)
        stack.extend(children.get(pid, []))
    return total / 1024


def run_engine(engine: str, base: Path) -> Dict[str, Any]:
    """Runs one engine in a fresh process; peak memory is the highest sampled RSS of its whole process tree (incl. the JVM)."""
    proc = subprocess.Popen([sys.executable, "-m", "src.validation.performance_benchmark", "--child", engine, "--base", str(base)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=PROJECT_ROOT)
    peak = 0.0
    while proc.poll() is None:
        peak = max(peak, _tree_rss_mb(proc.pid))
        time.sleep(0.1)
    stdout, stderr = proc.communicate()
    line = next((l for l in stdout.splitlines() if l.startswith("BENCH_RESULT ")), None)
    if proc.returncode != 0 or line is None:
        raise RuntimeError(f"{engine} benchmark failed:\n{stderr[-2000:]}")
    data = json.loads(line[len("BENCH_RESULT "):])
    data["peak_rss_mb"] = round(peak, 1)
    return data


def dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main(snapshot: str = "latest", repeats: int = 3, gold_dir: str = None) -> Dict[str, Any]:
    from src.utils.config_loader import load_data_sources_config, resolve_path
    storage = load_data_sources_config()["storage"]
    base = resolve_path(gold_dir or storage["gold_dir"]) / snapshot
    import pyarrow.dataset as ds
    inputs = {n: {"rows": ds.dataset(str(base / n), format="parquet").count_rows(), "bytes": dir_size(base / n)} for n in ("case_drug", "case_reaction")}

    runs: Dict[str, List[Dict[str, Any]]] = {"pandas": [], "spark": []}
    for _ in range(repeats):
        for engine in runs:
            runs[engine].append(run_engine(engine, base))
    summary = {}
    for engine, rs in runs.items():
        secs = [r["seconds"] for r in rs]
        summary[engine] = {"seconds_each": [round(s, 2) for s in secs], "median_seconds": round(median(secs), 2),
                           "peak_rss_mb": max(r["peak_rss_mb"] for r in rs), "result": rs[0]["result"]}
    summary["results_match"] = summary["pandas"]["result"] == summary["spark"]["result"]

    stages_path = resolve_path("reports/performance_stages.json")
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "snapshot": snapshot,
        "machine": {"platform": platform.platform(), "cpu_count": os.cpu_count(), "python": platform.python_version()},
        "spark_mode": "local[*] on a single machine (not a multi-node cluster)",
        "inputs": inputs, "comparison": summary, "repeats": repeats,
        "stages": json.loads(stages_path.read_text()) if stages_path.exists() else None,
    }
    out = resolve_path("reports")
    (out / "performance_summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (out / "performance_report.md").write_text(render(report), encoding="utf-8")
    return report


def render(r: Dict[str, Any]) -> str:
    c = r["comparison"]
    L = ["# PharmaSafe Performance Report", f"Generated: {r['generated_at']}  |  Snapshot `{r['snapshot']}`\n",
         f"Machine: {r['machine']['platform']}, {r['machine']['cpu_count']} logical CPUs, Python {r['machine']['python']}. "
         f"Spark: {r['spark_mode']}.\n"]
    if r.get("stages"):
        L += ["## 1. Pipeline stage timings (this machine, measured)\n", "| Stage | Input rows | Output rows | Seconds |", "| :--- | ---: | ---: | ---: |"]
        for s in r["stages"]["stages"]:
            L.append(f"| {s['stage']} | {s.get('input_rows', '—'):,} | {s.get('output_rows', '—'):,} | {s['seconds']} |" if isinstance(s.get("input_rows"), int) and isinstance(s.get("output_rows"), int)
                     else f"| {s['stage']} | {s.get('input_rows', '—')} | {s.get('output_rows', '—')} | {s['seconds']} |")
        L.append("")
    L += ["## 2. Pandas vs PySpark: drug-event 2x2 counts (primary-suspect policy)\n",
          "Same inputs, same logic, each engine in a fresh process, wall time includes reading Parquet and (for Spark) session start-up.\n",
          "| Input | Rows | Parquet bytes |", "| :--- | ---: | ---: |",
          *[f"| {n} | {v['rows']:,} | {v['bytes']:,} |" for n, v in r["inputs"].items()], "",
          f"| Engine | Median s ({r['repeats']} runs) | All runs (s) | Peak memory, process tree incl. JVM (MB, sampled every 0.1 s) |", "| :--- | ---: | :--- | ---: |",
          *[f"| {e} | {c[e]['median_seconds']} | {c[e]['seconds_each']} | {c[e]['peak_rss_mb']} |" for e in ("pandas", "spark")], "",
          f"Both engines produced identical results: **{c['results_match']}** (`{c['pandas']['result']}`).\n",
          "Interpretation: on one machine and data that fits in memory, a single-process engine can be competitive or faster; "
          "Spark's value here is that the same code scales past memory and across cores or nodes. No speed-up claim is made beyond these measurements."]
    return "\n".join(L) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", choices=["pandas", "spark"])
    parser.add_argument("--base")
    parser.add_argument("--snapshot", default="latest")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--gold-dir", help="Gold root (default: configured gold_dir)")
    args = parser.parse_args()
    if args.child:
        child_main(args.child, args.base)
    else:
        main(args.snapshot, args.repeats, args.gold_dir)
