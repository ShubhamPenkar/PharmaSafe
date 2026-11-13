# PharmaSafe Architecture

```
FDA AEMS / FAERS quarterly ZIPs
   |  scripts/acquire_and_verify_official_data.py   (SHA-256 + CRC, data/raw/manifest.json)
   v
data/raw/official/<QUARTER>/*.zip        original archives, never modified
   |  scripts/run_bronze_pipeline.py     provenance gate, checksum re-check, Spark parse
   v
data/bronze/<QUARTER>/<table>/           all source columns as strings + provenance columns
   |                                     quarantine/<table>/ (malformed rows), ingestion_summary.json
   |  scripts/run_silver_pipeline.py     typing, normalization, duplicate + version resolution
   v
data/silver/                             demo_versions (validity intervals), demo (latest), 6 child tables,
   |                                     deleted_cases, quarantine/<table>/, silver_metrics.json
   |  scripts/run_gold_pipeline.py       case-level marts, 2x2 counts, ROR/PRR/screening
   v
data/gold/<snapshot>/                    case_summary, case_drug, case_reaction, case_outcome, drug_summary,
   |                                     reaction_summary, drug_name_index, drug_{outcomes,sex,age,quarterly},
   |                                     outcome_summary, quarterly_trends, drug_event_counts/, safety_signals/,
   |                                     temporal/ (quarterly_signals, signal_persistence, ...),
   |                                     combinations/ (drug_combinations, combination_signals)
   |
   +--> src/validation/   reference signals, robustness, temporal validation, quality + performance reports
   +--> src/serving/gold_store.py   read-only PyArrow access (filter push-down)
          +--> api/main.py          FastAPI (uvicorn api.main:app)
          +--> dashboard/app.py     Streamlit (streamlit run dashboard/app.py)
```

## Layer contracts
| Layer | Guarantee |
| :--- | :--- |
| Bronze | Every source record is either written or quarantined; the count is reconciled against an independent line count and the run fails otherwise. Re-running a completed quarter is a no-op unless `--force`. |
| Silver | `raw = retained + excluded` per table (enforced). Reported values are preserved next to normalized ones. No value is imputed. |
| Gold | Every case-level table is unique on its natural key before any join, so no case is counted twice per drug, reaction or pair; a + b + c + d = N for every stored pair. |
| Serving | Reads Gold only; no statistic is recomputed per request. |

## Processing choices
- All large transformations run in Spark (`local[*]` unless configured otherwise). The Parquet write path is native Spark; a PyArrow streaming fallback exists only for Windows without Hadoop binaries.
- Signal statistics are Spark column expressions, not Python UDFs; `src/signal_detection/metrics.py` is the scalar reference and unit tests assert both agree (including zero cells).
- Spark runs locally. A multi-node deployment is not part of this project and is not claimed.

## Reproducing a result
Any dashboard number traces to a Gold table, to `silver_metrics.json` / `ingestion_summary.json`, and to an archive SHA-256 in `data/raw/manifest.json`.
