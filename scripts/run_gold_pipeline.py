"""
PharmaSafe Gold runner: builds analytical marts and safety signals from data/silver.

    python scripts/run_gold_pipeline.py                  # latest canonical snapshot -> data/gold/latest
    python scripts/run_gold_pipeline.py --asof 2025Q2    # historical snapshot -> data/gold/asof_2025Q2
"""

import sys
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analytics.gold_pipeline import GoldPipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--asof", nargs="*", default=[None], help="Source quarter(s) for historical snapshots")
    args = parser.parse_args()
    gold = GoldPipeline()
    for q in args.asof or [None]:
        gold.run(asof_quarter=q)
