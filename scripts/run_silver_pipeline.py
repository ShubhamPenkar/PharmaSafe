"""
PharmaSafe Silver runner: rebuilds data/silver from completed Bronze quarters.

    python scripts/run_silver_pipeline.py
    python scripts/run_silver_pipeline.py --quarters 2024Q3 2024Q4
"""

import sys
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.preprocessing.silver_pipeline import SilverPipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--quarters", nargs="*", help="Bronze quarters to include (default: all completed)")
    args = parser.parse_args()
    SilverPipeline().run(args.quarters)
