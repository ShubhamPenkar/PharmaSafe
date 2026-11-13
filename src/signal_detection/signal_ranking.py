"""
Signal ranking. Candidates are ordered by the strength and stability of the evidence, never by a
risk probability: lower bound of the ROR confidence interval (conservative), then PRR, then cases.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number

from src.signal_detection.metrics import SIGNAL_STATUS


def rank_signals(signals: DataFrame, flagged_only: bool = True) -> DataFrame:
    df = signals.filter(col("screening_status") == SIGNAL_STATUS) if flagged_only else signals
    order = [col("ROR_CI_lower").desc(), col("PRR").desc(), col("observed_cases").desc(), col("drug_name"), col("reaction_term")]
    return df.withColumn("signal_rank", row_number().over(Window.orderBy(*order)))
