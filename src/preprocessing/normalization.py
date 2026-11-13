"""
PharmaSafe Field Standardization Helpers
Column expressions shared by the Silver pipeline. Every helper derives a NEW column and leaves the
reported value untouched, so the verbatim source is always available next to the normalized one.

All parsing uses try_* functions, so malformed values become NULL instead of failing the job under
Spark's ANSI mode (the Spark 4 default). Nothing is imputed: a partial date stays partial, an
unknown unit yields a NULL conversion.
"""

from typing import Dict, Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, expr, lit, regexp_replace, trim, upper, when

FAERS_DATE_PATTERN_DAY = r"^(19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])$"
FAERS_DATE_PATTERN_MONTH = r"^(19|20)\d{2}(0[1-9]|1[0-2])$"
FAERS_DATE_PATTERN_YEAR = r"^(19|20)\d{2}$"

# Unit conversions documented in the FAERS ASCII README (age_cod / wt_cod)
AGE_UNIT_TO_YEARS: Dict[str, float] = {
    "DEC": 10.0, "YR": 1.0, "MON": 1.0 / 12.0, "WK": 1.0 / 52.1775, "DY": 1.0 / 365.25, "HR": 1.0 / 8766.0,
}
WEIGHT_UNIT_TO_KG: Dict[str, float] = {"KG": 1.0, "LBS": 0.45359237, "GMS": 0.001}

VALID_ROLE_CODES = ["PS", "SS", "C", "I"]
VALID_OUTCOME_CODES = ["DE", "LT", "HO", "DS", "CA", "RI", "OT"]
VALID_SEX_CODES = ["M", "F", "UNK"]


def blank_to_null(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    """Trims string columns and converts empty strings to NULL (FAERS encodes missing as empty)."""
    return df.select(*[
        when(trim(col(c)) == "", lit(None)).otherwise(trim(col(c))).alias(c) if c in set(columns) else col(c)
        for c in df.columns
    ])


def normalize_term(c: str) -> Column:
    """Uppercases, trims, collapses internal whitespace and strips trailing punctuation from free text."""
    cleaned = regexp_replace(regexp_replace(upper(trim(col(c))), r"\s+", " "), r"[\s.,;]+$", "")
    return when(cleaned == "", lit(None)).otherwise(cleaned)


def is_numeric_id(c: str) -> Column:
    """FAERS primaryid / caseid are positive integers encoded as text."""
    return col(c).isNotNull() & col(c).rlike(r"^\d+$")


def try_int(c: str) -> Column:
    return expr(f"try_cast(`{c}` AS INT)")


def try_double(c: str) -> Column:
    return expr(f"try_cast(`{c}` AS DOUBLE)")


def try_long(c: str) -> Column:
    return expr(f"try_cast(`{c}` AS BIGINT)")


def faers_date(c: str) -> Column:
    """Full YYYYMMDD dates only; partial or invalid dates are NULL (see faers_date_precision)."""
    return when(
        col(c).rlike(FAERS_DATE_PATTERN_DAY),
        expr(f"try_to_timestamp(`{c}`, 'yyyyMMdd')").cast("date")
    )


def faers_date_precision(c: str) -> Column:
    """DAY / MONTH / YEAR for well-formed (possibly partial) dates, INVALID otherwise, NULL if missing."""
    return (
        when(col(c).isNull(), lit(None))
        .when(faers_date(c).isNotNull(), lit("DAY"))
        .when(col(c).rlike(FAERS_DATE_PATTERN_MONTH), lit("MONTH"))
        .when(col(c).rlike(FAERS_DATE_PATTERN_YEAR), lit("YEAR"))
        .otherwise(lit("INVALID"))
    )


def with_parsed_dates(df: DataFrame, date_columns: Iterable[str]) -> DataFrame:
    """Adds <c>_date and <c>_precision for each FAERS date column present in df."""
    for c in date_columns:
        if c in df.columns:
            df = df.withColumn(f"{c}_date", faers_date(c)).withColumn(f"{c}_precision", faers_date_precision(c))
    return df


def unit_conversion(value_col: str, unit_col: str, factors: Dict[str, float]) -> Column:
    """Converts value_col into a reference unit using unit_col; unknown units give NULL."""
    unit = upper(col(unit_col))
    factor = None
    for code, f in factors.items():
        factor = when(unit == code, lit(f)) if factor is None else factor.when(unit == code, lit(f))
    return try_double(value_col) * factor


def coded_value(c: str, allowed: Iterable[str]) -> Column:
    """Uppercased code if it is in the documented code list, else NULL."""
    code = upper(col(c))
    return when(code.isin(list(allowed)), code)
