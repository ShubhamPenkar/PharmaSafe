"""
Conservative active-ingredient normalization for FAERS drug names.

Applied to FDA's mapped `prod_ai` (or the reported name when none), after uppercasing:
  1. A multi-ingredient product ("A\\B", FDA's separator) becomes a set of ingredients; a case is
     counted once for each ingredient.
  2. Trailing salt / hydrate / ester-form words are removed from each ingredient
     ("ATORVASTATIN CALCIUM" -> "ATORVASTATIN", "CIPROFLOXACIN HYDROCHLORIDE" -> "CIPROFLOXACIN").
     Stems that begin with a simple inorganic cation (SODIUM, CALCIUM, ...) are left untouched, so
     "CALCIUM CARBONATE" and "SODIUM CHLORIDE" never collapse into "CALCIUM"/"SODIUM".
  3. A bracketed generic in a reported name ("SOLONDO [ISOTRETINOIN]") yields the bracketed name.
No fuzzy matching and no dose parsing are attempted: unmapped brand spellings stay separate
(documented in docs/limitations.md). The reported name is always retained next to the key.
The regexes below are shared by the Spark expression and the Python reference used in tests.
"""

import re
from typing import List

from pyspark.sql import Column
from pyspark.sql.functions import (
    array_distinct, array_sort, col, filter as spark_filter, regexp_extract, regexp_replace, size, split, transform, trim, when,
)

SALT_TOKENS = [
    "HYDROCHLORIDE", "DIHYDROCHLORIDE", "HCL", "HYDROBROMIDE", "BROMIDE", "SULFATE", "SULPHATE", "BISULFATE", "PHOSPHATE",
    "DIPHOSPHATE", "ACETATE", "CITRATE", "TARTRATE", "BITARTRATE", "SUCCINATE", "FUMARATE", "MALEATE", "MESYLATE", "BESYLATE",
    "TOSYLATE", "LACTATE", "GLUCONATE", "NITRATE", "OXALATE", "PAMOATE", "HYDROXIDE", "DECANOATE", "ENANTHATE", "PROPIONATE",
    "VALERATE", "BENZOATE", "STEARATE", "MONOHYDRATE", "DIHYDRATE", "TRIHYDRATE", "HEMIHYDRATE", "SESQUIHYDRATE", "ANHYDROUS",
    "HYDRATE", "SODIUM", "POTASSIUM", "CALCIUM", "MAGNESIUM", "DISODIUM", "TROMETHAMINE", "MEGLUMINE", "ARGININE", "LYSINE",
    "ETABONATE", "AXETIL", "PIVOXIL", "MEDOXOMIL", "DIPROPIONATE", "HEMIFUMARATE", "BESILATE", "MESILATE", "EDISYLATE",
]
PROTECTED_STEMS = [
    "SODIUM", "CALCIUM", "POTASSIUM", "MAGNESIUM", "ZINC", "IRON", "FERROUS", "FERRIC", "LITHIUM", "COPPER", "AMMONIUM",
    "ALUMINUM", "ALUMINIUM", "SILVER", "BARIUM", "SELENIUM", "WATER", "SALINE", "GLUCOSE", "DEXTROSE",
]
_TRAILING = r"(?:\s+(?:" + "|".join(sorted(SALT_TOKENS, key=len, reverse=True)) + r"))+$"
_PROTECTED = r"^(?:" + "|".join(PROTECTED_STEMS) + r")\b"
_SEPARATOR = r"\\"   # regex for one literal backslash


def normalize_ingredient(name: str) -> str:
    """Python reference implementation of the per-ingredient rule."""
    s = re.sub(r"\s+", " ", name.upper().strip())
    if re.search(_PROTECTED, s):
        return s
    return re.sub(_TRAILING, "", s).strip()


def ingredients(product: str) -> List[str]:
    """Python reference: sorted unique ingredient list for one product string."""
    parts = [normalize_ingredient(p) for p in re.split(_SEPARATOR, product) if p.strip()]
    return sorted({p for p in parts if p})


def ingredient_array(prod_ai_norm: str, drugname_norm: str) -> Column:
    """
    Spark expression: sorted unique ingredient array from the normalized prod_ai, falling back to the
    normalized reported name (bracketed generic preferred). NULL when neither is available.
    """
    bracketed = regexp_extract(col(drugname_norm), r"\[([^\]]+)\]", 1)
    source = (
        when(col(prod_ai_norm).isNotNull(), col(prod_ai_norm))
        .when(bracketed != "", bracketed)
        .otherwise(col(drugname_norm))
    )
    cleaned = transform(
        split(source, _SEPARATOR),
        lambda x: when(trim(x).rlike(_PROTECTED), trim(x)).otherwise(trim(regexp_replace(trim(x), _TRAILING, ""))),
    )
    result = array_sort(array_distinct(spark_filter(cleaned, lambda x: x != "")))
    return when(source.isNotNull() & (size(result) > 0), result)
