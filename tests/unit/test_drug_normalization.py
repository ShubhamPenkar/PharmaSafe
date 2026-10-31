import pytest

from src.preprocessing.drug_normalization import ingredient_array, ingredients, normalize_ingredient


@pytest.mark.parametrize("raw,expected", [
    ("ATORVASTATIN CALCIUM", "ATORVASTATIN"),
    ("CIPROFLOXACIN HYDROCHLORIDE", "CIPROFLOXACIN"),
    ("ATORVASTATIN CALCIUM TRIHYDRATE", "ATORVASTATIN"),
    ("METOPROLOL TARTRATE", "METOPROLOL"),
    ("LEVOTHYROXINE SODIUM", "LEVOTHYROXINE"),
    ("CALCIUM CARBONATE", "CALCIUM CARBONATE"),       # protected inorganic stem
    ("SODIUM CHLORIDE", "SODIUM CHLORIDE"),
    ("LITHIUM CARBONATE", "LITHIUM CARBONATE"),
    ("ROSIGLITAZONE MALEATE", "ROSIGLITAZONE"),
    ("PEMBROLIZUMAB", "PEMBROLIZUMAB"),
    ("  isotretinoin  ", "ISOTRETINOIN"),
])
def test_normalize_ingredient(raw, expected):
    assert normalize_ingredient(raw) == expected


def test_multi_ingredient_products_split_and_deduplicate():
    assert ingredients("AMLODIPINE BESYLATE\\ATORVASTATIN CALCIUM") == ["AMLODIPINE", "ATORVASTATIN"]
    assert ingredients("ATORVASTATIN CALCIUM\\EZETIMIBE") == ["ATORVASTATIN", "EZETIMIBE"]
    assert ingredients("ATORVASTATIN\\ATORVASTATIN CALCIUM") == ["ATORVASTATIN"]


def test_spark_expression_matches_python_reference(spark):
    rows = [("AMLODIPINE BESYLATE\\ATORVASTATIN CALCIUM", None), ("ATORVASTATIN CALCIUM", "LIPITOR"), ("CALCIUM CARBONATE", None),
            (None, "SOLONDO [ISOTRETINOIN]"), (None, "SOME BRAND"), (None, None)]
    df = spark.createDataFrame(rows, "prod_ai_norm string, drugname_norm string").withColumn("ing", ingredient_array("prod_ai_norm", "drugname_norm"))
    got = [r["ing"] for r in df.collect()]
    assert got[0] == ["AMLODIPINE", "ATORVASTATIN"]
    assert got[1] == ["ATORVASTATIN"]
    assert got[2] == ["CALCIUM CARBONATE"]
    assert got[3] == ["ISOTRETINOIN"]
    assert got[4] == ["SOME BRAND"]
    assert got[5] is None
    for (prod, _), result in zip(rows[:3], got[:3]):
        assert result == ingredients(prod)
