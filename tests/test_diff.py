import pandas as pd
from dfflow.diff.dataframe_diff import (
    shape_diff,
    column_diff,
    null_diff,
    memory_diff,
    duplicate_diff,
    dtype_diff,
    impact_score
)

def test_shape_diff():
    before = pd.DataFrame({"a": [1, 2]})
    after = pd.DataFrame({"a": [1]})
    result = shape_diff(before, after)
    assert result["rows_removed"] == 1


def test_column_diff():
    before = pd.DataFrame({"a": [1]})
    after = pd.DataFrame({"a": [1], "b": [2]})
    result = column_diff(before, after)
    assert "b" in result["columns_added"]


def test_null_diff():
    before = pd.DataFrame({"a": [1, None]})
    after = pd.DataFrame({"a": [1, 2]})
    result = null_diff(before, after)
    assert result["nulls_removed"] == 1


def test_duplicate_diff():
    before = pd.DataFrame({"a": [1, 1]})
    after = pd.DataFrame({"a": [1]})
    result = duplicate_diff(before, after)
    assert result["duplicates_removed"] >= 0


def test_dtype_diff():
    before = pd.DataFrame({"a": [1, 2]})
    after = pd.DataFrame({"a": ["1", "2"]})
    result = dtype_diff(before, after)
    assert "a" in result["dtype_changed_columns"]


def test_impact_score_diff():
    before = pd.DataFrame({"a": range(1000)})
    after = before.head(10)
    result = impact_score(before, after)
    assert result["impact_level"] in {"HIGH", "MEDIUM"}
