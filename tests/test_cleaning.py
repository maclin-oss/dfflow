import pandas as pd
from dfflow.steps.cleaning import drop_nulls, lowercase_columns

def test_drop_nulls():
    df = pd.DataFrame({"a": [1, None]})
    result = drop_nulls(df)
    assert result.shape[0] == 1


def test_lowercase_columns():
    df = pd.DataFrame({"A": [1, 2]})
    result = lowercase_columns(df)
    assert "a" in result.columns
