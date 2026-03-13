import pandas as pd
from dfflow.utils.hashing import hash_df

def test_hash_df():
    df = pd.DataFrame({"a": [1, 2, 3]})
    assert hash_df(df) == hash_df(df.copy())


def test_hash_changes():
    df1 = pd.DataFrame({"a": [1, 2, 3]})
    df2 = pd.DataFrame({"a": [1, 2, 4]})
    assert hash_df(df1) != hash_df(df2)


def test_hash_df_fallback():
    df = pd.DataFrame({"a": [1, 2],
                       "b": [[1], [2]]})
    result = hash_df(df)
    assert isinstance(result, str)