"""
Common DataFrame cleaning step implementation.
"""

from __future__ import annotations

import pandas as pd
from ..decorators.step_decorator import step

@step("Drop Nulls", cacheable=True)
def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows containing null values.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    df : pd.DataFrame
        DataFrame with rows containing null values removed.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError("drop_nulls expects pandas DataFrame")

    return df.dropna().copy()

@step("Lowercase Columns")
def lowercase_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert columns names to lowercase.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    df : pd.DataFrame
        DataFrame with lowercase columns.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("lowercase_columns expects pandas DataFrame")

    df_copy = df.copy()
    df_copy.columns = [str(c).lower() for c in df_copy.columns]

    return df_copy
