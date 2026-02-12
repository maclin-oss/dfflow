import pandas as pd
from .decorators import log_step


@log_step("Drop Nulls")
def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows containing missing values from a DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame to clean.

    Returns
    -------
    pandas.DataFrame
        DataFrame with rows containing null values removed.
    """
    return df.dropna()


@log_step("Lowercase Columns")
def lowercase_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all column names in a DataFrame to lowercase.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame whose column names will be transformed.

    Returns
    -------
    pandas.DataFrame
        DataFrame with lowercase column names.
    """
    df = df.copy()
    df.columns = [col.lower() for col in df.columns]
    return df
