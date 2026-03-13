"""
DataFrame hashing utilities.
"""

from __future__ import annotations

import pandas as pd
import hashlib

def hash_df(df: pd.DataFrame) -> str:
    """
    Generate deterministic hash for DataFrame contents.

    Used internally for step-level cache validation.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame to hash.

    Returns
    -------
    str
        MD5 hex digest of the DataFrame contents.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError("hash_df expects pandas DataFrame")

    try:
        values = pd.util.hash_pandas_object(df, index=True).values

    except TypeError:
        values = pd.util.hash_pandas_object(df.astype(str, copy=False), index=True).values

    return hashlib.md5(values, usedforsecurity=False).hexdigest()
