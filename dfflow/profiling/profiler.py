import pandas as pd
from typing import Dict, Any


def profile_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate a lightweight summary of a DataFrame.

    The summary includes:
    - Shape of the DataFrame
    - Column names
    - Null value counts per column
    - Data types of each column

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame to summarize.

    Returns
    -------
    dict
        Dictionary containing DataFrame metadata and statistics.
    """
    return {
        "shape": df.shape,
        "columns": list(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "dtypes": df.dtypes.astype(str).to_dict(),
    }
