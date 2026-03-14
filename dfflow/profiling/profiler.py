"""
DataFrame profiling utilities.
"""

from __future__ import annotations

import pandas as pd

class DataProfiler:
    """
    Provides quick DataFrame metadata summary.

    Useful for pre and post pipeline data quality checks.
    """
    @staticmethod
    def summary(df: pd.DataFrame) -> dict[str, object]:
        """
        Generate a metadata summary of a DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame to summarize.

        Returns
        -------
        dict[str, object]
            Summary containing shape, column names, null counts,
            dtypes, memory usage, and duplicated row count.
        """
        return {
            "shape": df.shape,
            "column_names": df.columns.tolist(),
            "null_counts": df.isnull().sum().to_dict(),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "memory_mb": df.memory_usage(deep=True).sum() / 1e6,
            "duplicated_rows": int(df.duplicated().sum()),
        }
