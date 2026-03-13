"""
DataFrame diff utilities for pipeline observability.

This module provides three layers of diff functionality:

Layer 1 — Structure Diff
------------------------
- shape_diff
- column_diff

Layer 2 — Data Observability Diff
---------------------------------
- null_diff
- memory_diff
- duplicate_diff
- dtype_diff

Layer 3 — Intelligence Layer
----------------------------
- impact_score
"""

from __future__ import annotations

import pandas as pd


# LAYER 1 — STRUCTURE DIFF

def shape_diff(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> dict[str, int]:
    """
    Compute shape differences between two DataFrames.
    """
    return {
        "rows_before": before_df.shape[0],
        "rows_after": after_df.shape[0],
        "cols_before": before_df.shape[1],
        "cols_after": after_df.shape[1],
        "rows_removed": max(before_df.shape[0] - after_df.shape[0], 0),
        "rows_added": max(after_df.shape[0] - before_df.shape[0], 0),
    }


def column_diff(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> dict[str, list[str]]:
    """
    Compute column additions and removals between two DataFrames.
    """
    before_cols = set(before_df.columns)
    after_cols = set(after_df.columns)

    return {
        "columns_added": sorted(list(after_cols - before_cols)),
        "columns_removed": sorted(list(before_cols - after_cols)),
    }


# LAYER 2 — DATA OBSERVABILITY DIFF

def null_diff(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> dict[str, int]:
    """
    Compute null value count differences between two DataFrames.
    """
    before_nulls = int(before_df.isna().sum().sum())
    after_nulls = int(after_df.isna().sum().sum())

    return {
        "nulls_before_total": before_nulls,
        "nulls_after_total": after_nulls,
        "nulls_added": max(after_nulls - before_nulls, 0),
        "nulls_removed": max(before_nulls - after_nulls, 0),
    }


def memory_diff(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> dict[str, float]:
    """
    Compute memory usage differences in MB between two DataFrames.
    """
    before_mem = before_df.memory_usage(deep=True).sum() / 1e6
    after_mem = after_df.memory_usage(deep=True).sum() / 1e6

    return {
        "memory_before_mb": round(before_mem, 3),
        "memory_after_mb": round(after_mem, 3),
        "memory_change_mb": round(after_mem - before_mem, 3),
    }


def duplicate_diff(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> dict[str, int]:
    """
    Compute duplicate row count differences between two DataFrames.
    """
    before_dup = int(before_df.duplicated().sum())
    after_dup = int(after_df.duplicated().sum())

    return {
        "duplicates_before": before_dup,
        "duplicates_after": after_dup,
        "duplicates_added": max(after_dup - before_dup, 0),
        "duplicates_removed": max(before_dup - after_dup, 0),
    }


def dtype_diff(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> dict[str, list[str]]:
    """
    Detect dtype changes between two DataFrames.
    """
    changed_cols: list[str] = []

    common_cols = set(before_df.columns) & set(after_df.columns)

    for col in common_cols:
        if str(before_df[col].dtype) != str(after_df[col].dtype):
            changed_cols.append(col)

    return {
        "dtype_changed_columns": sorted(changed_cols)
    }


# LAYER 3 — IMPACT SCORING

def impact_score(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> dict[str, object]:
    """
    Compute overall transformation impact score.

    Parameters
    ----------
    before_df : pd.DataFrame
        DataFrame before transformation.
    after_df : pd.DataFrame
        DataFrame after transformation.

    Returns
    -------
    dict[str, object]
        Impact level (LOW / MEDIUM / HIGH) and contributing metrics.
    """

    before_rows = max(len(before_df), 1)
    after_rows = len(after_df)

    row_change_pct = abs(after_rows - before_rows) / before_rows * 100

    # Null diff
    nulls = null_diff(before_df, after_df)
    null_change_pct = (
        nulls["nulls_added"] / before_rows * 100
        if before_rows > 0 else 0
    )

    # Memory diff
    mem = memory_diff(before_df, after_df)
    mem_change_pct = (
        abs(mem["memory_change_mb"])
        / max(mem["memory_before_mb"], 0.001)
        * 100
    )

    # Dtype diff
    dtype = dtype_diff(before_df, after_df)
    dtype_changed = len(dtype["dtype_changed_columns"]) > 0

    # Duplicate diff
    dup = duplicate_diff(before_df, after_df)
    dup_added = dup["duplicates_added"]

    severity = "LOW"

    if (
        row_change_pct > 20
        or null_change_pct > 10
        or mem_change_pct > 50
        or dtype_changed
        or dup_added > 1000
    ):
        severity = "HIGH"

    elif (
        row_change_pct > 5
        or null_change_pct > 2
        or mem_change_pct > 20
        or dup_added > 100
    ):
        severity = "MEDIUM"

    return {
        "impact_level": severity,
        "row_change_pct": round(row_change_pct, 2),
        "null_change_pct": round(null_change_pct, 2),
        "memory_change_pct": round(mem_change_pct, 2),
        "dtype_changed": dtype_changed,
        "duplicates_added": dup_added,
    }
