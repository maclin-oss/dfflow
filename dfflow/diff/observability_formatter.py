"""
Human-readable observability log formatting utilities.
"""

from __future__ import annotations

import pandas as pd

from .dataframe_diff import (
    shape_diff,
    column_diff,
    null_diff,
    memory_diff,
    duplicate_diff,
    dtype_diff,
    impact_score,
)


def format_observability_report(
    step_name: str,
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> str:
    """
    Generate human-readable observability summary string.

    Parameters
    ----------
    step_name : str
        Name of the pipeline step.
    before_df : pd.DataFrame
        DataFrame before step execution.
    after_df : pd.DataFrame
        DataFrame after step execution.

    Returns
    -------
    str
        Multiline human-readable observability report.
    """

    # Compute Diffs
    shape = shape_diff(before_df, after_df)
    columns = column_diff(before_df, after_df)
    nulls = null_diff(before_df, after_df)
    memory = memory_diff(before_df, after_df)
    dup = duplicate_diff(before_df, after_df)
    dtype = dtype_diff(before_df, after_df)
    impact = impact_score(before_df, after_df)

    # Row Change
    before_rows = max(shape["rows_before"], 1)
    after_rows = shape["rows_after"]
    row_pct = ((after_rows - before_rows) / before_rows) * 100

    # Dtype Text
    dtype_text = (
        ", ".join(dtype["dtype_changed_columns"])
        if dtype["dtype_changed_columns"]
        else "None"
    )

    # Column Diff Conditional
    column_section = ""
    added_cols = columns["columns_added"]
    removed_cols = columns["columns_removed"]

    if added_cols or removed_cols:
        added_text = ", ".join(added_cols) if added_cols else "None"
        removed_text = ", ".join(removed_cols) if removed_cols else "None"

        column_section = f"""

Column Changes:
Added: {added_text}
Removed: {removed_text}
"""

    # Build Report
    report = f"""
[OBSERVABILITY] {step_name}
Impact: {impact["impact_level"]}

Rows: {shape["rows_before"]} → {shape["rows_after"]} ({row_pct:+.2f}%)
Nulls Added: {nulls["nulls_added"]}
Nulls Removed: {nulls["nulls_removed"]}

Memory: {memory["memory_before_mb"]}MB → {memory["memory_after_mb"]}MB
Memory Change: {memory["memory_change_mb"]:+.3f} MB

Duplicates Added: {dup["duplicates_added"]}
Duplicates Removed: {dup["duplicates_removed"]}

Dtype Changes: {dtype_text}{column_section}
""".strip()

    return report
