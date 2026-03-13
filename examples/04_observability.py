"""
04 - Observability & Caching
------------------------------
Demonstrates:
- cacheable=True — skip steps on unchanged data
- show_observability=True — full diff report after every step
- DataProfiler — pre and post pipeline summary
"""

import pandas as pd
from dfflow import FlowPipeline, DFLogger, step, drop_nulls, lowercase_columns, DataProfiler


@step("Normalize Salary", cacheable=True)
def normalize_salary(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    max_salary = df["salary"].max()
    df["salary_score"] = (df["salary"] / max_salary).round(2)
    return df


@step("Remove Duplicates", cacheable=True)
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates().copy()


def main() -> None:
    df = pd.DataFrame({
        "Employee_ID": [1, 2, 2, 3, 4, 5],
        "Name":       ["Alice", "Bob", "Bob", None, "Eve", "Frank"],
        "DEPARTMENT": ["Engineering", "Marketing", "Marketing", "Sales", None, "Engineering"],
        "Salary":     [95000.0, 72000.0, 72000.0, 68000.0, 55000.0, 110000.0],
    })

    logger = DFLogger(log_file="observability.log", min_level="INFO", mode="text")

    # Pre-Pipeline Profile
    print("=== Pre-Pipeline Profile ===")
    for key, value in DataProfiler.summary(df).items():
        print(f"  {key}: {value}")
    print()

    # Pipeline
    pipeline = FlowPipeline(logger=logger)
    pipeline.add_step(drop_nulls)
    pipeline.add_step(remove_duplicates)
    pipeline.add_step(lowercase_columns)
    pipeline.add_step(normalize_salary)

    print(pipeline.visualize())
    print()

    result = pipeline.run(df, show_observability=True)

    # Post-Pipeline Profile
    print("\n=== Post-Pipeline Profile ===")
    for key, value in DataProfiler.summary(result).items():
        print(f"  {key}: {value}")

    print("\n=== Result ===")
    print(result)

    # Cache Hit Demo

    print("\n=== Running Again (cache hit demo) ===")
    pipeline.run(result)
    print("Cacheable steps skipped — same data, no reprocessing.")


if __name__ == "__main__":
    main()