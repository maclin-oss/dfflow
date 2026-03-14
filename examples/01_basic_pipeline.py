"""
01 - Basic Pipeline
--------------------
Demonstrates:
- FlowPipeline setup
- Built-in steps: drop_nulls, lowercase_columns
- Custom step using @step decorator
- visualize() for pipeline structure
"""

import pandas as pd
from dfflow import FlowPipeline, step, drop_nulls, lowercase_columns


@step("Round Revenue")
def round_revenue(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["revenue"] = df["revenue"].round(0)
    return df


def main() -> None:
    df = pd.DataFrame({
        "CustomerName": ["Alice", "Bob", None, "Diana", "Eve"],
        "AGE":          [25, 30, 22, None, 28],
        "Revenue":      [500.0, 1200.0, 750.0, 300.0, 900.0],
        "REGION":       ["North", "South", "East", "West", "North"],
    })

    pipeline = FlowPipeline()
    pipeline.add_step(drop_nulls)
    pipeline.add_step(lowercase_columns)
    pipeline.add_step(round_revenue)

    print(pipeline.visualize())
    print()

    result = pipeline.run(df)

    print("=== Result ===")
    print(result)


if __name__ == "__main__":
    main()
