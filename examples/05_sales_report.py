"""
05 - Sales Report Example.
-----------------------------------------
Demonstrates all dfflow features together:
- FlowPipeline with sequential steps
- @step decorator with cacheable=True
- Branch: conditional routing
- DFLogger in JSON mode
- show_observability=True
- DataProfiler: pre and post summary
- visualize() for documentation
"""

import json
import pandas as pd
from dfflow import (
    FlowPipeline, Step, DFLogger,
    step, drop_nulls, lowercase_columns, DataProfiler
)


@step("Fill Missing Regions", cacheable=True)
def fill_missing_regions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["region"] = df["region"].fillna("Unknown")
    return df


@step("Compute Sales Rank", cacheable=True)
def compute_sales_rank(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["rank"] = df["amount"].rank(ascending=False).astype(int)
    return df


@step("Add Performance Label")
def add_performance_label(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["performance"] = df["amount"].apply(
        lambda x: "top" if x >= 1000 else "average" if x >= 200 else "low"
    )
    return df


@step("Apply High Value Discount")
def apply_high_value_discount(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["final_amount"] = df["amount"].apply(
        lambda x: x * 0.90 if x >= 1000 else x
    )
    return df


@step("Apply Standard Pricing")
def apply_standard_pricing(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["final_amount"] = df["amount"]
    return df


@step("Flag Unknown Regions")
def flag_unknown_regions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["region_flag"] = df["region"].apply(
        lambda x: "needs_review" if x == "Unknown" else "ok"
    )
    return df


def main() -> None:
    sales = pd.DataFrame({
        "Sales_ID":    [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
        "Sales_person": ["Alice", "Bob", "Alice", "Carol", None, "Bob", "Carol", "Alice", "Bob", "Carol"],
        "Product":     ["Laptop", "Mouse", "Monitor", "Keyboard", "Laptop", "Webcam", None, "Monitor", "Mouse", "Laptop"],
        "AMOUNT":      [1500.0, 25.0, 400.0, 75.0, 1800.0, 90.0, 350.0, 420.0, 30.0, 2100.0],
        "REGION":      ["North", "South", "North", None, "East", "West", "South", "North", "East", "West"],
        "QUARTER":     ["Q1", "Q1", "Q1", "Q2", "Q2", "Q1", "Q2", "Q2", "Q1", "Q2"],
    })

    logger = DFLogger(log_file="sales_report_json.log", min_level="INFO", mode="json")

    # Pre-Pipeline Profile
    print("=== Pre-Pipeline Data Quality Check ===")
    profile = DataProfiler.summary(sales)
    print(f"  Shape      : {profile['shape']}")
    print(f"  Nulls      : {profile['null_counts']}")
    print(f"  Duplicates : {profile['duplicated_rows']}")
    print(f"  Memory     : {profile['memory_mb']:.4f} MB")
    print()

    # Pipeline
    pipeline = FlowPipeline(logger=logger)
    pipeline.add_step(lowercase_columns)
    pipeline.add_step(fill_missing_regions)
    pipeline.add_step(drop_nulls)
    pipeline.add_step(compute_sales_rank)
    pipeline.add_step(add_performance_label)

    pipeline.add_branch(
        name="Deal Size Router",
        condition=lambda df: df["amount"].mean() >= 500,
        if_true=[Step("High Value Discount", apply_high_value_discount)],
        if_false=[Step("Standard Pricing", apply_standard_pricing)],
    )

    pipeline.add_branch(
        name="Region Quality Check",
        condition=lambda df: (df["region"] == "Unknown").any(),
        if_true=[Step("Flag Unknown Regions", flag_unknown_regions)],
        if_false=[],
    )

    print(pipeline.visualize())
    print()

    result = pipeline.run(sales, show_observability=True)

    # Post-Pipeline Profile
    print("\n=== Post-Pipeline Profile ===")
    profile_after = DataProfiler.summary(result)
    print(f"  Shape      : {profile_after['shape']}")
    print(f"  Nulls      : {profile_after['null_counts']}")
    print(f"  Memory     : {profile_after['memory_mb']:.4f} MB")

    # Final Report
    print("\n=== Sales Report ===")
    cols = ["sales_id", "sales_person", "product", "amount", "final_amount", "rank", "performance", "quarter"]
    print(result[cols].sort_values("rank").to_string(index=False))

    # JSON Log Preview
    print("\n=== JSON Log Preview ===")
    with open("sales_report_json.log") as f:
        for line in f:
            entry = json.loads(line)
            print(f"[{entry['level']}] {entry['message']}")


if __name__ == "__main__":
    main()