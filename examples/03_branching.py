"""
03 - Branching
---------------
Demonstrates:
- Branch with condition
- if_true / if_false paths
- Multiple branches in one pipeline
- visualize() with branch nodes
"""

import pandas as pd
from dfflow import FlowPipeline, Step, DFLogger, step, drop_nulls, lowercase_columns


@step("Apply Premium Discount")
def apply_premium_discount(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["final_amount"] = df["amount"] * 0.90
    df["tier"] = "premium"
    return df


@step("Apply Standard Pricing")
def apply_standard_pricing(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["final_amount"] = df["amount"]
    df["tier"] = "standard"
    return df


@step("Flag for Review")
def flag_for_review(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["status"] = "needs_review"
    return df


@step("Mark as Verified")
def mark_as_verified(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["status"] = "verified"
    return df


def main() -> None:
    df = pd.DataFrame({
        "Order_ID":  [1, 2, 3, 4, 5, 6],
        "Customer": ["Alice", "Bob", None, "Diana", "Eve", "Frank"],
        "AMOUNT":   [1500.0, 80.0, 200.0, 950.0, 45.0, 3000.0],
        "REGION":   ["North", "South", "East", "West", "North", "North"],
    })

    logger = DFLogger(log_file="branching.log", min_level="INFO", mode="text")

    pipeline = FlowPipeline(logger=logger)
    pipeline.add_step(drop_nulls)
    pipeline.add_step(lowercase_columns)

    pipeline.add_branch(
        name="Order Value Router",
        condition=lambda df: df["amount"].mean() >= 500,
        if_true=[Step("Premium Discount", apply_premium_discount)],
        if_false=[Step("Standard Pricing", apply_standard_pricing)],
    )

    pipeline.add_branch(
        name="Order Verification",
        condition=lambda df: (df["amount"] < 50).any(),
        if_true=[Step("Flag for Review", flag_for_review)],
        if_false=[Step("Mark Verified", mark_as_verified)],
    )

    print(pipeline.visualize())
    print()

    result = pipeline.run(df)

    print("=== Result ===")
    print(result[["order_id", "customer", "amount", "final_amount", "tier", "status"]])


if __name__ == "__main__":
    main()