"""
02 - Logging
-------------
Demonstrates:
- DFLogger in text mode (human-readable)
- DFLogger in json mode (cloud-ready)
- min_level filtering
"""

import json
import pandas as pd
from dfflow import FlowPipeline, DFLogger, drop_nulls, lowercase_columns


def main() -> None:
    df = pd.DataFrame({
        "Product":  ["Widget", "Gadget", None, "Monitor", "Webcam"],
        "PRICE":    [500.0, 1200.0, 75.0, None, 90.0],
        "CATEGORY": ["Electronics", "Electronics", "Accessories", "Electronics", "Accessories"],
    })

    # Text Mode Logger
    print("=== Text Mode ===")
    text_logger = DFLogger(log_file="pipeline_text.log", min_level="INFO", mode="text")

    pipeline = FlowPipeline(logger=text_logger)
    pipeline.add_step(drop_nulls)
    pipeline.add_step(lowercase_columns)
    pipeline.run(df)

    print("Logged to: pipeline_text.log")
    print()

    # JSON Mode Logger
    print("=== JSON Mode ===")
    json_logger = DFLogger(log_file="pipeline_json.log", min_level="INFO", mode="json")

    pipeline2 = FlowPipeline(logger=json_logger)
    pipeline2.add_step(drop_nulls)
    pipeline2.add_step(lowercase_columns)
    pipeline2.run(df)

    print("Logged to: pipeline_json.log")
    print()

    # Preview JSON Log
    print("=== JSON Log Preview ===")
    with open("pipeline_json.log") as f:
        for line in f:
            entry = json.loads(line)
            print(f"[{entry['level']}] {entry['message']}")


if __name__ == "__main__":
    main()