import pandas as pd
from dfflow.profiling.profiler import DataProfiler


def test_summary():
    df = pd.DataFrame({"a": [1, 2, 3]})
    summary = DataProfiler.summary(df)

    assert "shape" in summary
    assert "column_names" in summary
    assert "memory_mb" in summary
