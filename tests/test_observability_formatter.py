import pandas as pd
from dfflow.diff.observability_formatter import format_observability_report


def test_format_observability_report():
    before = pd.DataFrame({"A": [1, 2, 3],
                           "B": ["x", "y", "z"]})
    after = pd.DataFrame({"A":[1, 2],
                          "B": ["x", "y"],
                          "C": [100, 200]})

    report = format_observability_report("TestStep", before, after)

    assert "OBSERVABILITY" in report
    assert "Impact" in report
    assert "Rows" in report
    assert "Nulls Added" in report
    assert "Memory" in report
    assert "Dtype Changes" in report
    assert "Column Changes" in report
