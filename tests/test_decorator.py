import pandas as pd
import pytest
from dfflow.decorators.step_decorator import step


def test_step_decorator_metadata():
    @step("Test Step", cacheable=True)
    def func(df: pd.DataFrame) -> pd.DataFrame:
        return df

    assert hasattr(func, "_dfflow_meta")
    assert func._dfflow_meta["name"] == "Test Step"
    assert func._dfflow_meta["cacheable"] is True


def test_decorator_invalid_name():
    with pytest.raises(ValueError):
        step("", cacheable=False)


def test_decorator_invalid_cacheable():
    with pytest.raises(TypeError):
        step("Test", cacheable="yes")

