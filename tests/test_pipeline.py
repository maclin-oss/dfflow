import pytest
import pandas as pd

from dfflow import FlowPipeline, Step, Branch
from dfflow.core.exceptions import PipelineStepError
from dfflow.decorators.step_decorator import step


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2, 3]})


def test_pipeline_basic_execution(sample_df):
    def multiply(df):
        df = df.copy()
        df["a"] = df["a"] * 2
        return df

    pipeline = FlowPipeline()
    pipeline.add_step(Step("Multiply", multiply))

    result = pipeline.run(sample_df)
    assert result["a"].tolist() == [2, 4, 6]


def test_pipeline_empty_df():
    pipeline = FlowPipeline()
    result = pipeline.run(pd.DataFrame())
    assert result.empty


def test_pipeline_invalid_input():
    pipeline = FlowPipeline()
    with pytest.raises(TypeError):
        pipeline.run("not a dataframe")


def test_step_returns_invalid_type(sample_df):
    pipeline = FlowPipeline()
    pipeline.add_step(Step("Bad", lambda df: "not df"))
    with pytest.raises(PipelineStepError):
        pipeline.run(sample_df)


def test_add_step_invalid_type():
    pipeline = FlowPipeline()
    with pytest.raises(TypeError):
        pipeline.add_step("not a step")


def test_cache_skips_execution(sample_df):
    calls = {"count": 0}

    def count_step(df):
        calls["count"] += 1
        return df

    pipeline = FlowPipeline()
    pipeline.add_step(Step("Counter", count_step, cacheable=True))

    pipeline.run(sample_df)
    pipeline.run(sample_df)

    assert calls["count"] == 1


def test_pipeline_with_decorated_function(sample_df):
    @step("Add Column")
    def add_column(df):
        df = df.copy()
        df["b"] = 10
        return df

    pipeline = FlowPipeline()
    pipeline.add_step(add_column)

    result = pipeline.run(sample_df)
    assert "b" in result.columns


def test_pipeline_branch_true_path(sample_df):
    pipeline = FlowPipeline()
    pipeline.add_branch(
        name="Test Branch",
        condition=lambda df: True,
        if_true=[Step("Tag True", lambda df: df.assign(path="true"))],
        if_false=[Step("Tag False", lambda df: df.assign(path="false"))],
    )

    result = pipeline.run(sample_df)
    assert (result["path"] == "true").all()


def test_pipeline_branch_false_path(sample_df):
    pipeline = FlowPipeline()
    pipeline.add_branch(
        name="Test Branch",
        condition=lambda df: False,
        if_true=[Step("Tag True", lambda df: df.assign(path="true"))],
        if_false=[Step("Tag False", lambda df: df.assign(path="false"))],
    )

    result = pipeline.run(sample_df)
    assert (result["path"] == "false").all()


def test_pipeline_branch_empty_false_path(sample_df):
    pipeline = FlowPipeline()
    pipeline.add_branch(
        name="One Path",
        condition=lambda df: False,
        if_true=[Step("Tag True", lambda df: df.assign(path="true"))],
        if_false=[],
    )

    result = pipeline.run(sample_df)
    assert "path" not in result.columns


def test_pipeline_branch_condition_non_bool_raises(sample_df):
    pipeline = FlowPipeline()
    pipeline.add_branch(
        name="Bad Condition",
        condition=lambda df: "not_bool",
        if_true=[Step("Tag", lambda df: df.assign(path="true"))],
    )

    with pytest.raises(PipelineStepError):
        pipeline.run(sample_df)


def test_pipeline_multiple_branches(sample_df):
    pipeline = FlowPipeline()

    pipeline.add_branch(
        name="Branch 1",
        condition=lambda df: True,
        if_true=[Step("Add X", lambda df: df.assign(x=1))],
        if_false=[],
    )

    pipeline.add_branch(
        name="Branch 2",
        condition=lambda df: True,
        if_true=[Step("Add Y", lambda df: df.assign(y=2))],
        if_false=[],
    )

    result = pipeline.run(sample_df)
    assert "x" in result.columns
    assert "y" in result.columns


def test_pipeline_visualize():
    pipeline = FlowPipeline()
    pipeline.add_step(Step("TestStep", lambda df: df))

    output = pipeline.visualize()
    assert "Pipeline Flow" in output
    assert "TestStep" in output